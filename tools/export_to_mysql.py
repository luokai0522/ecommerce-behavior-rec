"""把离线结果（推荐 / 画像标签）写入 MySQL，对应论文 4.3 节的库表设计。

依赖：pymysql（`pip install pymysql`）。DDL 见 sql/mysql_ddl.sql。

用法：
    # 只看数据形状，不连数据库
    python tools/export_to_mysql.py --dry-run

    # 真正写入
    python tools/export_to_mysql.py --host 127.0.0.1 --user root --password xxx --database ecommerce_rec

写入策略：全量覆盖（先 TRUNCATE 再批量插入），适合离线批处理产出的结果表。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
RECS_DIR = BASE / "data" / "processed" / "recs_als"
TAGS_DIR = BASE / "data" / "processed" / "user_tag"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="离线结果导入 MySQL")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="")
    p.add_argument("--database", default="ecommerce_rec")
    p.add_argument("--recs-dir", default=str(RECS_DIR))
    p.add_argument("--tags-dir", default=str(TAGS_DIR))
    p.add_argument("--algorithm", default="als")
    p.add_argument("--top-n", type=int, default=10, help="每个用户写入的推荐条数")
    p.add_argument("--batch", type=int, default=5000, help="单批插入行数")
    p.add_argument("--dry-run", action="store_true", help="不连数据库，只统计待写入数据")
    p.add_argument(
        "--sqlite",
        default=None,
        help="写入 SQLite 文件而不是 MySQL（用于本地验证 ETL 逻辑，schema 相同）",
    )
    return p.parse_args(argv)


def build_recommend_rows(args: argparse.Namespace) -> list[tuple]:
    """把推荐结果聚合成 recommend_result 表的一行行：一个用户一行，商品列表存 JSON。"""
    import pandas as pd

    df = pd.read_parquet(args.recs_dir, columns=["user_id", "item_id", "rank"])
    df = df[df["rank"] <= args.top_n].sort_values(["user_id", "rank"])

    today = dt.date.today().isoformat()
    rows: list[tuple] = []
    for uid, grp in df.groupby("user_id", sort=False):
        rows.append(
            (
                int(uid),
                json.dumps([int(x) for x in grp["item_id"].tolist()]),
                json.dumps([]),  # 当前输出未保存得分，留空
                args.algorithm,
                today,
            )
        )
    return rows


def build_tag_rows(args: argparse.Namespace) -> list[tuple]:
    """画像标签表：一类标签一行。"""
    import pandas as pd

    df = pd.read_parquet(args.tags_dir, columns=["user_id", "tag_type", "tag_value", "weight"])
    today = dt.date.today().isoformat()
    return [
        (
            int(r.user_id),
            str(r.tag_type),
            str(r.tag_value),
            float(r.weight),
            today,
        )
        for r in df.itertuples(index=False)
    ]


DDL = {
    "recommend_result": """
        CREATE TABLE IF NOT EXISTS recommend_result (
            id BIGINT NOT NULL AUTO_INCREMENT,
            user_id BIGINT NOT NULL,
            item_ids TEXT NOT NULL,
            scores TEXT,
            algorithm VARCHAR(20) NOT NULL,
            gen_date DATE NOT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY uk_user_algo_date (user_id, algorithm, gen_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "user_tag": """
        CREATE TABLE IF NOT EXISTS user_tag (
            id BIGINT NOT NULL AUTO_INCREMENT,
            user_id BIGINT NOT NULL,
            tag_type VARCHAR(30) NOT NULL,
            tag_value VARCHAR(50) NOT NULL,
            weight DOUBLE DEFAULT 1.0,
            update_date DATE NOT NULL,
            PRIMARY KEY (id),
            KEY idx_user (user_id),
            KEY idx_type (tag_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
}


def insert_batches(cursor, sql: str, rows: list[tuple], batch: int) -> int:
    total = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i : i + batch]
        cursor.executemany(sql, chunk)
        total += len(chunk)
    return total


SQLITE_DDL = {
    "recommend_result": """
        CREATE TABLE recommend_result (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            item_ids TEXT NOT NULL,
            scores TEXT,
            algorithm TEXT NOT NULL,
            gen_date TEXT NOT NULL
        )
    """,
    "user_tag": """
        CREATE TABLE user_tag (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            tag_type TEXT NOT NULL,
            tag_value TEXT NOT NULL,
            weight REAL DEFAULT 1.0,
            update_date TEXT NOT NULL
        )
    """,
}


def export_sqlite(args: argparse.Namespace, rec_rows: list[tuple], tag_rows: list[tuple]) -> int:
    """写入 SQLite：不需要装数据库服务，用于本地验证 ETL 与表结构。"""
    import sqlite3

    path = Path(args.sqlite)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    conn = sqlite3.connect(str(path))
    try:
        cur = conn.cursor()
        for ddl in SQLITE_DDL.values():
            cur.execute(ddl)

        cur.executemany(
            "INSERT INTO recommend_result (user_id, item_ids, scores, algorithm, gen_date) "
            "VALUES (?, ?, ?, ?, ?)",
            rec_rows,
        )
        for i in range(0, len(tag_rows), args.batch):
            cur.executemany(
                "INSERT INTO user_tag (user_id, tag_type, tag_value, weight, update_date) "
                "VALUES (?, ?, ?, ?, ?)",
                tag_rows[i : i + args.batch],
            )
        conn.commit()

        n1 = cur.execute("SELECT COUNT(*) FROM recommend_result").fetchone()[0]
        n2 = cur.execute("SELECT COUNT(*) FROM user_tag").fetchone()[0]
        top = cur.execute(
            "SELECT tag_type, tag_value, COUNT(*) c FROM user_tag "
            "WHERE tag_type='活跃度' GROUP BY tag_type, tag_value ORDER BY c DESC"
        ).fetchall()

        print(f"[export] SQLite 写入完成: {path}")
        print(f"[export]   recommend_result {n1:,} 行，user_tag {n2:,} 行")
        print("[export]   活跃度分布校验:", ", ".join(f"{v}={c:,}" for _, v, c in top))
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        print(f"[export] SQLite 写入失败: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    for path in (Path(args.recs_dir), Path(args.tags_dir)):
        if not path.exists():
            print(f"[export] 缺少输入: {path}", file=sys.stderr)
            return 1

    rec_rows = build_recommend_rows(args)
    tag_rows = build_tag_rows(args)

    print(f"[export] 推荐结果: {len(rec_rows):,} 个用户（每人 top-{args.top_n}）")
    if rec_rows:
        print(f"[export]   示例: user_id={rec_rows[0][0]} item_ids={rec_rows[0][1][:60]}...")
    print(f"[export] 画像标签: {len(tag_rows):,} 条")
    if tag_rows:
        print(f"[export]   示例: {tag_rows[0]}")

    if args.dry_run:
        print("[export] dry-run 结束，未连接数据库")
        return 0

    if args.sqlite:
        return export_sqlite(args, rec_rows, tag_rows)

    try:
        import pymysql
    except ImportError:
        print("[export] 需要 pymysql：python -m pip install pymysql", file=sys.stderr)
        return 1

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        charset="utf8mb4",
        autocommit=False,
    )
    try:
        with conn.cursor() as cur:
            for name, ddl in DDL.items():
                cur.execute(ddl)
            print("[export] 表结构就绪")

            cur.execute("TRUNCATE TABLE recommend_result")
            n1 = insert_batches(
                cur,
                "INSERT INTO recommend_result (user_id, item_ids, scores, algorithm, gen_date) "
                "VALUES (%s, %s, %s, %s, %s)",
                rec_rows,
                args.batch,
            )

            cur.execute("TRUNCATE TABLE user_tag")
            n2 = insert_batches(
                cur,
                "INSERT INTO user_tag (user_id, tag_type, tag_value, weight, update_date) "
                "VALUES (%s, %s, %s, %s, %s)",
                tag_rows,
                args.batch,
            )
        conn.commit()
        print(f"[export] 写入完成: recommend_result {n1:,} 行，user_tag {n2:,} 行")
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        print(f"[export] 失败，已回滚: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
