"""ALS 矩阵分解召回与离线评测（隐式反馈）。

与 itemcf.py 使用完全相同的时间切分、正样本定义与评测口径，
保证两种算法的结果可直接对比（论文 6.3 节）。

用法：
    spark-submit spark_jobs/als.py \
        --input  data/processed/ods \
        --output data/processed/recs_als \
        [--ranks 10,50,100] [--reg 0.1] [--alpha 10] [--iters 10] \
        [--top-n 10] [--test-days 2] [--eval-users 2000]

说明：
  * 隐式反馈 ALS：把行为权重当作置信度，alpha 放大正反馈强度；
  * 用户/商品先做稠密编号（dense_rank），训练后映射回原始 ID；
  * --ranks 支持多组隐因子维度，自动完成参数实验并输出对比表。
"""
from __future__ import annotations

import argparse
import math
import sys

from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

BEHAVIOR_WEIGHT = {1: 1, 2: 2, 3: 3, 4: 4}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ALS 召回与离线评测")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--ranks", default="10,50,100", help="隐因子维度，逗号分隔")
    p.add_argument("--reg", type=float, default=0.1, help="正则系数")
    p.add_argument("--alpha", type=float, default=10.0, help="隐式反馈置信度放大系数")
    p.add_argument("--iters", type=int, default=10, help="迭代次数")
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--test-days", type=int, default=2)
    p.add_argument("--test-behaviors", default="3,4")
    p.add_argument("--eval-users", type=int, default=2000)
    p.add_argument("--min-user", type=int, default=5, help="用户最少交互数，低于此值不参与训练")
    p.add_argument("--min-item", type=int, default=10, help="商品最少被交互次数，低于此值不参与训练")
    p.add_argument("--shuffle-partitions", type=int, default=8)
    return p.parse_args(argv)


def build_spark(shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder.appName("ecommerce-als")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def weighted_interactions(df: DataFrame) -> DataFrame:
    wcol = F.create_map(*[F.lit(x) for pair in BEHAVIOR_WEIGHT.items() for x in pair])
    return (
        df.withColumn("w", wcol[F.col("behavior_type")])
        .groupBy("user_id", "item_id")
        .agg(F.max("w").cast("float").alias("w"))
    )


def split_by_time(df: DataFrame, test_days: int):
    days = [r["dt"] for r in df.select("dt").distinct().orderBy("dt").collect()]
    if len(days) <= test_days:
        raise SystemExit(f"日期数不足：{len(days)} 天")
    boundary = days[-test_days]
    return df.filter(F.col("dt") < boundary), df.filter(F.col("dt") >= boundary), boundary


def filter_interactions(train: DataFrame, min_user: int, min_item: int) -> DataFrame:
    """过滤冷启动用户与长尾商品：行为太少的用户/商品无法学到有效隐向量。"""
    uc = (
        train.groupBy("user_id")
        .agg(F.count("*").alias("c"))
        .filter(F.col("c") >= min_user)
        .select("user_id")
    )
    train = train.join(uc, "user_id", "left_semi")
    ic = (
        train.groupBy("item_id")
        .agg(F.count("*").alias("c"))
        .filter(F.col("c") >= min_item)
        .select("item_id")
    )
    return train.join(ic, "item_id", "left_semi")


def index_map(df: DataFrame, col: str, idx_col: str) -> DataFrame:
    w = Window.orderBy(col)
    return df.select(col).distinct().withColumn(
        idx_col, (F.dense_rank().over(w) - 1).cast("int")
    )


def evaluate(recs: DataFrame, test: DataFrame, catalog: int, eval_users: int, top_n: int, prefix: str):
    truth = test.groupBy("user_id").agg(F.collect_set("item_id").alias("items"))
    pool = truth.orderBy("user_id").limit(eval_users).cache()
    joined = pool.join(
        recs.groupBy("user_id").agg(F.collect_list(F.struct("rank", "item_id")).alias("recs")),
        "user_id",
        "left",
    )

    recalls, ndcgs = [], []
    for row in joined.collect():
        items = set(row["items"])
        rs = sorted(row["recs"] or [], key=lambda x: x["rank"])[:top_n]
        hits, dcg = 0, 0.0
        for pos, r in enumerate(rs, start=1):
            if r["item_id"] in items:
                hits += 1
                dcg += 1.0 / math.log2(pos + 1)
        idcg = sum(1.0 / math.log2(i + 1) for i in range(1, min(len(items), top_n) + 1))
        recalls.append(hits / len(items) if items else 0.0)
        ndcgs.append(dcg / idcg if idcg else 0.0)

    n = max(len(recalls), 1)
    recall = sum(recalls) / n
    ndcg = sum(ndcgs) / n
    coverage = recs.select("item_id").distinct().count() / max(catalog, 1)
    print(
        f"[{prefix}] Recall@{top_n}={recall:.4f}  NDCG@{top_n}={ndcg:.4f}  "
        f"Coverage={coverage:.4f}  评测用户={len(recalls):,}"
    )
    pool.unpersist()
    return recall, ndcg, coverage


def popularity_baseline(train: DataFrame, top_n: int) -> DataFrame:
    top = [
        r["item_id"]
        for r in train.groupBy("item_id")
        .agg(F.count("*").alias("freq"))
        .orderBy(F.desc("freq"), F.asc("item_id"))
        .limit(top_n)
        .collect()
    ]
    users = train.select("user_id").distinct()
    rows = [(r["user_id"], it, i + 1) for r in users.collect() for i, it in enumerate(top)]
    return train.sparkSession.createDataFrame(rows, "user_id long, item_id long, rank int")


def train_and_evaluate(
    train: DataFrame,
    u_map: DataFrame,
    i_map: DataFrame,
    rank: int,
    args: argparse.Namespace,
    test: DataFrame,
    catalog: int,
) -> tuple[float, float, float, DataFrame]:
    train_idx = (
        train.join(u_map, "user_id").join(i_map, "item_id").select("uidx", "iidx", "w")
    )
    als = ALS(
        rank=rank,
        regParam=args.reg,
        alpha=args.alpha,
        implicitPrefs=True,
        maxIter=args.iters,
        userCol="uidx",
        itemCol="iidx",
        ratingCol="w",
        coldStartStrategy="drop",
        seed=42,
    )
    model = als.fit(train_idx)

    raw = (
        model.recommendForAllUsers(args.top_n)
        .select("uidx", F.explode("recommendations").alias("r"))
        .select("uidx", F.col("r.iidx").alias("iidx"), F.col("r.rating").alias("score"))
    )
    recs = (
        raw.join(u_map, "uidx")
        .join(i_map, "iidx")
        .withColumn(
            "rank",
            F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("score"))),
        )
        .select("user_id", "item_id", "rank")
        .cache()
    )
    print(f"[als] rank={rank}: 推荐记录 {recs.count():,}")
    metrics = evaluate(recs, test, catalog, args.eval_users, args.top_n, f"als-rank{rank}")
    return (*metrics, recs)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spark = build_spark(args.shuffle_partitions)
    try:
        df = spark.read.parquet(args.input).cache()
        print(f"[als] 输入记录 {df.count():,}")

        train_raw, test_raw, boundary = split_by_time(df, args.test_days)
        print(f"[als] 切分点 {boundary}，训练集 {train_raw.count():,}，测试集 {test_raw.count():,}")

        train = weighted_interactions(train_raw)
        before = train.count()
        train = filter_interactions(train, args.min_user, args.min_item).cache()
        after = train.count()
        print(
            f"[als] 过滤冷启动：交互 {before:,} -> {after:,}"
            f"（用户>={args.min_user} 次，商品>={args.min_item} 次）"
        )

        u_map = index_map(train, "user_id", "uidx").cache()
        i_map = index_map(train, "item_id", "iidx").cache()

        behaviours = [int(x) for x in args.test_behaviors.split(",") if x.strip()]
        test = (
            test_raw.filter(F.col("behavior_type").isin(behaviours))
            .select("user_id", "item_id")
            .distinct()
        )
        catalog = i_map.count()
        print(f"[als] 用户 {u_map.count():,} / 商品 {catalog:,}，正样本 {test.count():,}")

        print(f"[als] ---- 热门基线 ----")
        evaluate(
            popularity_baseline(train, args.top_n), test, catalog, args.eval_users,
            args.top_n, "热门基线",
        )

        print(f"[als] ---- ALS 参数实验 ----")
        best = None
        best_rank = None
        for rank in [int(x) for x in args.ranks.split(",") if x.strip()]:
            recall, ndcg, coverage, recs = train_and_evaluate(
                train, u_map, i_map, rank, args, test, catalog
            )
            if best is None or recall > best[0]:
                best, best_rank, best_recs = (recall, ndcg, coverage), rank, recs

        print(f"[als] 最优 rank={best_rank}：Recall={best[0]:.4f} NDCG={best[1]:.4f} Coverage={best[2]:.4f}")
        best_recs.write.mode("overwrite").parquet(args.output)
        print(f"[als] 已写入: {args.output}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
