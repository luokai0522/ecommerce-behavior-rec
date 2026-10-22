"""推荐服务：把离线算好的推荐结果与画像标签暴露成 REST 接口（论文 4.1 服务层）。

启动：
    python server/app.py                  # 默认 http://127.0.0.1:8000
    PORT=9000 python server/app.py

接口：
    GET /                         服务信息与接口列表
    GET /health                   健康检查
    GET /api/recommend/<user_id>  个性化推荐，?top=10 控制返回条数
    GET /api/profile/<user_id>    用户画像标签
    GET /api/stats                推荐与标签的总体统计

数据来源（离线作业产出，需先跑 run_job.ps1）：
    data/processed/recs_als/      ALS 推荐结果（user_id, item_id, rank）
    data/processed/user_tag/      画像标签（user_id, tag_type, tag_value, weight）

说明：服务只读预计算结果，不做在线计算，因此响应时间稳定。
"""
from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
WEB_DIR = BASE / "web"
RECS_DIR = Path(os.getenv("RECS_DIR", BASE / "data" / "processed" / "recs_als"))
TAGS_DIR = Path(os.getenv("TAGS_DIR", BASE / "data" / "processed" / "user_tag"))
TOP_LIMIT = 50

app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="/static")
app.json.ensure_ascii = False

_recs: dict[int, list[int]] | None = None
_tags: pd.DataFrame | None = None
_tag_index: pd.DataFrame | None = None
_popular: list[int] | None = None


def _require(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"数据目录不存在: {path}\n"
            f"请先运行离线作业：run_job.ps1 -Job als（推荐）或 -Job profile（画像标签）"
        )


def load_recs() -> dict[int, list[int]]:
    """推荐结果载入内存：{user_id: [item_id, ...]}（按 rank 排序）。"""
    global _recs, _popular
    if _recs is None:
        _require(RECS_DIR)
        df = pd.read_parquet(RECS_DIR, columns=["user_id", "item_id", "rank"])
        df = df.sort_values(["user_id", "rank"])

        _recs = {
            int(uid): [int(x) for x in grp["item_id"].tolist()]
            for uid, grp in df.groupby("user_id", sort=False)
        }
        # 冷启动兜底：被推荐次数最多的商品当作“热门榜”
        _popular = [int(x) for x in df["item_id"].value_counts().head(TOP_LIMIT).index]
    return _recs


def load_tags() -> pd.DataFrame:
    global _tags, _tag_index
    if _tags is None:
        _require(TAGS_DIR)
        _tags = pd.read_parquet(TAGS_DIR)
        # 按 user_id 建索引：画像接口用 .loc 做哈希查找，
        # 避免每次请求都对 333 万行做全表布尔过滤（实测 35ms → 1ms）
        _tag_index = _tags.set_index("user_id", drop=False).sort_index()
    return _tags


def tags_of(user_id: int) -> pd.DataFrame | None:
    load_tags()
    if _tag_index is None or user_id not in _tag_index.index:
        return None
    return _tag_index.loc[[user_id]]


@app.errorhandler(FileNotFoundError)
def handle_missing(err):
    return jsonify({"error": "data_not_ready", "detail": str(err)}), 503


@app.after_request
def force_utf8(response):
    """显式声明 charset，避免客户端（如 PowerShell）按 Latin-1 解码导致中文乱码。"""
    if response.mimetype == "application/json":
        response.headers["Content-Type"] = "application/json; charset=utf-8"
    return response


@app.get("/")
def index():
    """有前端页面就直接返回大屏，否则返回接口清单。"""
    page = WEB_DIR / "index.html"
    if page.exists():
        return send_from_directory(WEB_DIR, "index.html")
    return jsonify(
        {
            "service": "ecommerce-recommend",
            "endpoints": [
                "/health",
                "/api/recommend/<user_id>?top=10",
                "/api/profile/<user_id>",
                "/api/stats",
            ],
            "note": "离线预计算 + 在线只读，对应论文 4.1 节的离线/在线分离架构",
        }
    )


@app.get("/health")
def health():
    """健康检查。features 用于确认进程是否是最新代码（排查"重启后仍是旧进程"的问题）。"""
    return jsonify(
        {
            "status": "ok",
            "recs_ready": RECS_DIR.exists(),
            "tags_ready": TAGS_DIR.exists(),
            "features": ["recs_cache", "tag_index", "stats_cache", "utf8_header"],
        }
    )


@app.get("/api/recommend/<int:user_id>")
def recommend(user_id: int):
    top = request.args.get("top", default=10, type=int)
    top = max(1, min(top, TOP_LIMIT))

    recs = load_recs()
    items = recs.get(user_id)

    if not items:
        # 冷启动：新用户没有历史行为，回退到热门榜
        return jsonify(
            {
                "user_id": user_id,
                "algorithm": "popular_fallback",
                "cold_start": True,
                "items": [
                    {"item_id": iid, "rank": i + 1}
                    for i, iid in enumerate((_popular or [])[:top])
                ],
            }
        )

    return jsonify(
        {
            "user_id": user_id,
            "algorithm": "als",
            "cold_start": False,
            "items": [
                {"item_id": iid, "rank": i + 1} for i, iid in enumerate(items[:top])
            ],
        }
    )


@app.get("/api/profile/<int:user_id>")
def profile(user_id: int):
    sub = tags_of(user_id)
    if sub is None or sub.empty:
        return jsonify({"user_id": user_id, "tags": [], "found": False})

    return jsonify(
        {
            "user_id": user_id,
            "found": True,
            "tags": [
                {
                    "type": str(r["tag_type"]),
                    "value": str(r["tag_value"]),
                    "weight": round(float(r["weight"]), 4),
                }
                for _, r in sub.iterrows()
            ],
        }
    )


_stats_cache: dict | None = None


@app.get("/api/stats")
def stats():
    """统计接口：结果只依赖离线数据，首次计算后缓存，避免每次请求都扫全量标签表。"""
    global _stats_cache
    if _stats_cache is not None:
        return jsonify(_stats_cache)

    recs = load_recs()
    tags = load_tags()

    tag_dist: dict[str, dict[str, int]] = {}
    # 一次 groupby 拿到全部计数，比逐组遍历快一个数量级
    counts = tags.groupby(["tag_type", "tag_value"]).size()
    for (ttype, tvalue), n in counts.items():
        tag_dist.setdefault(str(ttype), {})[str(tvalue)] = int(n)

    _stats_cache = {
        "users_with_recs": len(recs),
        "total_rec_items": int(sum(len(v) for v in recs.values())),
        "tag_records": int(len(tags)),
        "tag_distribution": tag_dist,
    }
    return jsonify(_stats_cache)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    print(f"数据目录: {RECS_DIR}")
    print(f"标签目录: {TAGS_DIR}")
    print(f"启动: http://127.0.0.1:{port}")
    app.run(host="127.0.0.1", port=port, debug=False)
