"""推荐结果的多样性分析（论文 6.6 节用）。

统计每个用户 Top-10 里覆盖的类目数、单一类目最大占比，
用来回答"推荐是否过于集中在少数品类"。

用法：
    python tools/analyze_diversity.py --recs data/processed/recs_itemcf --ods data/processed/ods
"""
from __future__ import annotations

import argparse
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="推荐结果多样性分析")
    p.add_argument("--recs", default=str(BASE / "data" / "processed" / "recs_itemcf"))
    p.add_argument("--ods", default=str(BASE / "data" / "processed" / "ods"))
    p.add_argument("--sample-users", type=int, default=5000, help="抽样用户数，0 表示全部")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    import pandas as pd

    recs = pd.read_parquet(args.recs, columns=["user_id", "item_id", "rank"])
    ods = pd.read_parquet(args.ods, columns=["item_id", "category_id"])

    # 商品 -> 主类目（同一商品可能出现在多个类目下，取出现次数最多的）
    item_cat = (
        ods.groupby(["item_id", "category_id"]).size().reset_index(name="n")
        .sort_values("n", ascending=False)
        .drop_duplicates("item_id")[["item_id", "category_id"]]
    )

    users = recs["user_id"].unique()
    if args.sample_users and len(users) > args.sample_users:
        users = pd.Series(users).sample(args.sample_users, random_state=42).values

    sub = recs[recs["user_id"].isin(users)].merge(item_cat, on="item_id", how="left")
    sub["category_id"] = sub["category_id"].fillna(-1)

    stats = []
    for _, grp in sub.groupby("user_id"):
        cats = grp["category_id"].tolist()
        top_share = grp["category_id"].value_counts().iloc[0] / len(cats)
        stats.append({"n_cat": len(set(cats)), "top_share": top_share, "k": len(cats)})

    df = pd.DataFrame(stats)
    print(f"[diversity] 分析用户数: {len(df):,}，平均推荐长度: {df['k'].mean():.2f}")
    print(f"[diversity] Top-10 平均覆盖类目数: {df['n_cat'].mean():.2f}")
    print(f"[diversity] 单一类目最大占比（均值）: {df['top_share'].mean():.3f}")
    print("[diversity] 覆盖类目数分布:")
    for k, c in df["n_cat"].value_counts().sort_index().items():
        print(f"  {int(k)} 个类目: {c:,} 用户 ({c/len(df)*100:.1f}%)")
    print(f"[diversity] 只含单一类目的用户占比: {(df['n_cat'] == 1).mean()*100:.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
