"""ItemCF 召回与离线评测：按时间切分训练/测试集，输出 Top-N 推荐并计算指标。

评测指标（论文 6.3 节）：Recall@N、NDCG@N、Coverage、以及热门榜基线对比。

用法：
    spark-submit spark_jobs/itemcf.py \
        --input  data/processed/ods \
        --output data/processed/recs_itemcf \
        [--top-k 50] [--top-n 10] [--test-days 2] [--eval-users 2000]

设计要点：
  * 按 dt 切分，训练集只包含测试期之前的行为，避免用未来预测过去；
  * 每用户最多保留 max-user-items 个商品参与相似度计算，控制共现矩阵规模；
  * 相似度采用余弦形式，并用行为权重（浏览1/收藏2/加购3/下单4）加权。
"""
from __future__ import annotations

import argparse
import math
import sys

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

BEHAVIOR_WEIGHT = {1: 1, 2: 2, 3: 3, 4: 4}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ItemCF 召回与离线评测")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--top-k", type=int, default=50, help="每个商品的近邻数")
    p.add_argument("--top-n", type=int, default=10, help="推荐列表长度")
    p.add_argument("--test-days", type=int, default=2, help="测试集天数（最后 N 天）")
    p.add_argument("--eval-users", type=int, default=2000, help="评测抽样用户数")
    p.add_argument("--test-behaviors", default="3,4", help="作为正样本的行为类型，逗号分隔")
    p.add_argument("--max-user-items", type=int, default=50, help="每用户参与计算的最大商品数")
    p.add_argument("--min-user", type=int, default=5, help="用户最少交互数，低于此值不参与训练")
    p.add_argument("--min-item", type=int, default=10, help="商品最少被交互次数，低于此值不参与训练")
    p.add_argument("--shuffle-partitions", type=int, default=8)
    return p.parse_args(argv)


def build_spark(shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder.appName("ecommerce-itemcf")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.autoBroadcastJoinThreshold", -1)  # 避免探针阶段自动广播
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def weighted_interactions(df: DataFrame) -> DataFrame:
    wcol = F.create_map(*[F.lit(x) for pair in BEHAVIOR_WEIGHT.items() for x in pair])
    return (
        df.withColumn("w", wcol[F.col("behavior_type")])
        .groupBy("user_id", "item_id")
        .agg(F.max("w").alias("w"))
    )


def filter_interactions(train: DataFrame, min_user: int, min_item: int) -> DataFrame:
    """过滤冷启动用户与长尾商品，保证与 ALS 使用同一份训练数据。"""
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


def split_by_time(df: DataFrame, test_days: int) -> tuple[DataFrame, DataFrame, str]:
    days = [r["dt"] for r in df.select("dt").distinct().orderBy("dt").collect()]
    if len(days) <= test_days:
        raise SystemExit(f"日期数不足：共 {len(days)} 天，无法留出 {test_days} 天测试集")
    boundary = days[-test_days]          # 测试期第一天
    train = df.filter(F.col("dt") < boundary)
    test = df.filter(F.col("dt") >= boundary)
    return train, test, boundary


def item_similarity(inter: DataFrame, top_k: int) -> DataFrame:
    a = inter.select(F.col("user_id"), F.col("item_id").alias("i"), F.col("w").alias("wi"))
    b = inter.select(F.col("user_id"), F.col("item_id").alias("j"), F.col("w").alias("wj"))
    pairs = (
        a.join(b, "user_id")
        .filter(F.col("i") < F.col("j"))
        .groupBy("i", "j")
        .agg(F.sum(F.col("wi") * F.col("wj")).alias("dot"))
    )
    norm = inter.groupBy("item_id").agg(F.sum(F.col("w") * F.col("w")).alias("sq"))
    pairs = (
        pairs.join(norm.withColumnRenamed("item_id", "i").withColumnRenamed("sq", "si"), "i")
        .join(norm.withColumnRenamed("item_id", "j").withColumnRenamed("sq", "sj"), "j")
        .withColumn("sim", F.col("dot") / F.sqrt(F.col("si") * F.col("sj")))
        .select("i", "j", "sim")
    )
    # 对称化：既要有 i->j，也要有 j->i
    both = pairs.select("i", "j", "sim").unionByName(
        pairs.select(F.col("j").alias("i"), F.col("i").alias("j"), "sim")
    )
    rank = Window.partitionBy("i").orderBy(F.desc("sim"))
    return (
        both.withColumn("rn", F.row_number().over(rank))
        .filter(F.col("rn") <= top_k)
        .select("i", "j", F.col("sim").alias("sim"))
    )


def recommend(inter: DataFrame, sim: DataFrame, top_n: int) -> DataFrame:
    scored = (
        inter.select(F.col("user_id"), F.col("item_id").alias("i"), F.col("w"))
        .join(sim, "i")
        .groupBy("user_id", F.col("j").alias("item_id"))
        .agg(F.sum(F.col("w") * F.col("sim")).alias("score"))
    )
    seen = inter.select("user_id", F.col("item_id").alias("seen_item"))
    scored = scored.join(
        seen,
        (scored["user_id"] == seen["user_id"]) & (scored["item_id"] == seen["seen_item"]),
        "left_anti",
    )
    rank = Window.partitionBy("user_id").orderBy(F.desc("score"), F.asc("item_id"))
    return (
        scored.withColumn("rn", F.row_number().over(rank))
        .filter(F.col("rn") <= top_n)
        .select("user_id", "item_id", F.col("rn").alias("rank"), F.col("score"))
    )


def popularity_baseline(train: DataFrame, top_n: int) -> DataFrame:
    top = (
        train.groupBy("item_id")
        .agg(F.count("*").alias("freq"))
        .orderBy(F.desc("freq"), F.asc("item_id"))
        .limit(top_n)
        .collect()
    )
    items = [r["item_id"] for r in top]
    users = train.select("user_id").distinct()
    rows = [(r["user_id"], it, i + 1) for r in users.collect() for i, it in enumerate(items)]
    return train.sparkSession.createDataFrame(rows, "user_id long, item_id long, rank int")


def evaluate(
    recs: DataFrame, test: DataFrame, catalog_size: int, eval_users: int, top_n: int, prefix: str
) -> dict:
    truth = (
        test.groupBy("user_id")
        .agg(F.collect_set("item_id").alias("items"))
        .withColumn("n_true", F.size("items"))
    )
    pool = truth.orderBy("user_id").limit(eval_users).cache()
    joined = pool.join(
        recs.groupBy("user_id").agg(F.collect_list(F.struct("rank", "item_id")).alias("recs")),
        "user_id",
        "left",
    )

    def score_row(row) -> tuple[float, float, int, int]:
        items = set(row["items"])
        recs = sorted(row["recs"] or [], key=lambda x: x["rank"])
        hits = 0
        dcg = 0.0
        for pos, rec in enumerate(recs[:top_n], start=1):
            if rec["item_id"] in items:
                hits += 1
                dcg += 1.0 / math.log2(pos + 1)
        idcg = sum(
            1.0 / math.log2(i + 1) for i in range(1, min(len(items), top_n) + 1)
        )
        recall = hits / len(items) if items else 0.0
        ndcg = dcg / idcg if idcg else 0.0
        return recall, ndcg, hits, len(items)

    stats = [score_row(r) for r in joined.collect()]
    n = max(len(stats), 1)
    recall = sum(s[0] for s in stats) / n
    ndcg = sum(s[1] for s in stats) / n
    coverage = recs.select("item_id").distinct().count() / max(catalog_size, 1)
    print(
        f"[{prefix}] Recall@{top_n}={recall:.4f}  NDCG@{top_n}={ndcg:.4f}  "
        f"Coverage={coverage:.4f}  评测用户={len(stats):,}"
    )
    pool.unpersist()
    return {"recall": recall, "ndcg": ndcg, "coverage": coverage}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spark = build_spark(args.shuffle_partitions)
    try:
        df = spark.read.parquet(args.input).cache()
        print(f"[itemcf] 输入记录 {df.count():,}")

        train_raw, test_raw, boundary = split_by_time(df, args.test_days)
        print(f"[itemcf] 切分点 {boundary}，训练集 {train_raw.count():,}，测试集 {test_raw.count():,}")

        train = weighted_interactions(train_raw)
        before = train.count()
        train = filter_interactions(train, args.min_user, args.min_item).cache()
        print(
            f"[itemcf] 过滤冷启动：交互 {before:,} -> {train.count():,}"
            f"（用户>={args.min_user} 次，商品>={args.min_item} 次）"
        )
        # 控制每用户参与计算的商品数
        rank = Window.partitionBy("user_id").orderBy(F.desc("w"), F.asc("item_id"))
        train = (
            train.withColumn("rn", F.row_number().over(rank))
            .filter(F.col("rn") <= args.max_user_items)
            .drop("rn")
            .cache()
        )
        print(f"[itemcf] 交互对 {train.count():,}")

        sim = item_similarity(train, args.top_k).cache()
        print(f"[itemcf] 相似度对 {sim.count():,}（top-k={args.top_k}）")

        recs = recommend(train, sim, args.top_n).cache()
        n_users = recs.select("user_id").distinct().count()
        print(f"[itemcf] 覆盖用户 {n_users:,}，推荐记录 {recs.count():,}")

        # 测试集正样本：默认只取加购(3)与下单(4)，浏览噪声太大
        behaviours = [int(x) for x in args.test_behaviors.split(",") if x.strip()]
        test = (
            test_raw.filter(F.col("behavior_type").isin(behaviours))
            .select("user_id", "item_id")
            .distinct()
        )
        catalog = train.select("item_id").distinct().count()
        print(f"[itemcf] ---- 离线评测（正样本行为={behaviours}）----")
        evaluate(recs, test, catalog, args.eval_users, args.top_n, "itemcf")
        evaluate(
            popularity_baseline(train, args.top_n), test, catalog, args.eval_users, args.top_n,
            "热门基线",
        )

        (recs.write.mode("overwrite").parquet(args.output))
        print(f"[itemcf] 已写入: {args.output}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
