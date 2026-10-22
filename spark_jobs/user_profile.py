"""用户画像作业：从行为明细计算多维标签，输出 long 表（user_id, tag_type, tag_value, weight）。

维度与论文 4.4 节对应：
    基础属性    —— 需要用户维表（--user-dim），缺省跳过
    活跃度      —— 按行为次数分位切分
    品类偏好    —— 加权行为最多的品类及其占比
    购买倾向    —— 下单/加购在全部行为中的占比
    价格敏感度  —— 需要商品维表的价格字段（--item-dim），缺省跳过
    生命周期    —— 首次/末次行为跨度 + 距今未活跃天数

用法：
    spark-submit spark_jobs/profile.py \
        --input  data/processed/ods \
        --output data/processed/user_tag \
        [--item-dim data/raw/item.csv] [--user-dim data/raw/user.csv]
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

BEHAVIOR_WEIGHT = {1: 1, 2: 2, 3: 3, 4: 4}  # pv/fav/cart/buy


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="用户画像标签计算")
    p.add_argument("--input", required=True, help="清洗后的 Parquet 目录")
    p.add_argument("--output", required=True, help="标签输出目录")
    p.add_argument("--item-dim", default=None, help="商品维表 CSV（item_id,price）")
    p.add_argument("--user-dim", default=None, help="用户维表 CSV（user_id,gender,age,city）")
    p.add_argument("--shuffle-partitions", type=int, default=8)
    return p.parse_args(argv)


def build_spark(shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder.appName("ecommerce-user-profile")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def tag_activity(df: DataFrame) -> DataFrame:
    """活跃度：按行为次数四分位切分。"""
    cnt = df.groupBy("user_id").agg(F.count("*").alias("cnt"))
    q1, q2, q3 = cnt.approxQuantile("cnt", [0.25, 0.50, 0.80], 0.01)
    return (
        cnt.withColumn(
            "tag_value",
            F.when(F.col("cnt") >= q3, "高活跃")
            .when(F.col("cnt") >= q2, "中活跃")
            .when(F.col("cnt") >= q1, "低活跃")
            .otherwise("沉默"),
        )
        .select(
            "user_id",
            F.lit("活跃度").alias("tag_type"),
            F.col("tag_value"),
            F.round(F.col("cnt") / F.lit(max(q3, 1.0)), 4).alias("weight"),
        )
    )


def tag_category(df: DataFrame) -> DataFrame:
    """品类偏好：加权行为最多的品类，权重为该品类占比。"""
    wcol = F.create_map(*[F.lit(x) for pair in BEHAVIOR_WEIGHT.items() for x in pair])
    weighted = df.withColumn("w", wcol[F.col("behavior_type")])
    per_cat = weighted.groupBy("user_id", "category_id").agg(F.sum("w").alias("w"))
    total = per_cat.groupBy("user_id").agg(F.sum("w").alias("total"))
    share = per_cat.join(total, "user_id").withColumn(
        "share", F.col("w") / F.col("total")
    )
    rank = Window.partitionBy("user_id").orderBy(F.desc("share"), F.asc("category_id"))
    return (
        share.withColumn("rn", F.row_number().over(rank))
        .filter(F.col("rn") == 1)
        .select(
            "user_id",
            F.lit("品类偏好").alias("tag_type"),
            F.concat(F.lit("品类"), F.col("category_id").cast("string")).alias("tag_value"),
            F.round("share", 4).alias("weight"),
        )
    )


def tag_intent(df: DataFrame) -> DataFrame:
    """购买倾向：加购与下单占总行为的比例。"""
    agg = df.groupBy("user_id").agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("behavior_type").isin(3, 4), 1).otherwise(0)).alias("intent"),
        F.sum(F.when(F.col("behavior_type") == 4, 1).otherwise(0)).alias("buy"),
    )
    ratio = agg.withColumn("r", F.col("intent") / F.col("total")).withColumn(
        "br", F.col("buy") / F.col("total")
    )
    return ratio.select(
        "user_id",
        F.lit("购买倾向").alias("tag_type"),
        F.when(F.col("br") >= 0.05, "高意向")
        .when(F.col("r") >= 0.10, "中意向")
        .otherwise("低意向")
        .alias("tag_value"),
        F.round(F.col("r"), 4).alias("weight"),
    )


def tag_lifecycle(df: DataFrame) -> DataFrame:
    """生命周期：首次-末次跨度 + 距今未活跃天数。"""
    max_dt = df.agg(F.max("dt")).first()[0]
    life = df.groupBy("user_id").agg(
        F.min("dt").alias("first_dt"), F.max("dt").alias("last_dt")
    )
    life = life.withColumn(
        "span", F.datediff(F.to_date("last_dt"), F.to_date("first_dt"))
    ).withColumn(
        "recency", F.datediff(F.lit(max_dt).cast("date"), F.to_date("last_dt"))
    )
    return life.select(
        "user_id",
        F.lit("生命周期").alias("tag_type"),
        F.when(F.col("recency") >= 14, "流失")
        .when(F.col("span") <= 1, "新用户")
        .when(F.col("span") <= 7, "成长期")
        .otherwise("成熟期")
        .alias("tag_value"),
        F.round((F.col("span") + 1) / F.lit(30.0), 4).alias("weight"),
    )


def tag_price(df: DataFrame, item_dim_path: str) -> DataFrame | None:
    """价格敏感度：互动商品均价与全站均价对比。"""
    items = (
        df.sparkSession.read.option("header", True)
        .option("inferSchema", True)
        .csv(item_dim_path)
        .select("item_id", "price")
    )
    if items.limit(1).count() == 0:
        return None
    global_avg = items.agg(F.avg("price")).first()[0]
    joined = df.join(items, "item_id", "inner")
    per_user = joined.groupBy("user_id").agg(F.avg("price").alias("avg_price"))
    return per_user.select(
        "user_id",
        F.lit("价格敏感度").alias("tag_type"),
        F.when(F.col("avg_price") < global_avg * 0.7, "高敏感")
        .when(F.col("avg_price") > global_avg * 1.3, "低敏感")
        .otherwise("中敏感")
        .alias("tag_value"),
        F.round(F.col("avg_price") / F.lit(global_avg), 4).alias("weight"),
    )


def tag_base(df: DataFrame, user_dim_path: str) -> list[DataFrame]:
    """基础属性：性别 / 年龄段 / 城市。"""
    users = (
        df.sparkSession.read.option("header", True)
        .option("inferSchema", True)
        .csv(user_dim_path)
    )
    cols = set(users.columns)
    out = []
    if "gender" in cols:
        out.append(
            users.select(
                "user_id",
                F.lit("性别").alias("tag_type"),
                F.when(F.col("gender") == 1, "男")
                .when(F.col("gender") == 2, "女")
                .otherwise("未知")
                .alias("tag_value"),
                F.lit(1.0).alias("weight"),
            )
        )
    if "age" in cols:
        out.append(
            users.select(
                "user_id",
                F.lit("年龄段").alias("tag_type"),
                F.when(F.col("age") < 18, "18以下")
                .when(F.col("age") < 25, "18-24")
                .when(F.col("age") < 35, "25-34")
                .when(F.col("age") < 45, "35-44")
                .otherwise("45以上")
                .alias("tag_value"),
                F.lit(1.0).alias("weight"),
            )
        )
    if "city" in cols:
        out.append(
            users.select(
                "user_id",
                F.lit("城市").alias("tag_type"),
                F.col("city").cast("string").alias("tag_value"),
                F.lit(1.0).alias("weight"),
            )
        )
    return out


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spark = build_spark(args.shuffle_partitions)
    try:
        df = spark.read.parquet(args.input).cache()
        users = df.select("user_id").distinct().count()
        rows = df.count()
        print(f"[profile] 输入记录 {rows:,}，用户 {users:,}")

        frames = [tag_activity(df), tag_category(df), tag_intent(df), tag_lifecycle(df)]

        if args.item_dim:
            t = tag_price(df, args.item_dim)
            if t is not None:
                frames.append(t)
                print("[profile] 已计算：价格敏感度")
            else:
                print("[profile] 商品维表为空，跳过价格敏感度")

        if args.user_dim:
            extra = tag_base(df, args.user_dim)
            frames.extend(extra)
            print(f"[profile] 已计算基础属性 {len(extra)} 项")

        tag_df = frames[0]
        for f in frames[1:]:
            tag_df = tag_df.unionByName(f)
        tag_df = tag_df.withColumn("dt", F.current_date().cast("string")).cache()

        total = tag_df.count()
        print(f"[profile] 标签条数 {total:,}（平均每人 {total / max(users, 1):.1f} 个）")

        summary = (
            tag_df.groupBy("tag_type", "tag_value")
            .count()
            .orderBy("tag_type", F.desc("count"))
            .collect()
        )
        print("[profile] 标签分布（每类只显示前 6 项）：")
        grouped: dict[str, list] = {}
        for r in summary:
            grouped.setdefault(r["tag_type"], []).append((r["tag_value"], r["count"]))
        for tag_type, items in grouped.items():
            print(f"  【{tag_type}】共 {len(items)} 种取值")
            for value, count in items[:6]:
                pct = count / users * 100
                print(f"    - {value:<10} {count:>8,} ({pct:5.1f}%)")
            if len(items) > 6:
                rest = sum(c for _, c in items[6:])
                print(f"    - …其余 {len(items) - 6} 项合计 {rest:>8,}")

        (
            tag_df.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(args.output)
        )
        print(f"[profile] 已写入: {args.output}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
