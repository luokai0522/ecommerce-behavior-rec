"""行为日志清洗作业：CSV -> 校验 -> 去重 -> 行为编码 -> 按日分区 Parquet。

适配阿里天池 UserBehavior 数据集，原始文件无表头，字段顺序为：
    user_id, item_id, category_id, behavior_type, timestamp

用法：
    spark-submit spark_jobs/clean.py \
        --input data/raw/UserBehavior.csv \
        --output /warehouse/ecommerce/ods/behavior_log \
        [--start-ts 1511539200] [--end-ts 1517414400]

输出：Parquet 目录，按 dt（yyyy-MM-dd）分区，字段 behavior_type 编码为 1-4。
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# 行为类型编码：与论文表 4-1 一致
BEHAVIOR_CODE = {"pv": 1, "fav": 2, "cart": 3, "buy": 4}

RAW_SCHEMA = StructType(
    [
        StructField("user_id", LongType(), nullable=True),
        StructField("item_id", LongType(), nullable=True),
        StructField("category_id", LongType(), nullable=True),
        StructField("behavior_type", StringType(), nullable=True),
        StructField("ts", LongType(), nullable=True),
    ]
)

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="电商行为日志清洗")
    parser.add_argument("--input", required=True, help="原始 CSV 路径或 HDFS 路径")
    parser.add_argument("--output", required=True, help="输出 Parquet 路径")
    parser.add_argument("--start-ts", type=int, default=None, help="起始时间戳（秒）")
    parser.add_argument("--end-ts", type=int, default=None, help="结束时间戳（秒）")
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=8,
        help="shuffle 分区数（本地模式用小值；集群上可调到 200）",
    )
    return parser.parse_args(argv)


def build_spark(shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder.appName("ecommerce-behavior-clean")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    # 本地模式下调低日志级别，否则一个作业能刷出几十 MB 的 INFO 日志
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "false").schema(RAW_SCHEMA).csv(path)


def clean(df: DataFrame, start_ts: int | None, end_ts: int | None) -> DataFrame:
    code_col = F.create_map(
        *[F.lit(x) for pair in BEHAVIOR_CODE.items() for x in pair]
    )

    out = (
        df.dropna(subset=["user_id", "item_id", "category_id", "behavior_type", "ts"])
        .withColumn("behavior_type", code_col[F.col("behavior_type")])
        .filter(F.col("behavior_type").isNotNull())
        .filter(F.col("user_id") > 0)
        .filter(F.col("item_id") > 0)
        .withColumn("behavior_time", F.to_timestamp(F.from_unixtime("ts")))
        .filter(F.col("behavior_time").isNotNull())
    )

    if start_ts is not None:
        out = out.filter(F.col("ts") >= start_ts)
    if end_ts is not None:
        out = out.filter(F.col("ts") < end_ts)

    out = (
        out.withColumn("dt", F.date_format("behavior_time", "yyyy-MM-dd"))
        .select(
            "user_id",
            "item_id",
            "category_id",
            "behavior_type",
            "behavior_time",
            "dt",
        )
        .dropDuplicates(
            ["user_id", "item_id", "category_id", "behavior_type", "behavior_time"]
        )
    )
    return out


def summarize(df: DataFrame) -> None:
    """打印清洗结果，便于人工核对与写进实验记录。"""
    rows = df.count()
    print(f"[clean] 清洗后记录数: {rows:,}")

    by_type = (
        df.groupBy("behavior_type")
        .count()
        .orderBy("behavior_type")
        .collect()
    )
    names = {v: k for k, v in BEHAVIOR_CODE.items()}
    for row in by_type:
        label = names.get(row["behavior_type"], str(row["behavior_type"]))
        pct = row["count"] / rows * 100 if rows else 0
        print(f"  - {label:>4}: {row['count']:>12,} ({pct:5.2f}%)")

    stats = df.agg(
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("item_id").alias("items"),
        F.countDistinct("category_id").alias("categories"),
        F.min("dt").alias("min_dt"),
        F.max("dt").alias("max_dt"),
    ).collect()[0]
    print(
        f"[clean] 用户 {stats['users']:,} / 商品 {stats['items']:,} / "
        f"品类 {stats['categories']:,}"
    )
    print(f"[clean] 时间范围: {stats['min_dt']} ~ {stats['max_dt']}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spark = build_spark(args.shuffle_partitions)
    try:
        raw = read_raw(spark, args.input)
        cleaned = clean(raw, args.start_ts, args.end_ts).cache()
        summarize(cleaned)
        (
            cleaned.write.mode("overwrite")
            .partitionBy("dt")
            .option("compression", "snappy")
            .parquet(args.output)
        )
        print(f"[clean] 已写入: {args.output}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
