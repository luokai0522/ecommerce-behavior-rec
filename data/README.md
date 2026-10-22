# 数据说明

## 数据来源

使用阿里天池 **UserBehavior** 公开数据集（淘宝用户行为）：

- 数据集主页：https://tianchi.aliyun.com/dataset/649
- 规模：约 1 亿条行为记录，时间跨度约 3 个月（2017.11.25 – 2017.12.03）
- 原始文件无表头，字段顺序固定

## 字段定义

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | BIGINT | 用户 ID（已脱敏） |
| item_id | BIGINT | 商品 ID（已脱敏） |
| category_id | BIGINT | 品类 ID（已脱敏） |
| behavior_type | STRING | 行为类型：pv 浏览 / fav 收藏 / cart 加购 / buy 下单 |
| timestamp | BIGINT | 行为发生时间，Unix 时间戳（秒） |

清洗后 `behavior_type` 编码为 1–4，并增加 `behavior_time`、`dt` 两个字段，见 `sql/hive_ddl.sql`。

## 目录约定

```
data/
├── raw/          # 原始数据（不入库，已被 .gitignore 忽略）
├── sample/       # 小样本，用于快速跑通流程（入库）
└── processed/    # 清洗中间结果（不入库）
```

## 使用建议

```bash
# 1. 创建目录
mkdir -p data/raw data/sample

# 2. 下载并解压数据集，放入 data/raw/UserBehavior.csv

# 3. 抽样 10 万行做开发调试（PowerShell）
Get-Content data/raw/UserBehavior.csv -TotalCount 100000 |
    Set-Content data/sample/sample_100k.csv -Encoding utf8

# 4. 全量清洗（提交到 Spark）
spark-submit spark_jobs/clean.py \
    --input data/raw/UserBehavior.csv \
    --output /warehouse/ecommerce/ods/behavior_log \
    --start-ts 1511539200 --end-ts 1517414400
```

## 注意

- **原始数据不要提交到 Git**：单文件超过 100MB 会被 GitHub 拒绝，且仓库体积将无法回退。
- 数据为脱敏后的公开数据，可用于学习与研究；论文中请注明来源。
- 若改用其他数据源，请同步修改 `spark_jobs/clean.py` 中的 `RAW_SCHEMA` 与本节说明。
