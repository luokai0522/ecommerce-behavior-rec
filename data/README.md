# 数据说明

## 两种数据，请勿混淆

| 目录 | 内容 | 是否入库 |
|---|---|---|
| `data/sample/` | **合成样本** `sample_100k.csv`（12 万条，脚本生成，含潜在品类偏好） | ✅ 入库，用于跑通流程 |
| `data/raw/` | **真实数据**（阿里天池 UserBehavior 原始 CSV，需自行下载） | ❌ 被 .gitignore 忽略 |
| `data/processed/` | 清洗与实验结果（Parquet、SQLite） | ❌ 被 .gitignore 忽略 |

> **`data/sample/sample_100k.csv` 不是真实数据的抽样**，而是按真实数据格式生成的合成数据：
> 文件名沿用 100k 是历史原因，实际 12 万行。它只用于在没有真实数据时快速跑通
> `clean → profile → itemcf → als` 全链路，**不能用来产出论文里的实验数值**。

## 真实数据来源

阿里天池 **UserBehavior** 公开数据集（淘宝用户行为）：

- 数据集主页：https://tianchi.aliyun.com/dataset/649
- 规模：约 1 亿条行为记录，时间跨度 2017-11-25 ~ 2017-12-03（9 天）
- 原始文件无表头，字段顺序固定

### 字段定义

| 字段 | 类型 | 说明 |
|---|---|---|
| user_id | BIGINT | 用户 ID（已脱敏） |
| item_id | BIGINT | 商品 ID（已脱敏） |
| category_id | BIGINT | 品类 ID（已脱敏） |
| behavior_type | STRING | 行为类型：pv 浏览 / fav 收藏 / cart 加购 / buy 下单 |
| timestamp | BIGINT | 行为发生时间，Unix 时间戳（秒） |

清洗后 `behavior_type` 编码为 1–4，并增加 `behavior_time`、`dt` 两个字段，见 `sql/hive_ddl.sql`。

## 使用真实数据

```powershell
# 1. 下载 UserBehavior.csv.zip（约 906MB）并解压到 data/raw/
#    推荐先完成天池的学生认证，可免审批下载

# 2. 等距抽样 200 万行（保留完整时间跨度；不要用 Get-Content 取前 N 行，
#    因为原始数据按时间戳递增，取前 N 行只会落在第一天）
python tools\sample_dataset.py data\raw\UserBehavior.csv data\raw\sample_2m.csv --rows 2000000

# 3. 用真实数据跑清洗（不传 -DataFile 时默认使用合成样本）
powershell -ExecutionPolicy Bypass -File .\run_job.ps1 -Job clean -DataFile data\raw\sample_2m.csv
```

清洗默认时间窗为 `2017-11-24 ~ 2017-12-04`（本地时间，UTC+8），用于过滤原始数据中
1905/2036 年这类脏时间戳。换数据集时用 `--start-ts` / `--end-ts` 覆盖。

## 注意

- **原始数据不要提交到 Git**：单文件超过 100MB 会被 GitHub 拒绝，且仓库体积将无法回退。
- 数据为脱敏后的公开数据，可用于学习与研究；论文中请注明来源。
- 若改用其他数据源，请同步修改 `spark_jobs/clean.py` 中的 `RAW_SCHEMA` 与本文档。
