# 电商用户行为分析与个性化推荐系统

> 基于 Hadoop + Hive + Spark 的电商用户行为分析与个性化推荐系统（本科毕业设计）

基于真实的电商用户行为日志，构建 **数据清洗 → 用户画像 → 个性化推荐 → 服务接口 → 可视化** 的完整链路。系统采用离线批处理生成推荐结果、在线服务只查预计算结果的架构，兼顾数据规模与接口响应。

## 功能特性

- **数据链路**：原始日志落 HDFS，PySpark 完成清洗与特征工程，按日期分区写入 Hive 明细表
- **用户画像**：基础属性、活跃度、品类偏好、价格敏感度、生命周期 5 个维度标签
- **统计分析**：PV/UV、品类排行、转化漏斗、复购率
- **个性化推荐**：ItemCF 与 ALS（矩阵分解）双算法召回，冷启动兜底与多样性重排
- **离线评测**：Precision / Recall / Coverage / Diversity 四项指标，支持算法与参数对比
- **服务与展示**：Flask RESTful 接口 + Vue3 管理端 + ECharts 可视化大屏

## 界面预览

![可视化大屏](screenshots/dashboard_preview.png)

> 数据来自天池 UserBehavior 抽样 200 万条行为记录（30,008 名活跃用户、300,080 条推荐、333 万条画像标签）。
> 顶部：核心指标与服务状态；中部：用户活跃度分布、品类偏好 Top 10；
> 下部：热门基线与 ItemCF、ALS（10 / 50 / 100 维）的 Recall@10 与 NDCG@10 对比；
> 最下方：按用户 ID 查询画像标签与 Top 10 推荐商品。

## 技术栈

| 层次 | 技术 |
|---|---|
| 存储 | Hadoop HDFS、Hive、MySQL、Redis |
| 计算 | Spark 3.5 / PySpark |
| 建模 | pandas、NumPy、scikit-learn、implicit（ALS） |
| 服务 | Flask |
| 前端 | Vue3 + Element Plus + ECharts |
| 部署 | Docker / Docker Compose，Linux 单机伪分布式 |

## 系统架构

```mermaid
flowchart LR
    A[行为日志 / 商品 / 用户数据] --> B[HDFS 原始层]
    B --> C[PySpark 清洗与特征工程]
    C --> D[Hive 明细层<br/>按日分区]
    D --> E[画像与统计作业]
    D --> F[推荐训练作业<br/>ItemCF / ALS]
    E --> G[(MySQL 结果表)]
    F --> G
    G --> H[Flask 服务层]
    H --> I[Vue3 管理端]
    H --> J[ECharts 可视化大屏]
```

离线与在线分离：耗时计算全部离线完成，推荐结果预计算入库，在线接口只做查询。

## 快速开始

### 环境要求

- JDK 8 / 11、Python 3.10+
- Hadoop 3.3.x、Hive 3.1.x、Spark 3.5.x、MySQL 8.0
- 内存 16GB 以上（单机伪分布式）

### 步骤

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 初始化数据库
mysql -u root -p < sql/mysql_ddl.sql
hive -f sql/hive_ddl.sql

# 3. 准备数据（见 data/README.md，不要将原始数据提交到仓库）
#    将行为日志放入 data/raw/

# 4. 提交清洗作业
spark-submit spark_jobs/clean.py \
    --input data/raw/user_behavior.csv \
    --output /warehouse/ecommerce/ods/behavior_log

# 5. 启动服务
python server/app.py
```

### Windows 一键运行

清洗、画像、召回、ALS 都可以用同一个脚本跑（输出先写到临时目录，成功后再拷回项目）：

```powershell
powershell -ExecutionPolicy Bypass -File .\run_job.ps1 -Job clean     # CSV -> 分区 Parquet
powershell -ExecutionPolicy Bypass -File .\run_job.ps1 -Job profile   # 用户画像标签
powershell -ExecutionPolicy Bypass -File .\run_job.ps1 -Job itemcf    # ItemCF 召回 + 评测
powershell -ExecutionPolicy Bypass -File .\run_job.ps1 -Job als       # ALS 召回 + 参数实验
```

### 启动推荐服务

```powershell
python -m pip install flask pandas pyarrow
powershell -ExecutionPolicy Bypass -File .\run_server.ps1
```

```bash
curl http://127.0.0.1:8000/api/recommend/123?top=10
curl http://127.0.0.1:8000/api/profile/123
curl http://127.0.0.1:8000/api/stats
```

服务只读离线预计算的结果，不做在线计算，对应论文 4.1 节的「离线/在线分离」架构。

## 实验结果

> 数据：阿里天池 UserBehavior（淘宝用户行为）抽样 200 万条，覆盖 2017-11-24 ~ 2017-12-03。
> 过滤交互 <5 次的用户与曝光 <10 次的商品后：交互 43,364 条、用户 30,008、候选商品 2,669。
> 按时间切分：前 8 天训练、后 2 天测试（切分点 2017-12-03）；正样本取测试期的加购与下单行为；N=10。

| 算法 | Recall@10 | NDCG@10 | Coverage |
|---|---|---|---|
| 热门榜单（基线） | 0.0016 | 0.0006 | 0.0037 |
| ItemCF | 0.0024 | 0.0018 | 0.9981 |
| ALS rank=10 | 0.0008 | 0.0005 | 0.1263 |
| ALS rank=50 | 0.0039 | 0.0022 | 0.8018 |
| **ALS rank=100（当前最优）** | **0.0047** | **0.0031** | 1.0000 |

三项结论：

1. **个性化优于热门榜**：ItemCF 的 Recall 是基线的 1.5 倍，ALS 最优是基线的 3 倍；
2. **隐因子模型优于邻域模型**：ALS（rank=100）约为 ItemCF 的 2 倍，说明隐式反馈建模比共现相似度更能刻画稀疏交互；
3. **维度尚未到拐点**：10 → 50 → 100 单调提升，与合成数据上的"50 后回落"不同，说明真实数据的用户偏好更复杂、需要更高维度才能表达。下一步应补跑 rank=200 定位拐点。

### 参数实验：隐因子维度的影响

| 隐因子维度 | 10 | 50 | 100 |
|---|---|---|---|
| Recall@10 | 0.0008 | 0.0039 | **0.0047** |
| NDCG@10 | 0.0005 | 0.0022 | **0.0031** |
| Coverage | 0.1263 | 0.8018 | 1.0000 |

维度从 10 增到 100 时准确率持续提升，尚未观察到过拟合；覆盖率随维度上升而扩大，rank=100 时已覆盖全部 2,669 个候选商品。

> 说明：真实数据上 Recall 绝对值偏低（0.16%–0.47%）是数据集特性——2,669 个候选商品、Top-10 推荐、测试期跨度 2 天，
> 且正样本仅 1,277 条。**评测的关键是与基线的相对提升**，而非绝对值。
> 合成样本上的结果（Recall 0.0539）不可用于论文，仅用于验证代码链路。

## 目录结构

```
.
├── docker/            # Docker Compose 与集群配置
├── sql/               # Hive / MySQL 建表脚本
├── spark_jobs/        # PySpark 作业（清洗、画像、训练）
├── eval/              # 离线评测脚本与结果
├── server/            # Flask 服务层
├── web/               # Vue3 前端
├── data/              # 数据说明与小样本（原始数据不入库）
├── docs/              # 设计文档、架构图、实验记录
└── screenshots/       # 系统截图
```

## 开发进度

- [x] 需求分析与系统设计
- [x] 数据库与数据仓库模型设计
- [x] PySpark 数据清洗作业
- [x] 用户画像标签计算（活跃度 / 品类偏好 / 购买倾向 / 生命周期）
- [x] ItemCF 召回与离线评测（Recall@10 / NDCG@10 / Coverage）
- [x] ALS 训练与参数实验（rank 10/50/100，最优 rank=50）
- [x] Flask 推荐接口（推荐 / 画像 / 统计）
- [ ] Vue3 管理端与可视化大屏
- [ ] 性能测试与论文数据替换

## 数据说明

数据来源、字段定义与获取方式见 [data/README.md](data/README.md)。**原始数据集不纳入版本管理**，仓库仅保留小样本用于跑通流程。

## 相关文档

- [系统实现与实验报告](docs/系统实现与实验报告.md)：章节对照、算法口径、评测结果、工程问题记录
- 毕业设计论文：`docs/thesis.pdf`（定稿后补充）

## License

本项目仅用于学习与毕业设计，数据集版权归原始发布方所有。
