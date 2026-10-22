# Docker 环境

计划用 Docker Compose 在单机启动伪分布式环境，服务清单：

| 服务 | 镜像 | 端口 | 说明 |
|---|---|---|---|
| namenode | bde2020/hadoop-namenode | 9870 | HDFS NameNode |
| datanode | bde2020/hadoop-datanode | 9864 | HDFS DataNode |
| hive-metastore | bde2020/hive | 9083 | Hive 元数据服务 |
| spark-master | bitnami/spark | 8080 | Spark Master |
| spark-worker | bitnami/spark | 8081 | Spark Worker |
| mysql | mysql:8.0 | 3306 | 业务库 |

启动后按 README 的「快速开始」执行建表与清洗作业。

> 若环境反复启动失败，可降级为本地 Spark（`local[*]`）+ 本地 Parquet + MySQL，
> 论文中的分层架构仍然成立，把「可平滑迁移至集群」作为设计特点说明即可。
