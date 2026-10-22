-- ============================================================
-- Hive 数据仓库建表脚本
-- 分层：ODS（原始层） -> DWD（明细层） -> DWS（画像/聚合层）
-- 与论文表 4-1 ~ 4-6 对应
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce
COMMENT '电商用户行为分析与推荐系统数据仓库'
LOCATION '/warehouse/ecommerce';

USE ecommerce;

-- ---------- ODS：原始行为日志（外部表，按日分区） ----------
CREATE EXTERNAL TABLE IF NOT EXISTS ods_behavior_log (
    user_id       BIGINT     COMMENT '用户ID',
    item_id       BIGINT     COMMENT '商品ID',
    category_id   BIGINT     COMMENT '品类ID',
    behavior_type TINYINT    COMMENT '行为类型：1浏览 2收藏 3加购 4下单',
    behavior_time TIMESTAMP  COMMENT '行为发生时间'
)
COMMENT '清洗后的用户行为明细'
PARTITIONED BY (dt STRING COMMENT '分区日期 yyyy-MM-dd')
STORED AS PARQUET
LOCATION '/warehouse/ecommerce/ods/behavior_log';

-- 分区写入后执行（或由 Spark 自动发现）
MSCK REPAIR TABLE ods_behavior_log;

-- ---------- 维表 ----------
CREATE EXTERNAL TABLE IF NOT EXISTS dim_item (
    item_id        BIGINT        COMMENT '商品ID',
    item_name      STRING        COMMENT '商品名称',
    category_id    BIGINT        COMMENT '所属品类',
    price          DECIMAL(10,2) COMMENT '商品单价',
    brand          STRING        COMMENT '品牌',
    on_shelf_time  DATE          COMMENT '上架时间'
)
COMMENT '商品维度表'
STORED AS PARQUET
LOCATION '/warehouse/ecommerce/dim/item';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_user (
    user_id       BIGINT COMMENT '用户ID',
    gender        TINYINT COMMENT '性别：0未知 1男 2女',
    age           INT    COMMENT '年龄',
    city          STRING COMMENT '所在城市',
    register_time DATE   COMMENT '注册时间'
)
COMMENT '用户维度表'
STORED AS PARQUET
LOCATION '/warehouse/ecommerce/dim/user';

-- ---------- DWS：用户画像标签 ----------
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user_tag (
    user_id    BIGINT      COMMENT '用户ID',
    tag_type   STRING      COMMENT '标签类别：基础属性/活跃度/品类偏好/价格敏感度/生命周期',
    tag_value  STRING      COMMENT '标签值',
    weight     DOUBLE      COMMENT '标签权重',
    update_dt  STRING      COMMENT '标签更新日期'
)
COMMENT '用户画像标签表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/warehouse/ecommerce/dws/user_tag';

-- ---------- ADS：推荐结果 ----------
CREATE EXTERNAL TABLE IF NOT EXISTS ads_recommend_result (
    user_id   BIGINT COMMENT '用户ID',
    item_ids  STRING COMMENT 'Top-N 商品ID列表（JSON 数组）',
    scores    STRING COMMENT '对应推荐得分（JSON 数组）',
    algorithm STRING COMMENT '算法标识：itemcf / als / hot',
    gen_dt    STRING COMMENT '推荐生成日期'
)
COMMENT '离线推荐结果表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/warehouse/ecommerce/ads/recommend_result';

-- ---------- 常用统计（样例） ----------
-- 每日各行为类型数量
SELECT dt, behavior_type, COUNT(1) AS cnt
FROM ods_behavior_log
GROUP BY dt, behavior_type
ORDER BY dt, behavior_type;

-- 品类 TOP10（按下单量）
SELECT category_id, COUNT(1) AS order_cnt
FROM ods_behavior_log
WHERE behavior_type = 4
GROUP BY category_id
ORDER BY order_cnt DESC
LIMIT 10;
