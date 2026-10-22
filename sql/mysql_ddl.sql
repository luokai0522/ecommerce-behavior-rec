-- ============================================================
-- MySQL 业务库建表脚本
-- 存放画像查询、推荐结果与系统管理数据（在线服务直接读取）
-- 与论文表 4-2 ~ 4-5 对应
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce_rec
    DEFAULT CHARACTER SET utf8mb4
    COLLATE utf8mb4_general_ci;

USE ecommerce_rec;

-- ---------- 用户画像标签 ----------
CREATE TABLE IF NOT EXISTS user_tag (
    id          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    user_id     BIGINT       NOT NULL COMMENT '用户ID',
    tag_type    VARCHAR(30)  NOT NULL COMMENT '标签类别',
    tag_value   VARCHAR(50)  NOT NULL COMMENT '标签值',
    weight      DOUBLE       DEFAULT 1.0 COMMENT '标签权重',
    update_date DATE         NOT NULL COMMENT '标签更新日期',
    PRIMARY KEY (id),
    KEY idx_user (user_id),
    KEY idx_type (tag_type)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '用户画像标签表';

-- ---------- 推荐结果 ----------
CREATE TABLE IF NOT EXISTS recommend_result (
    id         BIGINT      NOT NULL AUTO_INCREMENT COMMENT '主键',
    user_id    BIGINT      NOT NULL COMMENT '用户ID',
    item_ids   TEXT        NOT NULL COMMENT 'Top-N 商品ID列表（JSON）',
    scores     TEXT        COMMENT '对应推荐得分（JSON）',
    algorithm  VARCHAR(20) NOT NULL COMMENT '算法标识：itemcf / als / hot',
    gen_date   DATE        NOT NULL COMMENT '生成日期',
    PRIMARY KEY (id),
    UNIQUE KEY uk_user_algo_date (user_id, algorithm, gen_date),
    KEY idx_algo_date (algorithm, gen_date)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '离线推荐结果表';

-- ---------- 商品 / 用户（从 Hive 同步的在线副本） ----------
CREATE TABLE IF NOT EXISTS item (
    item_id       BIGINT       NOT NULL COMMENT '商品ID',
    item_name     VARCHAR(200) DEFAULT NULL COMMENT '商品名称',
    category_id   BIGINT       DEFAULT NULL COMMENT '品类ID',
    price         DECIMAL(10,2) DEFAULT NULL COMMENT '单价',
    brand         VARCHAR(80)  DEFAULT NULL COMMENT '品牌',
    on_shelf_time DATE         DEFAULT NULL COMMENT '上架时间',
    PRIMARY KEY (item_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '商品表';

CREATE TABLE IF NOT EXISTS user (
    user_id       BIGINT      NOT NULL COMMENT '用户ID',
    gender        TINYINT     DEFAULT 0 COMMENT '性别：0未知 1男 2女',
    age           INT         DEFAULT NULL COMMENT '年龄',
    city          VARCHAR(50) DEFAULT NULL COMMENT '城市',
    register_time DATE        DEFAULT NULL COMMENT '注册时间',
    PRIMARY KEY (user_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '用户表';

-- ---------- 系统管理 ----------
CREATE TABLE IF NOT EXISTS sys_user (
    id            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    username      VARCHAR(50)  NOT NULL COMMENT '登录名',
    password_hash VARCHAR(128) NOT NULL COMMENT '密码哈希（bcrypt）',
    role          VARCHAR(20)  NOT NULL DEFAULT 'operator' COMMENT '角色：admin / operator',
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_username (username)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '系统用户表';

-- ---------- 离线任务执行记录 ----------
CREATE TABLE IF NOT EXISTS task_log (
    id          BIGINT      NOT NULL AUTO_INCREMENT COMMENT '主键',
    task_name   VARCHAR(80) NOT NULL COMMENT '任务名',
    status      VARCHAR(20) NOT NULL COMMENT '状态：running / success / failed',
    start_time  DATETIME    NOT NULL COMMENT '开始时间',
    end_time    DATETIME    DEFAULT NULL COMMENT '结束时间',
    row_count   BIGINT      DEFAULT NULL COMMENT '处理行数',
    message     TEXT        COMMENT '备注 / 异常信息',
    PRIMARY KEY (id),
    KEY idx_task_time (task_name, start_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '任务执行记录表';
