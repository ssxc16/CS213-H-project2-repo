-- 创建 Users 表
CREATE TABLE IF NOT EXISTS Users
(
    mid      BIGINT PRIMARY KEY,
    name     VARCHAR(50),
    sex      VARCHAR(10),
    birthday VARCHAR(20),
    level    SMALLINT,
    sign     VARCHAR(100),
    coin     INT,
    identity VARCHAR(10),
    password VARCHAR(32),
    qq       VARCHAR(50),
    wechat   VARCHAR(50)
);

-- 创建 Videos 表

CREATE TABLE IF NOT EXISTS Videos
(
    bv           VARCHAR(255) PRIMARY KEY,
    title        VARCHAR(50),
    ownerMid     BIGINT,
    ownerName    VARCHAR(20),
    commitTime   TIMESTAMP,
    reviewTime   TIMESTAMP,
    publicTime   TIMESTAMP,
    duration     FLOAT,
    description  TEXT,
    reviewer     BIGINT,
    viewCount    BIGINT,
    coinCount    BIGINT,
    likeCount    BIGINT,
    collectCount BIGINT,
    danmuCount   BIGINT,
    watchTime    DOUBLE PRECISION
);

-- 创建 Danmu 表

CREATE TABLE IF NOT EXISTS Danmu
(
    id       BIGSERIAL PRIMARY KEY,
    bv       VARCHAR(255),
    mid      BIGINT,
    "time"   FLOAT,
    content  TEXT,
    postTime TIMESTAMP
);

-- 创建 DanmuRela 表

CREATE TABLE IF NOT EXISTS DanmuRela
(
    mid BIGINT,
    id  BIGINT,
    PRIMARY KEY (mid, id)
);

-- 创建 FollRela 表
CREATE TABLE IF NOT EXISTS FollRela
(
    followerMid BIGINT,
    followeeMid BIGINT,
    PRIMARY KEY (followerMid, followeeMid)
);

-- 创建 ViewRela 表
CREATE TABLE IF NOT EXISTS ViewRela
(
    viewerMid BIGINT,
    bv        VARCHAR(255),
    viewTime  FLOAT
);

-- 创建 InteractionRela 表
CREATE TABLE IF NOT EXISTS InteractionRela
(
    mid      BIGINT,
    bv       VARCHAR(255),
    behavior VARCHAR(20) --CHECK (behavior IN ('like', 'coin', 'collect'))
);

-- 创建自动生成 MID 的函数
CREATE OR REPLACE FUNCTION generate_unique_mid()
    RETURNS BIGINT AS
$$
DECLARE
    new_mid BIGINT;
BEGIN
    LOOP
        new_mid := (RANDOM() * 9223372036854775807)::BIGINT;
        IF NOT EXISTS (SELECT 1 FROM Users WHERE mid = new_mid) THEN
            RETURN new_mid;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 创建设置 MID 的触发器函数
CREATE OR REPLACE FUNCTION set_mid()
    RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.mid IS NULL THEN
        NEW.mid := generate_unique_mid();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建 MID 触发器
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_set_mid') THEN
            CREATE TRIGGER trigger_set_mid
                BEFORE INSERT
                ON Users
                FOR EACH ROW
            EXECUTE FUNCTION set_mid();
        END IF;
    END
$$;

-- 创建自动生成 BV 的函数
CREATE OR REPLACE FUNCTION generate_unique_bv()
    RETURNS VARCHAR AS
$$
DECLARE
    new_bv VARCHAR(13);
    i      INT;
    ch     CHAR;
BEGIN
    LOOP
        new_bv := 'BV';
        FOR i IN 1..10
            LOOP
                ch := SUBSTRING(MD5(RANDOM()::TEXT) FROM i FOR 1);
                IF RANDOM() < 0.5 THEN
                    new_bv := new_bv || UPPER(ch);
                ELSE
                    new_bv := new_bv || ch;
                END IF;
            END LOOP;
        IF NOT EXISTS (SELECT 1 FROM Videos WHERE bv = new_bv) THEN
            RETURN new_bv;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 创建设置 BV 的触发器函数
CREATE OR REPLACE FUNCTION set_bv()
    RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.bv IS NULL THEN
        NEW.bv := generate_unique_bv();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建 BV 触发器
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_set_bv') THEN
            CREATE TRIGGER trigger_set_bv
                BEFORE INSERT
                ON Videos
                FOR EACH ROW
            EXECUTE FUNCTION set_bv();
        END IF;
    END
$$;

-- 创建各种索引
CREATE INDEX IF NOT EXISTS idx_FollRela_followerMid
    ON FollRela (followerMid);
CREATE INDEX IF NOT EXISTS idx_FollRela_followeeMid
    ON FollRela (followeeMid);
CREATE INDEX IF NOT EXISTS idx_ViewRela_viewerMid
    ON ViewRela (viewerMid);
CREATE INDEX IF NOT EXISTS idx_ViewRela_bv
    ON ViewRela (bv);
CREATE INDEX IF NOT EXISTS idx_InteractionRela_bv
    ON InteractionRela (bv);
CREATE INDEX IF NOT EXISTS idx_InteractionRela_comp
    ON InteractionRela (mid, bv, behavior);
CREATE INDEX IF NOT EXISTS idx_Videos_ownerMid
    ON Videos (ownerMid);
CREATE INDEX IF NOT EXISTS idx_Users_qq
    ON Users (qq);
CREATE INDEX IF NOT EXISTS idx_Users_wechat
    ON Users (wechat);
CREATE INDEX IF NOT EXISTS idx_Danmu_time
    ON Danmu (time);
CREATE INDEX IF NOT EXISTS idx_DanmuRela_mid
    ON DanmuRela (mid);
CREATE INDEX IF NOT EXISTS idx_Danmu_bv
    ON Danmu (bv);

-- 创建字符串匹配函数
CREATE OR REPLACE FUNCTION count_non_overlapping_substrings(main_str text, sub_str text) RETURNS integer AS $$
DECLARE
    count integer := 0;
    sub_str_array text[];
    current_sub_str text;
BEGIN
    IF sub_str = '' OR main_str = '' THEN
        RETURN 0;
    END IF;
    sub_str_array := string_to_array(sub_str, ' ');
    FOREACH current_sub_str IN ARRAY sub_str_array
        LOOP
            if current_sub_str =''THEN
                CONTINUE;
            END IF;
            count := count + regexp_count(main_str, '(?i)' || regexp_replace(current_sub_str, '([\[\].^$*+?|{}()])', '\\\1', 'g'));
        END LOOP;
    RETURN count;
END;
$$ LANGUAGE plpgsql;

