--load.hql 실행하여 tmp_zipcode라는 테이블 생성됨 ($ hive -f load.hql)
--hivevar part_value="'2023-05-01'"
CREATE TABLE IF NOT EXISTS raw_zipcode
(
    province_name STRING COMMENT '도',
    city_name STRING COMMENT '시',
    child_objects ARRAY<struct<postal_code: INT, eup_myeon_dong_name: STRING >> COMMENT '상세정보'
)
PARTITIONED BY (dt STRING)
;

-- MSCK REPAIR TABLE raw_zipcode;
ALTER TABLE raw_zipcode DROP IF EXISTS PARTITION (dt = ${part_value});

INSERT INTO TABLE raw_zipcode PARTITION (dt = ${part_value})
SELECT
        -- a. '(dt)?+.+' -> means 'dt를 빼고 다 가져온다'
        province_name,
        city_name,
        child_objects
FROM tmp_zipcode
-- WHERE dt = ${part_value}
;

DESCRIBE FORMATTED raw_zipcode;
--검증
SELECT ASSERT_TRUE(COUNT(*)>200) FROM raw_zipcode;

