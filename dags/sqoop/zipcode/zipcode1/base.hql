-- 실행 : $ hive -f base.hql --hivevar part_value="'2023-05-01'" --띄어쓰기하면 작동안됨

CREATE TABLE IF NOT EXISTS base_zipcode
(
    province_name STRING COMMENT 'province name',
    city_name STRING COMMENT 'city name',
    postal_code STRING COMMENT 'postal code',
    dong_name STRING COMMENT 'dong name'
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
;

ALTER TABLE base_zipcode DROP IF EXISTS PARTITION (dt = ${part_value});

INSERT INTO TABLE base_zipcode PARTITION (dt = ${part_value})
SELECT 
    province_name,
    city_name,
    d.postal_code,
    d.eup_myeon_dong_name
FROM
    raw_zipcode
    LATERAL VIEW explode(child_objects) exploded_table AS d
WHERE dt = ${part_value}
;

DESCRIBE FORMATTED base_zipcode;

SELECT ASSERT_TRUE(COUNT(*)>5000) FROM base_zipcode;
