-- 실행 : hive -f dags/sqoop/zipcode/gudong/base.hql --hivevar part_value="'2023-05-02'"
DROP TABLE base_gudong_mst;

CREATE TABLE base_gudong_mst
(
    gu_name STRING COMMENT 'GU',
    dong_name STRING COMMENT 'DONG'
)
STORED AS PARQUET
;

INSERT INTO TABLE base_gudong_mst
SELECT
    DISTINCT
        city_name AS gu_name,
        dong_name AS dong_name
FROM 
    base_zipcode
WHERE
    dt=${part_value}
    AND dong_name != 'NULL'
    AND province_name='서울특별시'
;

DESCRIBE FORMATTED base_gudong_mst;

SELECT ASSERT_TRUE(COUNT(*)>700) FROM base_gudong_mst;
