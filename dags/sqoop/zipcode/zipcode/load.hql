DROP TABLE tmp_zipcode;

-- 원천 데이터 입수 / 테이블 삭제와 상관없이 원천 데이터 유지 (명시적) 설정
CREATE EXTERNAL TABLE tmp_zipcode
(
        city_name STRING,
        child_objects ARRAY<struct<postal_code:INT, eup_myeon_dong_name:STRING>>,
        province_name STRING

)

ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION "/pd24/tmp/zipcode/"
TBLPROPERTIES ('external.table.purge'='false') -- https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.4/using-hiveql/content/hive_drop_external_table_data.html
;

-- 입수 테이블 스키마 확인
DESCRIBE FORMATTED tmp_zipcode;

-- 입수 테이블 검증
--https://stackoverflow.com/questions/44122731/can-you-conditionally-fail-a-hive-script
SELECT ASSERT_TRUE(COUNT(*)>200) FROM tmp_zipcode; --row가 200줄 이상이어야

-- LOAD to json.gz(압축) : zipcode-zip.json.gz
-- DROP TABLE tmp_zipcode_zip;

-- CREATE EXTERNAL TABLE tmp_zipcode_zip
--(
--     city_name STRING,
--     child_objects ARRAY<struct<postal_code:INT, eup_myeon_dong_name:STRING>>,
--     province_name STRING
-- )

-- ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
-- LOCATION "/pd24/tmp/zipcode_zip/"

-- DESCRIBE FORMATTED tmp_zipcode_zip;

-- SELECT ASSERT_TRUE(COUNT(*)>200) FROM tmp_zipcode_zip;
