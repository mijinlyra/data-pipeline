DROP TABLE tmp_spotify;

-- 원천 데이터 입수 / 테이블 삭제와 상관없이 원천 데이터 유지 (명시적) 설정
CREATE EXTERNAL TABLE tmp_spotify
(
    end_time STRING,
    artist_name STRING,
    track_name STRING,
    sec_played INT 

    --"endTime" : "2022-07-09 12:19",
    --"artistName" : "Kygo",
    --"trackName" : "Higher Love",
    --"msPlayed" : 11730
)

ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION "/pd24/tmp/spotify/"
-- LOCATION "/pd24/tmp/spotifytest/"

TBLPROPERTIES ('external.table.purge'='false') -- https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.4/using-hiveql/content/hive_drop_external_table_data.html
;

-- 입수 테이블 스키마 확인
DESCRIBE FORMATTED tmp_spotify;

-- 입수 테이블 검증
--https://stackoverflow.com/questions/44122731/can-you-conditionally-fail-a-hive-script
SELECT ASSERT_TRUE(COUNT(*)>20) FROM tmp_spotify; --row가 20줄 이상이어야 한다는 검증 
