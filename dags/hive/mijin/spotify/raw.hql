CREATE TABLE IF NOT EXISTS raw_spotify
(
    end_time STRING '스트리밍 종료된 날짜,시간',
    artist_name STRING '아티스트 이름',
    track_name STRING '음악 트랙 제목',
    sec_played INT '트랙을 들은 시간(초)'
)
PARTITIONED BY (dt STRING);

ALTER TABLE raw_spotify DROP IF EXISTS PARTITION (dt = ${part_value});

INSERT INTO TABLE raw_spotify PARTITION (dt = ${part_value})
SELECT
        end_time,
        artist_name,
        track_name,
        sec_played
FROM tmp_spotify
;

DESCRIBE FORMATTED raw_spotify;
-- 검증
SELECT ASSERT_TRUE(COUNT(*)>200) FROM raw_spotify;
