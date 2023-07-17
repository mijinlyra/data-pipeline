CREATE TABLE IF NOT EXISTS base_seoul (
    A STRING,
    B STRING,
    C STRING,
    D STRING,
    --E STRING,
    start_dong STRING,
    F STRING,
    G STRING,
    H STRING,
    I STRING,
    J STRING)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
;

ALTER TABLE base_seoul DROP IF EXISTS PARTITION (dt = ${part_value});
-- INSERT OVERWRITE TABLE base_seoul PARTITION (dt = ${part_value})
INSERT INTO TABLE base_seoul PARTITION (dt = ${part_value})
-- INSERT INTO base_seoul
(
    A,
    B,
    C,
    D,
    --E
    start_dong,
    F,
    I,
    J)
    SELECT
        A,
        B,
        C,
        D,
        SPLIT(E,'_')[0],
        F,
        G,
        H,
        I,
        J
    FROM raw_seoul
    WHERE dt = ${part_value}
;


describe formatted base_seoul;

