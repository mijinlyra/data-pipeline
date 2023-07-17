CREATE TABLE IF NOT EXISTS raw_seoul (
    A STRING,
    B STRING,
    C STRING,
    D STRING,
    E STRING,
    F STRING,
    G STRING,
    H STRING,
    I STRING,
    J STRING,  
)
PARTITIONED BY (dt STRING)
;

ALTER TABLE raw_seoul DROP IF EXISTS PARTITION (dt = ${part_value});

INSERT INTO raw_seoul PARTITION (dt = ${part_value})
    --INSERT INTO raw_seoul
(    
     A,
     B,
     C,
     D,
     E,
     F,
     G,
     H,
     I,
     J)
    SELECT
        A,
        B,
        C,
        D,
        E,
        F,
        G,
        H,
        I,
        J
    FROM temp_seoul
;
