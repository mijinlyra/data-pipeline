DROP TABLE temp_seoul;

CREATE EXTERNAL TABLE IF NOT EXISTS temp_seoul (
    A STRING,
    B STRING,
    C STRING,
    D STRING,
    E STRING,
    F STRING,
    G STRING,
    H STRING,
    I STRING,
    J STRING)
    -- ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar"     = "\""
    )
    STORED AS TEXTFILE
    LOCATION '/pd24/hive/seoul/' --  The base csv file's path
    tblproperties("skip.header.line.count"="1"
    )