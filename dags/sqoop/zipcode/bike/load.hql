gDROP TABLE temp_bike;

CREATE EXTERNAL TABLE IF NOT EXISTS temp_bike (
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
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "seperatorChar" = ",",
        "quoteChar"     = "\""
    )
    STORED AS TEXFILE
    LOCATION '/pd24/hive/seoul'
    tblproperties ("skip.header.line.count"="1")