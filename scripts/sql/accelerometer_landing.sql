CREATE EXTERNAL TABLE IF NOT EXISTS `nd-spark-datalake`.`accelerometer_landing` (
  `user` string,
  `timeStamp` bigint,
  `x` float,
  `y` float,
  `z` float
) COMMENT "acceleromente landing table for data in accelormeter/landing/* loaded from the github project of the course."
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE',
  'serialization.format' = '1'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://nd-spark-data-lake/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');