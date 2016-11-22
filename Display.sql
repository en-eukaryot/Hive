SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 1024;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

SELECT  COUNT(DISTINCT user_id) AS unique_users,
        COUNT(1) AS displays
    FROM bi_data.partnerdb_bi_display
    WHERE day >= '2016-09-01'
      AND campaign_id = 95397
      AND client_id = 24573
      AND partner_partition = pmod(26906, 1000)
      AND user_id <> '00000000-0000-0000-0000-000000000000'
;
