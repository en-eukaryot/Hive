--- Set enviroment variables ---
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET hive.exec.parallel = TRUE;
SET hive.cli.print.header = TRUE;

--- Set query variables ---
SET partnerid = (11075);
SET start_date = '2016-11-01';
SET end_date = '2016-11-30';

--- Query ---
SELECT DISTINCT day
  FROM bi_data.partnerdb_catalog_stats
  WHERE partner_id IN ${hiveconf:partnerid}
    AND day >= ${hiveconf:start_date}
