SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET hive.enforce.bucketing = true;
SET hive.cli.print.header = true;
SET hive.exec.parallel = true;

SET partnerid = 10861;
SET enddate = '2016-10-30';
SET client_country_code = 'DK';


SELECT
    CASE WHEN merchant_id = ${hiveconf:partnerid} THEN 1 ELSE 0 END AS current_client,
    vertical_level_2_name,
    criteo_match,
    client_match,
    other_client_match,
    SUM(marketplace_revenue_raw_euro) AS revenue,
    COUNT(distinct user_id) AS num_users
FROM
    bi_data.bi_display_full d
JOIN
    bi_datamart.bi_dim_client c ON c.client_id = d.client_id
LEFT OUTER JOIN
    (
        SELECT
            uid,
            MAX(CASE WHEN providertype IN ('DATA_PROVIDER','APP_CK') THEN 1 ELSE 0 END) AS criteo_match,
            MAX(CASE WHEN (providertype = 'PARTNER_ID' AND providerid = ${hiveconf:partnerid}) THEN 1 ELSE 0 END) AS client_match,
            MAX(CASE WHEN (providertype != 'DATA_PROVIDER' AND providertype != 'APP_CK')
                AND (providertype != 'PARTNER_ID' OR providerid <> ${hiveconf:partnerid}) THEN 1 ELSE 0 END) AS other_client_match
        FROM
            (
                SELECT
                    e_events.uid AS uid,
                    e_providers.providertype AS providertype,
                    providerid
                FROM bi_data.uid_matching_full_lt
                    LATERAL VIEW EXPLODE(events) x AS e_events
                        LATERAL VIEW EXPLODE(e_events.providers) x AS e_providers
                            LATERAL VIEW EXPLODE(COALESCE(e_providers.providerids, array(0))) x AS providerid
                WHERE day = ${hiveconf:enddate}
                    AND host_platform = 'EU'
                GROUP BY
                    e_events.uid,
                    e_providers.providertype,
                    providerid
            ) umf
        GROUP BY
            uid
    ) match_table ON match_table.uid = d.user_id
WHERE
    client_country_code = ${hiveconf:client_country_code}
    AND day BETWEEN DATE_SUB(${hiveconf:enddate}, 6) AND ${hiveconf:enddate}
    AND host_platform = 'EU'
GROUP BY
    CASE WHEN merchant_id = ${hiveconf:partnerid} THEN 1 ELSE 0 END,
    vertical_level_2_name,
    criteo_match,
    client_match,
    other_client_match
;
