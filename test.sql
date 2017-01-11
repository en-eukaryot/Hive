SET hive.cli.print.header = TRUE;

-- SELECT
--     *
-- FROM
--     bi_data.partnerdb_bi_advertiser_event
-- WHERE
--     partner_id = 12629
--     AND partner_partition = 629
--     AND day = '2016-11-10'
--     AND user_id <> '00000000-0000-0000-0000-000000000000'
--     AND persistent_user = TRUE
--     AND event_name = 'Sales'
-- LIMIT 1000;

SELECT *
    FROM bi_data.partnerdb_matched_transactions
    WHERE day = '2016-11-10'
        AND partner_id = 12629
LIMIT 1000;
