-- Queries launched for a week of sales in July (one week to ensure that we are representative)
-- the goal is to get all the data needed to understand the full picture of a users browsing behavior across devices prior to buying
-- Gets for all matched buyers during that week the events and devices used to perform the events during exactly one month preceding the sale for all the matched uids

-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 2048;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

--- Set query parameters --
SET your_database = cluo;
SET calc_date_y_m_d = '2016-11-30';
SET partnerid = (4155, 4154);

--- Query ---
USE ${hiveconf:your_database};

-- STEP 1: gets ids for those who had a Sale-event during the period, with timestamp and partner_id, who also appear to be in the matched list and deemed as clean
DROP TABLE IF EXISTS all_buyers_id;
CREATE TABLE all_buyers_id AS
SELECT /* +MAPJOIN(m, c, s) */
    ev.partnerid AS partner_id
    , ev.uid
    , MIN(ev.`timestamp`) AS `timestamp`
FROM
    bi_data.bi_advertiser_event_rich ev
JOIN
    bi_datamart.bi_dim_merchant m
        ON ev.partnerid = m.merchant_id
JOIN
    bi_datamart.bi_dim_client c
        ON m.most_displayed_client_id = c.client_id
JOIN
    bi_data.uid_matching_full_lt ma
        ON ev.uid = ma.uid
            AND ma.day = ${hiveconf:calc_date_y_m_d}
            AND ma.host_platform = 'EU'
JOIN
    bi_lta.hulp_partner_daily_weekly_status s
        ON ev.partnerid = s.partner_id
            AND s.day = ${hiveconf:calc_date_y_m_d}
            AND s.period = 7
            AND s.status = 1
WHERE
    ev.day BETWEEN DATE_ADD(${hiveconf:calc_date_y_m_d}, -6) AND DATE_ADD(${hiveconf:calc_date_y_m_d}, 0)
    AND ev.eventtype = 'Sales'
    AND ev.partnerid IN ${hiveconf:partnerid}
    --- AND c.vertical_level_2_name IN ${hiveconf:vertical}
    --- AND c.client_country_name IN ${hiveconf:country}
GROUP BY
    ev.partnerid
    , ev.uid
;

-- STEP 2: gets matched buyers with all their matched uids (lateral view explode) including the main uid, with also timestamp and partner_id
DROP TABLE IF EXISTS all_buyers_id_full;
CREATE TABLE all_buyers_id_full AS
SELECT *
    FROM
    (
    SELECT
            `timestamp`
            , partner_id
            -- ,country
            -- ,vertical
            -- ,subvertical
            -- ,tier
            , uid AS super_uid
            , uid AS user_id
        FROM all_buyers_id
    UNION ALL
    SELECT
            buyers.`timestamp`
            , buyers.partner_id
            -- ,buyers.country
            -- ,buyers.vertical
            -- ,buyers.subvertical
            -- ,buyers.tier
            , buyers.uid AS super_uid
            , matched_uid AS user_id
        FROM
            all_buyers_id buyers
        JOIN
            (
            SELECT
                uid
                , myusers.uid AS matched_uid
            FROM
                bi_data.uid_matching_full_lt
                    LATERAL VIEW EXPLODE(events) myTable2 AS myusers
            WHERE
                day = ${hiveconf:calc_date_y_m_d}
                AND host_platform = 'EU'
            ) matches ON buyers.uid = matches.uid
    ) a
;

-- STEP 3: gets all device and events from the month preceding the sale for each matched uids
DROP TABLE IF EXISTS all_buyers_events;
CREATE TABLE all_buyers_events AS
SELECT
	ev.`timestamp`
	, buyers.partner_id
	-- ,buyers.country
	-- ,buyers.vertical
	-- ,buyers.subvertical
	-- ,buyers.tier
	, super_uid
	, ev.uid AS user_id
	, CASE
		WHEN ua_device_family = 'iPad' THEN 'iPad'
		WHEN ua_device_family = 'iPhone' THEN 'iPhone'
		WHEN ua_os_family = 'Android'
            AND lower(useragent) LIKE '%mobile%' THEN 'Android - Smartphone'
		WHEN ua_os_family = 'Android' THEN 'Android - Tablet'
		WHEN ua_browser_family = 'other'
            AND display_env = 'app_android' THEN 'App - Android'
		WHEN ua_browser_family = 'other'
            AND display_env = 'app_ios' THEN 'App - Apple'
		WHEN lower(ua_browser_family) = 'safari'
            OR ua_device_family = 'Other'
            OR ua_device_family IS NULL
            OR ua_device_family = 'Unknown' THEN 'Desktop'
		ELSE 'Mobile - Other'
		END AS device_type
	, eventtype AS event_type
FROM
    all_buyers_id_full buyers
JOIN
    bi_data.bi_advertiser_event_rich ev
        ON buyers.user_id = ev.uid
            AND ev.partnerid = buyers.partner_id
WHERE
    ev.day BETWEEN DATE_ADD(${hiveconf:calc_date_y_m_d}, -29) AND DATE_ADD(${hiveconf:calc_date_y_m_d}, 0)
    AND ev.`timestamp` BETWEEN buyers.`timestamp` - 2592000 AND buyers.`timestamp`
;
