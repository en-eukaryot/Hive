-- Queries launched to get the distribution of the switch in devices prior to buying
-- check the partner_id in the parameters below before launching the query
-- only partner_ids that are deemed clean can be used for the analysis

-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 2048;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;
CREATE TEMPORARY FUNCTION row_sequence as 'com.criteo.hadoop.hive.udf.UDFRowSequence';

--- Set query parameters --
SET your_database = cluo;
SET partnerid = (4155, 4154);

--- Query ---
USE ${hiveconf:your_database};

-- STEP 1: gets for each user_id (principal and matched) the timestamp, partner_id but also device and events
-- gets the row numbers to be able to join afterwards
DROP TABLE IF EXISTS all_buyers_events_ordered;
CREATE TABLE all_buyers_events_ordered AS
SELECT
    row_sequence() AS num_row
    ,`timestamp`
    ,partner_id
    ,super_uid
    ,user_id
    ,CASE
        WHEN device_type IN ('iPhone', 'Android - Smartphone') THEN 'Smartphone'
		WHEN device_type IN ('iPad', 'Android - Tablet') THEN 'Tablet'
		ELSE 'Desktop'
		END AS device
    ,event_type
FROM
	(SELECT /* +MAPJOIN(m,c) */
		*
	FROM
        all_buyers_events ev
	JOIN
        bi_datamart.bi_dim_merchant m
            ON m.merchant_id = ev.partner_id
	JOIN
        bi_datamart.bi_dim_client c
            ON c.client_id = m.most_displayed_client_id
	WHERE
		device_type IN ('iPad','iPhone','Android - Smartphone', 'Android - Tablet', 'Desktop')
		      AND ev.partner_id IN ${hiveconf:partnerid}
	ORDER BY
        partner_id
        , super_uid ASC
        , ev.`timestamp` DESC
	) log
;

-- STEP 2: joins the table above with itself to have for each line the timestamp, partner_id, superuid and device but ensuring that same device use was removed (gets the switch)
--/* SWITCH TABLE */
DROP TABLE IF EXISTS all_switch;
CREATE TABLE all_switch AS
SELECT /* +MAPJOIN(t2) */
    t1.`timestamp`,
    t1.partner_id,
    t1.super_uid,
    t1.device
FROM
    all_buyers_events_ordered t1
JOIN
    all_buyers_events_ordered t2
        ON t1.num_row + 1 = t2.num_row
WHERE
    t1.user_id <> t2.user_id
    AND t1.user_id <> '00000000-0000-0000-0000-000000000000'
ORDER BY
    t1.partner_id
    , t1.super_uid ASC
    , t1.`timestamp` DESC
;



--STEP 3: get the top 3 device for browsing (on top of the device used for the sale) per partner and super_id
CREATE TEMPORARY FUNCTION row_sequence AS 'com.criteo.hadoop.hive.udf.UDFRowSequence';
-- concat partner_id and super_uid for ease of the query after
DROP TABLE IF EXISTS all_switch_temp;
CREATE TABLE all_switch_temp AS
SELECT
    row_sequence() AS num_row
	,CONCAT(partner_id, super_uid) AS id
	,`timestamp` AS ts
	,device
FROM
    all_switch
;
-- max (ts) to get the timestamp of the sale to ensure we keep the device used for the sale
DROP TABLE IF EXISTS list_id;
CREATE TABLE list_id AS
SELECT
	id
	, MAX(ts) AS timestamp_ref
FROM
    all_switch_temp
GROUP BY
    id
;

-- gets the last three devices used for browsing before buying the product
DROP TABLE IF EXISTS all_switch_id;
CREATE TABLE all_switch_id AS
SELECT /* + MAPJOIN(t1) */
	did.id
	,did.timestamp_ref
	,t1.device AS device_sale
	,t2.device AS device_1
	,t3.device AS device_2
	,t4.device AS device_3
FROM
    list_id did
JOIN
    all_switch_temp t1
        ON t1.id = did.id
            AND t1.ts = did.timestamp_ref
LEFT OUTER JOIN
    all_switch_temp t2
        ON t1.num_row + 1 = t2.num_row
            AND t2.id = t1.id
LEFT OUTER JOIN
    all_switch_temp t3
        ON t1.num_row + 2 = t3.num_row
            AND t3.id = t1.id
LEFT OUTER JOIN
    all_switch_temp t4
        ON t1.num_row + 3 = t4.num_row
            AND t4.id = t1.id
;


--STEP 4: count of the distribution in the devices

DROP TABLE IF EXISTS all_switch_distrib;
CREATE TABLE all_switch_distrib AS
SELECT
	device_sale
	, device_1
	, device_2
	, device_3
	, COUNT(0) AS count
FROM
    all_switch_id
GROUP BY
	device_sale
	, device_1
	, device_2
	, device_3
;

-- DROP TABLE all_buyers_events;
-- DROP TABLE all_switch;
-- DROP TABLE all_switch_temp;
-- DROP TABLE list_id;
-- DROP TABLE all_switch_id;
