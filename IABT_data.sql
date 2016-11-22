CREATE TEMPORARY FUNCTION abtpop AS 'com.criteo.hadoop.hive.udf.UDFABTestPopulation';
SET mapred.map.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
SET hive.exec.parallel = true;

-- Set variables --
SET bi_name = cluo;
SET test_name = dustindk;
SET papatool_test_id = 5696;
SET partner_id = (12629, 12364);

SET test_start_users = '2016-10-07';
SET test_start_measures = '2016-10-14';
SET test_end = '2016-11-15';

USE ${hiveconf:bi_name};

DROP TABLE IF EXISTS ${hiveconf:test_name}_20161115_02;

CREATE TABLE ${hiveconf:test_name}_20161115_02 AS
SELECT
	user_id
	, partner_id
	, population
	, min(first_day_seen) as first_day_seen
	, sum(events) as events
	, sum(clicks) as clicks
	, sum(spend) as spend
	, sum(transactions) as transactions
	, sum(order_value) as order_value
	, sum(displays) as displays
FROM
(
---events---
SELECT
	user_id
	, partner_id
	, CASE WHEN abtpop(user_id, ${hiveconf:papatool_test_id}) = 0 THEN 'exposed' else 'control' END AS population
	, to_date(from_unixtime(min(unixtime))) as first_day_seen
	, count(1) AS events
	, 0 AS clicks
	, 0.0 AS spend
	, 0 as transactions
	, 0.0 as order_value
	, 0 as displays
FROM
	bi_data.partnerdb_bi_advertiser_event
WHERE
	partner_id IN ${hiveconf:partner_id}
	and persistent_user = true
	and day >= ${hiveconf:test_start_users}
	and day <= ${hiveconf:test_end}
	and partner_partition IN (pmod(12629, 1000), pmod(12364, 1000))
	and user_id <> '00000000-0000-0000-0000-000000000000'
GROUP BY
	user_id
	, partner_id
	, CASE WHEN abtpop(user_id, ${hiveconf:papatool_test_id})=0 THEN 'exposed' else 'control' END

UNION ALL

---clicks/displays----
SELECT
	user_id
	, partner_id
	, CASE WHEN abtpop(user_id, ${hiveconf:papatool_test_id})=0 THEN 'exposed' else 'control' END AS population
	, null as first_day_seen
	, 0 as events
	, sum(clicks) as clicks
    , sum(revenue_local) as spend
    , 0 as transactions
    , 0.0 as order_value
    , sum(displays) as displays
FROM (
    SELECT
    	user_id
		, partner_id
    	, 1 as clicks
    	, revenue_local
    	, 0 as displays
    FROM
        bi_data.partnerdb_bi_click
    WHERE
    	partner_id IN ${hiveconf:partner_id}
		AND partner_partition IN (pmod(12629, 1000), pmod(12364, 1000))
		AND day >= ${hiveconf:test_start_measures}
		AND day <= ${hiveconf:test_end}
		AND user_id <> '00000000-0000-0000-0000-000000000000'
    UNION ALL
    SELECT
    	user_id
		, merchant_id AS partner_id
        , 0 as clicks
        , sum_display_revenue_local/1000 as revenue_local
        , 1 as displays
    FROM
        bi_data.bi_display_full
    WHERE
    	host_platform = 'EU'
    	AND merchant_id IN ${hiveconf:partner_id}
		AND day >= ${hiveconf:test_start_measures}
		AND day <= ${hiveconf:test_end}
		AND user_id <> '00000000-0000-0000-0000-000000000000'
) temp1
GROUP BY
	user_id
	, partner_id
	, CASE WHEN abtpop(user_id, ${hiveconf:papatool_test_id})=0 THEN 'exposed' else 'control' END

UNION ALL

---transactions---
SELECT
	user_id
	, partner_id
	, CASE WHEN abtpop(user_id, ${hiveconf:papatool_test_id})=0 THEN 'exposed' else 'control' END AS population
	, null as first_day_seen
	, 0 as events
	, 0 as clicks
	, 0.0 as spend
	, COUNT(DISTINCT transaction_id) as transactions
	, SUM(order_value) as order_value
	, 0 as displays
FROM
(
	SELECT
		user_id
		, partner_id
		, transaction_id
		, max(order_value) as order_value
	FROM
	(
		SELECT
			user_id
			, partner_id
			, transaction_id
			, unixtime as transaction_ts
			, sum(b.price * b.quantity) AS order_value
		FROM
			bi_data.partnerdb_bi_advertiser_event
				LATERAL VIEW explode(products) p AS b
		WHERE
			partner_id IN ${hiveconf:partner_id}
			AND partner_partition IN (pmod(12629, 1000), pmod(12364, 1000))
			AND day >= ${hiveconf:test_start_measures}
			AND day <= ${hiveconf:test_end}
			AND user_id <> '00000000-0000-0000-0000-000000000000'
			AND persistent_user = true
			AND event_name = 'Sales'
		GROUP BY user_id, partner_id, transaction_id, unixtime
	)trans1
	GROUP BY user_id, partner_id, transaction_id
)trans2
GROUP BY user_id, partner_id, CASE WHEN abtpop(user_id, ${hiveconf:papatool_test_id})=0 THEN 'exposed' else 'control' END


) temp3
GROUP BY
	user_id
	, partner_id
	, population
;
