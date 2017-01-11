-- Set enviroment parameters --
SET mapred.map.output.compression.codec = com.hadoop.compression.lzo.LzopCodec;
SET sort.io.mb = 2048;
SET hive.cli.print.header = TRUE;
SET hive.exec.parallel = TRUE;

--- Set query parameters --
SET startdate = "2016-12-01";
SET enddate = "2016-12-15";
SET part_id = (2429);
SET part_prt = (429);
SET your_database = cluo;

--- Query ---
USE ${hiveconf:your_database};

DROP TABLE IF EXISTS catalog_stats_display_click;
CREATE TABLE catalog_stats_display_click AS
SELECT /* +MAPJOIN(c) */
    dc.day
    , dc.partnerid
    , dc.itemid
    , c.id AS external_item_id
    -- , c.name AS product_name
    -- , c.producturl AS product_url
    -- , c.reallyrecommendable as really_recommendable
    -- , c.price as product_price
    , dc.displays
    , dc.clicks
    , dc.revenue_local
FROM (
        SELECT
            day
            , partnerid
            , itemid
            , SUM(display) AS displays
            , SUM(click) AS clicks
            , SUM(revenue_local) AS revenue_local
        FROM
            (SELECT
                day
                , partnerid
                , cast(products.itemid as string) as itemid
                , 1 AS display
                , 0 AS click
                , 0.0 AS revenue_local
            FROM
                glup.glup_reco_display
                    LATERAL VIEW EXPLODE (displayed_products) x AS products
            WHERE
                day BETWEEN ${hiveconf:startdate} AND ${hiveconf:enddate}
                AND displayed_products IS NOT NULL
                AND partnerid IN ${hiveconf:part_id}
                AND host_platform = 'EU'
            UNION ALL
            SELECT
                day
                , partner_id AS partnerid
                , cast(item_id AS string) AS itemid
                , 0 AS display
                , 1 AS click
                , revenue_local
            FROM
                bi_data.partnerdb_bi_click
            WHERE
                day BETWEEN ${hiveconf:startdate} AND ${hiveconf:enddate}
                AND item_id IS NOT NULL
                AND partner_partition IN ${hiveconf:part_prt}
            ) x
        GROUP BY day
            , partnerid
            , itemid
        ) dc
JOIN
    bi_data.partnerdb_catalogs c
        ON dc.partnerid = c.partnerid
        AND dc.itemid = c.sqlid
