#Rebuild Hourly Revenue and Forecasted Revenues
#this relies on hourly forecast models
DROP VIEW `<yourProject.yourDataset>.ga_realtime_revenue_hourly_view`;
CREATE VIEW `<yourProject.yourDataset>.ga_realtime_revenue_hourly_view` AS (
        WITH 
            CURRENT1 AS (
                #RevenueByHour	
                SELECT 
                    orderHour,
                    Revenue,
                    SUM (Revenue) OVER (ORDER BY orderHour) as CumulativeRevenue
                FROM (
                    SELECT
                        EXTRACT(HOUR FROM TIMESTAMP_MILLIS(visitstarttime*1000 + hits.time) AT TIME ZONE "America/Los_Angeles") AS orderHour,
                        SUM (prod.productRevenue / 1000000) AS Revenue,    
                    FROM
                        `<yourProject.yourDataset>.ga_realtime_view` ga, UNNEST(hits) AS hits, UNNEST(hits.product) AS prod
                    WHERE 
                        CURRENT_DATE("America/Los_Angeles") = EXTRACT(DATE FROM TIMESTAMP_MILLIS(visitstarttime*1000 + hits.time) AT TIME ZONE "America/Los_Angeles")
					    AND hits.page.hostname = "www.thenorthface.com"
                        AND NOT REGEXP_CONTAINS (hits.page.pagePath, "(en|fr)(_|-)CA")
                        AND NOT REGEXP_CONTAINS (hits.page.pagePath, "storeId=7002")
                    GROUP BY 1
                    ORDER BY 1 ASC
                )
            ),
            HISTORIC1 AS (
                #HistoricRevenueByHour
                SELECT 
                    orderHour,
                    RevenueHistoric,
                    SUM (RevenueHistoric) OVER (ORDER BY orderHour) as CumulativeRevenueHistoric
                FROM (
                    SELECT 
                        EXTRACT(HOUR FROM TIMESTAMP_MILLIS(visitstarttime*1000) AT TIME ZONE "America/Los_Angeles") AS orderHour,
                        SUM (prod.productRevenue/1000000) AS RevenueHistoric,
                    FROM
                        `<yourProject.yourDataset>.ga_sessions_*` ga, UNNEST(hits) AS hits, UNNEST(hits.product) AS prod 
                    WHERE 
                        _TABLE_SUFFIX =  FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE("America/Los_Angeles"), INTERVAL 364 DAY))
                        AND hits.page.hostname = "www.thenorthface.com"
                        AND NOT REGEXP_CONTAINS (hits.page.pagePath, "(en|fr)(_|-)CA")
                        AND NOT REGEXP_CONTAINS (hits.page.pagePath, "storeId=7002")
                    AND productRevenue IS NOT NULL 
                    GROUP BY 1
                    ORDER BY 1 ASC
                )
            ),
            FORECASTED1 AS (
                SELECT 
                    ForecastedOrderHour,
                    ForcastedRevenue,
                    SUM (ForcastedRevenue) OVER (ORDER BY ForecastedOrderHour) as CumulativeForecastedRevenue
                FROM (
                    SELECT
                        0 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-00`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        1 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-01`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        2 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-02`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        3 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-03`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        4 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-04`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        5 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-05`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        6 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-06`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        7 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-07`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        8 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-08`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        9 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-09`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        10 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-10`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        11 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-11`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        12 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-12`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        13 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-13`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        14 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-14`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        15 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-15`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        16 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-16`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        17 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-17`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        18 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-18`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        19 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-19`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        20 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-20`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        21 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-21`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        22 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-22`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                    UNION ALL
                    SELECT
                        23 AS ForecastedOrderHour,
                        IF (forecast_value < 0, 0, forecast_value) as ForcastedRevenue
                    FROM
                        ML.FORECAST(MODEL `<yourProject.yourDataset>.revenue-forecast-model-hour-23`, STRUCT(1 AS horizon, 0.8 AS confidence_level))
                ) 
            )
        SELECT 
            H.orderHour,
            C.Revenue,
            C.CumulativeRevenue,
            H.RevenueHistoric,
            H.CumulativeRevenueHistoric,
            F.ForcastedRevenue,
            F.CumulativeForecastedRevenue
        FROM HISTORIC1 H LEFT JOIN CURRENT1 C
        ON C.orderHour = H.orderHour
        JOIN FORECASTED1 F
        ON F.ForecastedOrderHour = H.orderHour
)