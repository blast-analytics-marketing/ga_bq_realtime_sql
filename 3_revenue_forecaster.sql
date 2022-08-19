#REVENUE FORECAST MODELS
#1 of 24
#using 1000 days of previous hourly revenue data
CREATE OR REPLACE MODEL ``<yourProject.yourDataset>.revenue-forecast-model-hour-00` 
OPTIONS(MODEL_TYPE = 'ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL = 'timestamp', 
  TIME_SERIES_DATA_COL = 'revenue',
  DATA_FREQUENCY = 'HOURLY',
  HOLIDAY_REGION =  'NA' )
AS
SELECT 
  timestamp, revenue
 FROM (
  SELECT      
    DATETIME(CAST(CONCAT(EXTRACT(DATE FROM TIMESTAMP_MILLIS(visitstarttime*1000) AT TIME ZONE "America/Los_Angeles"),' ',EXTRACT(HOUR FROM TIMESTAMP_MILLIS(visitstarttime*1000) AT TIME ZONE "America/Los_Angeles"),':00:00') AS TIMESTAMP), "America/Los_Angeles") as timestamp,
    EXTRACT(HOUR FROM TIMESTAMP_MILLIS(visitstarttime*1000) AT TIME ZONE "America/Los_Angeles") AS orderHour,
    SUM(IF(totals.transactionRevenue IS NOT NULL, totals.transactionRevenue/1000000,0)) AS revenue
  FROM
    `<yourProject.yourDataset>.ga_sessions_*` ga
  WHERE 
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE("America/Los_Angeles"), INTERVAL 1000 DAY))
                  AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE("America/Los_Angeles"), INTERVAL 1 DAY))
  GROUP BY 1,2
  ORDER BY 1 ASC	
) 
WHERE orderHour = 0;