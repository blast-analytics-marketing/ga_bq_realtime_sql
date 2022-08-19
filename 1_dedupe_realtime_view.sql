#15 Minute Refresh SQL for Real Time View Creation
DROP VIEW `<yourProject.yourDataset>.ga_realtime_view`;
CREATE VIEW <yourProject.yourDataset>.ga_realtime_view AS
SELECT
  CONCAT('20', _TABLE_SUFFIX) AS dataset_date,
  visitKey,
  ARRAY_AGG(
    (SELECT AS STRUCT t.* EXCEPT (visitKey))
    ORDER BY exportTimeUsec DESC LIMIT 1)[OFFSET(0)].*
FROM `<yourProject.yourDataset>.ga_realtime_sessions_20*` AS t
GROUP BY dataset_date, visitKey;