WITH CTE AS(
SELECT * FROM dbo.[202311-divvy-tripdata]
UNION ALL
SELECT * FROM dbo.[202310-divvy-tripdata]
UNION ALL
SELECT * FROM dbo.[202312-divvy-tripdata]
) ,
CTE1 AS (
 SELECT
  ride_id,
  rideable_type,
  started_at,
  ended_at,
  COALESCE(start_station_name, 'Unknown') AS start_station_name,
  COALESCE(start_station_id, 'Unknown') AS start_station_id,
  COALESCE(end_station_name, 'Unknown') AS end_station_name,
  COALESCE(end_station_id, 'Unknown') AS end_station_id,
  ROUND(start_lat,2)AS start_lat,
  ROUND(start_lng,2) AS start_lng,
  ROUND(end_lat,2) AS end_lat,
  ROUND(end_lng,2) AS end_lng,
  member_casual,
  DATEDIFF(MINUTE,started_at,ended_at) AS trip_duration,
  DATENAME(weekday, started_at) AS day_of_week,
  DATEPART(hour, started_at) AS hour
 FROM
  CTE
 WHERE
  started_at IS NOT NULL AND
  ended_at IS NOT NULL AND
  CONVERT(DATETIME, started_at, 101) IS NOT NULL AND
  CONVERT(DATETIME, ended_at, 101) IS NOT NULL
) ,
CTE2 AS (
 SELECT
  ride_id,
  rideable_type,
  started_at,
  ended_at,
  RTRIM(start_station_name) AS start_station_name,
  start_station_id,
  RTRIM(end_station_name) AS end_station_name,
  end_station_id,
  ROUND(start_lat,2)AS start_lat,
  ROUND(start_lng,2) AS start_lng,
  ROUND(end_lat,2) AS end_lat,
  ROUND(end_lng,2) AS end_lng,
  member_casual,
  trip_duration,
  day_of_week
 FROM
  CTE1
 WHERE
  started_at <> ended_at AND
  ended_at > started_at AND
  trip_duration > 3 AND
  CAST(trip_duration AS INT) IS NOT NULL AND
  CAST(start_lat AS FLOAT) IS NOT NULL AND
  CAST(start_lng AS FLOAT) IS NOT NULL
),
result AS (
  SELECT
    member_casual,
    MAX(trip_duration) AS max_trip,
    AVG(trip_duration) AS average_trip,
    day_of_week AS mode_day_of_week,
    COUNT(day_of_week) AS count
  FROM
    CTE2
  GROUP BY
    member_casual,
    day_of_week
)
SELECT *
FROM result
ORDER BY [count]DESC;
