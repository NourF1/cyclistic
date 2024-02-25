-- Combine data from multiple Divvy trip data tables
WITH raw_data AS (
  SELECT * FROM dbo.[202311-divvy-tripdata]
  UNION ALL
  SELECT * FROM dbo.[202310-divvy-tripdata]
  UNION ALL
  SELECT * FROM dbo.[202312-divvy-tripdata]
)

-- Filter and clean data
SELECT
  ride_id,
  rideable_type,
  started_at,
  ended_at,
  COALESCE(start_station_name, 'Unknown') AS start_station_name,
  COALESCE(start_station_id, 'Unknown') AS start_station_id,
  COALESCE(end_station_name, 'Unknown') AS end_station_name,
  COALESCE(end_station_id, 'Unknown') AS end_station_id,
  ROUND(start_lat, 2) AS start_lat,
  ROUND(start_lng, 2) AS start_lng,
  ROUND(end_lat, 2) AS end_lat,
  ROUND(end_lng, 2) AS end_lng,
  member_casual,
  DATEDIFF(MINUTE, started_at, ended_at) AS trip_duration,
  DATENAME(weekday, started_at) AS day_of_week,
  DATEPART(hour, started_at) AS hour
FROM raw_data
WHERE
  started_at IS NOT NULL AND
  ended_at IS NOT NULL AND
  CONVERT(DATETIME, started_at, 101) IS NOT NULL AND
  CONVERT(DATETIME, ended_at, 101) IS NOT NULL;

-- Filter valid trips and remove unnecessary columns
WITH filtered_data AS (
  SELECT
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    RTRIM(start_station_name) AS start_station_name,
    start_station_id,
    RTRIM(end_station_name) AS end_station_name,
    end_station_id,
    ROUND(start_lat, 2) AS start_lat,
    ROUND(start_lng, 2) AS start_lng,
    ROUND(end_lat, 2) AS end_lat,
    ROUND(end_lng, 2) AS end_lng,
    member_casual,
    trip_duration,
    day_of_week,
    hour
  FROM CTE
  WHERE
    started_at <> ended_at AND
    ended_at > started_at AND
    trip_duration > 3 AND
    CAST(trip_duration AS INT) IS NOT NULL AND
    CAST(start_lat AS FLOAT) IS NOT NULL AND
    CAST(start_lng AS FLOAT) IS NOT NULL
)

-- Find peak hour for each member type and day
WITH peak_hours AS (
  SELECT
    member_casual,
    day_of_week,
    hour,
    RANK() OVER (PARTITION BY day_of_week, member_casual ORDER BY COUNT(*) DESC) AS rn
  FROM filtered_data
  GROUP BY day_of_week, hour, member_casual
)

-- Calculate trip statistics and join with peak hour data
SELECT
  a.member_casual,
  MAX(trip_duration) AS max_trip,
  AVG(trip_duration) AS average_trip,
  a.day_of_week AS mode_day_of_week,
  COUNT(*) AS count,
  a.hour AS peak_hour
FROM filtered_data a
JOIN peak_hours b ON a.day_of_week = b.day_of_week AND a.hour = b.hour AND a.member_casual = b.member_casual
WHERE rn = 1
GROUP BY a.member_casual, a.day_of_week, a.hour
ORDER BY count DESC;
