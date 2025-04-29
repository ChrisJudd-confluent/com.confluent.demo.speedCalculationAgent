CREATE TABLE ws_flight_calc_data AS
WITH lagged_data AS (
    SELECT
        t.`time`,
        t.lat,
        t.lon,
        t.hMSL,
        LAG(t.lat, 1, t.lat) OVER (ORDER BY $rowtime) AS previous_lat,
        LAG(t.lon, 1, t.lon) OVER (ORDER BY $rowtime) AS previous_lon,
        LAG(t.hMSL, 1, t.hMSL) OVER (ORDER BY $rowtime) AS previous_hMSL,
        t.`time` as currTime,
        LAG(t.`time`, 1, t.`time`) OVER (ORDER BY $rowtime) AS previous_time,
        t.event_time
    FROM
        ws_flight_data AS t
)
SELECT
    l.`time`,
    l.lat,
    l.lon,
    l.hMSL AS current_hMSL,
    h.distance,
    h.bearing,
    h.heightDifference,
    h.glideRatio,
    h.speed
FROM
    lagged_data AS l
    CROSS JOIN LATERAL TABLE(mycalculateWSMetricsWithSpeed(
        l.previous_lat,
        l.previous_lon,
        l.lat,
        l.lon,
        l.previous_hMSL,
        l.hMSL,
        l.currTime,
        l.previous_time
)) AS h(distance, bearing, heightDifference, glideRatio, speed)