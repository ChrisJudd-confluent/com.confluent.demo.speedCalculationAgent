# Calculate Wingsuit Metrics with Speed

This Java code defines a Flink Table Function (`calculateWSMetricsWithSpeed`) designed to calculate flight metrics from wingsuit data. It reads data assumed to be derived from a CSV file, transforms it, and prepares it for sending to a Kafka topic using Apache Flink.

**Intended Use**

This code is primarily a demonstration of how to implement a custom Table Function in Flink for processing flight data. It is **NOT intended for production use** and should be considered for prototyping or experimental purposes only.

**Key Features**

* **Flink Table Function:** Defines a Flink Table Function (`calculateWSMetricsWithSpeed`) to calculate:
    * Distance (using the Haversine formula)
    * Bearing
    * Height difference
    * Glide ratio (based on Pythagorean theorem) 
    * Speed (based on provided timestamps)

* **Timestamp Handling:** Includes a helper function (`convertStringToTimestaamp`) to convert timestamp strings into a usable format.
* **Kafka Integration:** The broader context assumes this UDF is used within a Flink application that produces data to Kafka.

**Code Structure**

The code consists of the following main components:

* **`calculateWSMetricsWithSpeed` Class:**
    * This class defines the `calculateWSMetricsWithSpeed` Table Function.
    * The `eval` method performs the core calculations.
    * The `@FunctionHint` annotation specifies the output schema.
* **`convertStringToTimestaamp` Method:**
    * A helper method to convert timestamp strings to a long format.

**Dependencies**

The code relies on the following Java libraries:

* Apache Flink

**Usage Notes**

* This code is a starting point for a Flink application that processes flight data.
* You will need to adapt it to your specific environment and data sources.
* For production use, consider more robust error handling, logging, and configuration.
* The `calculateWSMetricsWithSpeed` Table Function can be called within Flink SQL queries to calculate the flight metrics.

**Example Flink SQL Query:**

```sql
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
