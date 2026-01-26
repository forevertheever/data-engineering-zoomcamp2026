## Homework Week1

Q1. Run docker with the python:3.13 image. Use an entrypoint bash to interact with the container. What's the version of pip in the image?

Answer: 25.3
Solution: Running the following code in the terminal to establish a python container in docker with version python:3.13
```bash
docker run -it \
    --rm \
    --entrypoint=bash \
    python:3.13
```
and then run **pip -v** to check the pip version, which shows **25.3**.

Q3. Counting short trips

Solution: Sql command as following:
```bash
SELECT COUNT(*) AS trips FROM green_trip_data WHERE DATE(lpep_pickup_datetime) >= '2025-11-01' AND DATE(lpep_pickup_datetime) < '2025-12-01' AND trip_distance <= 1;
```
Result: **8007**

Question 4. Longest trip for each day

Solution: Sql command as following:
```bash
SELECT DATE(lpep_pickup_datetime) AS longest_day, MAX(trip_distance) AS longest_distance FROM green_trip_data WHERE trip_distance < 100 GROUP BY DATE(lpep_pickup_datetime) ORDER BY longest_distance DESC LIMIT 1;
```
Result: **2025-11-14**

Question 5. Biggest pickup zone

Solution: Sql command as following:
```bash
SELECT
    z."Zone" AS pickup_zone,
    SUM(t.total_amount) AS total_amount_sum
FROM green_trip_data t
JOIN data_with_zones z
    ON t."PULocationID" = z."LocationID"
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;
```
Result: **East Harlem North**

Question 6. Largest tip

Solution: Sql command as following:
```bash

SELECT
    z_do."Zone" AS dropoff_zone,
    MAX(t.tip_amount) AS max_total_tip
FROM green_trip_data t
JOIN data_with_zones z_pu
    ON t."PULocationID" = z_pu."LocationID"
JOIN data_with_zones z_do
    ON t."DOLocationID" = z_do."LocationID"
WHERE z_pu."Zone" = 'East Harlem North'
AND DATE(t.lpep_pickup_datetime) >= '2025-11-01'
AND DATE(t.lpep_pickup_datetime) <  '2025-12-01'
GROUP BY z_do."Zone"
ORDER BY max_total_tip DESC
LIMIT 1;
```
Result: **Yorkville West**

