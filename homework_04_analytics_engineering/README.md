## Homework Week4

Q1 and Q2 has no sql command.

Question 3. Counting Records in fct_monthly_zone_revenue
```
select count(*) from ny_taxi.prod.fct_monthly_zone_revenue
```

Question 4. Best Performing Zone for Green Taxis (2020)
```
SELECT pickup_zone,
       SUM(revenue_monthly_total_amount) AS total_revenue
FROM ny_taxi.prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND YEAR(revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```

Question 5. Green Taxi Trip Counts (October 2019)
```
SELECT SUM(total_monthly_trips) AS total_trips
FROM ny_taxi.prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND YEAR(revenue_month) = 2019
  AND MONTH(revenue_month) = 10;
```

Question 6. Build a Staging Model for FHV Data
```
select count(*) from ny_taxi.prod.stg_fhv_tripdata
```