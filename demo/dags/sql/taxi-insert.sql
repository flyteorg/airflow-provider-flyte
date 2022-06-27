INSERT INTO nyc_taxi.trips (id, cab_type_id, vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, passenger_count, trip_distance, rate_code_id, store_and_fwd_flag, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, total_amount)
SELECT
  gen_random_text_uuid(),
  1,
  TRY_CAST("VendorID" AS INTEGER),
  TRY_CAST(tpep_pickup_datetime AS TIMESTAMP),
  TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP),
  "PULocationID",
  "DOLocationID",
  TRY_CAST(passenger_count AS INTEGER),
  TRY_CAST(trip_distance AS FLOAT),
  TRY_CAST("RatecodeID" AS INTEGER),
  store_and_fwd_flag,
  payment_type,
  TRY_CAST(fare_amount AS FLOAT),
  TRY_CAST(extra AS FLOAT),
  TRY_CAST(mta_tax AS FLOAT),
  TRY_CAST(tip_amount AS FLOAT),
  TRY_CAST(tolls_amount AS FLOAT),
  TRY_CAST(improvement_surcharge AS FLOAT),
  TRY_CAST(congestion_surcharge AS FLOAT),
  TRY_CAST(airport_fee as FLOAT),
  TRY_CAST(total_amount AS FLOAT)
FROM nyc_taxi.load_trips_staging
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL;
