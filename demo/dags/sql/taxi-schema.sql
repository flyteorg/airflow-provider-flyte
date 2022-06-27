CREATE TABLE IF NOT EXISTS "nyc_taxi"."load_files_processed" (
   "file_name" TEXT,
   PRIMARY KEY ("file_name")
)
CLUSTERED INTO 1 SHARDS;

CREATE TABLE IF NOT EXISTS "nyc_taxi"."load_trips_staging" (
   "VendorID" INTEGER,
   "tpep_pickup_datetime" TEXT,
   "tpep_dropoff_datetime" TEXT,
   "passenger_count" INTEGER,
   "trip_distance" REAL,
   "RatecodeID" INTEGER,
   "store_and_fwd_flag" TEXT,
   "PULocationID" INTEGER,
   "DOLocationID" INTEGER,
   "payment_type" INTEGER,
   "fare_amount" REAL,
   "extra" REAL,
   "mta_tax" REAL,
   "tip_amount" REAL,
   "tolls_amount" REAL,
   "improvement_surcharge" REAL,
   "total_amount" REAL,
   "congestion_surcharge" REAL
   "airport_fee" REAL
);

CREATE TABLE IF NOT EXISTS "nyc_taxi"."trips" (
   "id" TEXT NOT NULL,
   "cab_type_id" INTEGER,
   "vendor_id" TEXT,
   "pickup_datetime" TIMESTAMP WITH TIME ZONE,
   "pickup_year" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS DATE_TRUNC('year', "pickup_datetime"),
   "pickup_month" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS DATE_TRUNC('month', "pickup_datetime"),
   "dropoff_datetime" TIMESTAMP WITH TIME ZONE,
   "store_and_fwd_flag" TEXT,
   "rate_code_id" INTEGER,
   "pickup_location" GEO_POINT,
   "dropoff_location" GEO_POINT,
   "passenger_count" INTEGER,
   "trip_distance" DOUBLE PRECISION,
   "trip_distance_calculated" DOUBLE PRECISION GENERATED ALWAYS AS DISTANCE("pickup_location", "dropoff_location"),
   "fare_amount" DOUBLE PRECISION,
   "extra" DOUBLE PRECISION,
   "mta_tax" DOUBLE PRECISION,
   "tip_amount" DOUBLE PRECISION,
   "tolls_amount" DOUBLE PRECISION,
   "ehail_fee" DOUBLE PRECISION,
   "improvement_surcharge" DOUBLE PRECISION,
   "congestion_surcharge" DOUBLE PRECISION,
   "airport_fee" DOUBLE PRECISION,
   "total_amount" DOUBLE PRECISION,
   "payment_type" TEXT,
   "trip_type" INTEGER,
   "pickup_location_id" INTEGER,
   "dropoff_location_id" INTEGER
)
PARTITIONED BY ("pickup_year");
