#importing required libaries
from pyspark.sql import SparkSession

# creating SparkSession
spark = SparkSession.builder.appName("delay_flights").getOrCreate()

# creating an external table and loading data from S3 bucket
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS delay_flights (
      ID INT,
      Year INT,
      Month INT,
      DayOfMonth INT,
      DayOfWeek INT,
      DepTime INT,
      CRSDepTime INT,
      ArrTime INT,
      CRSArrTime INT,
      UniqueCarrier STRING,
      FlightNum INT,
      TailNum STRING,
      ActualElapsedTime INT,
      CRSElapsedTime INT,
      AirTime INT,
      ArrDelay INT,
      DepDelay INT,
      Origin STRING,
      Dest STRING,
      Distance INT,
      TaxiIn INT,
      TaxiOut INT,
      Cancelled INT,
      CancellationCode STRING,
      Diverted INT,
      CarrierDelay INT,
      WeatherDelay INT,
      NASDelay INT,
      SecurityDelay INT,
      LateAircraftDelay INT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 's3://myassingment/'
""")

# Checking existence of temporary database
spark.catalog.listDatabases()

# Reading the "delay_flights" table from the "delay_flights" database
df = spark.read.table("delay_flights")

# Showing the first 5 rows of the DataFrame
df.show(5)

-- year wise carrier delay 2003-2010

import time
start_at = time.time()
carrier_delay_df = spark.sql("SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS Average_NAS_Delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 AND NASDelay IS NOT NULL AND ArrDelay IS NOT NULL GROUP BY Year ORDER BY Year").show()
end_at = time.time()
execution_difference = end_at - start_at
print(f"Execution: {execution_difference} seconds")

-- year wise NAS delay 2003-2010
import time
start_at = time.time()
nas_delay_df = spark.sql("SELECT Year, AVG((NASDelay / ArrDelay) * 100) AS Average_NAS_Delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 AND NASDelay IS NOT NULL AND ArrDelay IS NOT NULL GROUP BY Year ORDER BY Year").show()
end_at = time.time()
execution_difference = end_at - start_at
print(f"Execution: {execution_difference} seconds")

-- year wise Weather delay 2003-2010
import time
start_at = time.time()
weather_delay_df = spark.sql("SELECT Year, AVG((WeatherDelay / ArrDelay) * 100) AS Average_Weather_Delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 AND WeatherDelay IS NOT NULL AND ArrDelay IS NOT NULL GROUP BY Year ORDER BY Year").show()
end_at = time.time()
execution_difference = end_at - start_at
print(f"Execution: {execution_difference} seconds")

-- year wise late aircraft delay 2003-2010
import time
start_at= time.time()
late_aircraft_delay_df = spark.sql("SELECT Year, AVG((LateAircraftDelay / ArrDelay) * 100) AS Average_Late_Air_Craft_Delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 AND LateAircraftDelay IS NOT NULL AND ArrDelay IS NOT NULL GROUP BY Year ORDER BY Year").show()
end_at = time.time()
execution_difference = end_at - start_at
print(f"Execution: {execution_difference} seconds")

-- year wise security delay 2003-2010
import time
start_at = time.time()
security_delay_df = spark.sql("SELECT Year, AVG((SecurityDelay / ArrDelay) * 100) AS Average_Security_Delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 AND SecurityDelay IS NOT NULL AND ArrDelay IS NOT NULL GROUP BY Year ORDER BY Year").show()
end_at = time.time()
execution_difference = end_at - start_at
print(f"Execution: {execution_difference} seconds")