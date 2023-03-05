

 -- Creating Table

CREATE EXTERNAL TABLE delay_flights (
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
LOCATION 's3://myassingment/';

-- Carrier delay 2003-2010
SELECT Year, 
AVG((CarrierDelay / ArrDelay) * 100) AS Average_Carrier_Delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010 
GROUP BY Year
ORDER BY Year;

-- Year wise NAS delay 2003-2010
SELECT Year, 
AVG((NASDelay / ArrDelay) * 100) AS Average_NAS_Delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010 
GROUP BY Year
ORDER BY Year;

-- Year wise Weather delay 2003-2010
SELECT Year, 
AVG((WeatherDelay / ArrDelay) * 100) AS Averge_Weather_Delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010 
GROUP BY Year
ORDER BY Year;

-- Year wise late aircraft delay 2003-2010
SELECT Year, 
AVG((LateAircraftDelay / ArrDelay) * 100) AS Average_Late_Air_Craft_Delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year
ORDER BY Year;

-- Year wise security delay 2003-2010
SELECT Year, 
AVG((SecurityDelay / ArrDelay) * 100) AS Average_Security_Delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year
ORDER BY Year;