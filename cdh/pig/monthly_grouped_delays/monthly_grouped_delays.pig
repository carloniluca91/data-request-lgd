/*
    parameters:
        - $udfJarPath: HDFS path of jar defining PIG UDFs
        - $db : database name
        - $flights: flights table name
        - $startDate: lower date (inclusive) (yyyy-MM-dd)
        - $endDate: upper date (inclusive) (yyyy-MM-dd)
        - $airlineIatas: Airline IATA code(s). Comma-separated
        - $outputTable: output Hive table name
        - $airlines: airlines table name
        - $userName: user that triggered the data request
        - $wfId: parent workflow job id
 */

REGISTER $udfJarPath;

-- FLIGHTS table
flights = LOAD '$db.$flights' USING org.apache.hive.hcatalog.pig.HCatLoader();

flights_filtered = FILTER flights BY

   ToDate(CONCAT(year, month, day), 'yyyyMMdd') >= ToDate('$startDate', 'yyyy-MM-dd') AND
   ToDate(CONCAT(year, month, day), 'yyyyMMdd') <= ToDate('$endDate', 'yyyy-MM-dd') AND
   pig.udf.IsIn(airline, '$airlineIatas') AND
   (departure_delay >= $delayThreshold OR arrival_delay >= $delayThreshold);

-- flights_filtered_generata = FOREACH flights_filtered GENERATE ;
