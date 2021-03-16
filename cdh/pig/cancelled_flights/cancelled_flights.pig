/*
    parameters:
        - $udfJarPath: HDFS path of jar defining PIG UDFs
        - $db : database name
        - $airports: airport table name
        - $iataCode: airport iata code
        - $flights: flights table name
        - $startDate: lower date (inclusive) (yyyy-MM-dd)
        - $endDate: upper date (inclusive) (yyyy-MM-dd)
        - $outputTable: output Hive table name
        - $airlines: airlines table name
        - $userName: user that triggered the data request
        - $wfId: parent workflow job id
 */

REGISTER $udfJarPath;

-- AIRPORTS table
airports = LOAD '$db.$airports' USING org.apache.hive.hcatalog.pig.HCatLoader();
airports_final = FOREACH airports GENERATE

    iata_code,
    airport,
    city;


-- FLIGHTS table
flights = LOAD '$db.$flights' USING org.apache.hive.hcatalog.pig.HCatLoader();

flights_filtered = FILTER flights BY
    (origin_airport == '$iataCode' OR destination_airport == '$iataCode') AND
    ToDate(CONCAT(year, month, day), 'yyyyMMdd') >= ToDate('$startDate', 'yyyy-MM-dd') AND
    ToDate(CONCAT(year, month, day), 'yyyyMMdd') <= ToDate('$endDate', 'yyyy-MM-dd') AND
    cancellation_reason IS NOT NULL;

cancelled_flights = FOREACH flights_filtered GENERATE

    origin_airport AS origin_iata,
    destination_airport AS destination_iata,
    airline,
    CONCAT(year, '-', month, '-', day) AS flight_date,
    cancellation_reason;

-- resolve origin by iata_code
join_airport_cancelled_flights_origin = JOIN cancelled_flights BY origin_iata, airports_final BY iata_code;
cancelled_flights_origin = FOREACH join_airport_cancelled_flights_origin GENERATE

    cancelled_flights::origin_iata AS origin_iata,
    airports_final::airport AS origin_aiport,
    airports_final::city AS origin_city,
    cancelled_flights::airline AS airline_iata,
    cancelled_flights::destination_iata AS destination_iata,
    cancelled_flights::flight_date AS flight_date,
    cancelled_flights::cancellation_reason AS cancellation_reason;

-- resolve destination by iata_code
join_airport_cancelled_flights_destination = JOIN cancelled_flights_origin BY destination_iata, airports_final BY iata_code;
cancelled_flights = FOREACH join_airport_cancelled_flights_destination GENERATE

    cancelled_flights_origin::origin_iata AS origin_iata,
    cancelled_flights_origin::origin_aiport AS origin_aiport,
    cancelled_flights_origin::origin_city AS origin_city,
    cancelled_flights_origin::airline_iata AS airline_iata,
    cancelled_flights_origin::destination_iata AS destination_iata,
    airports_final::airport AS destination_airport,
    airports_final::city AS destination_city,
    cancelled_flights_origin::flight_date AS flight_date,
    cancelled_flights_origin::cancellation_reason AS cancellation_code,
    pig.udf.DecodeCancellationReason(cancelled_flights_origin::cancellation_reason) AS cancellation_rationale;

-- join with airline by iata_code
airlines = LOAD '$db.$airlines' USING org.apache.hive.hcatalog.pig.HCatLoader();
airlines_subset = FOREACH airlines GENERATE

    iata_code,
    airline;
    
cancelled_flights_join_airlines = JOIN cancelled_flights BY airline_iata, airlines_subset BY iata_code;
cancelled_flights_output = FOREACH cancelled_flights_join_airlines GENERATE

    cancelled_flights::origin_iata AS origin_iata,
    cancelled_flights::origin_aiport AS origin_aiport,
    cancelled_flights::origin_city AS origin_city,
    cancelled_flights::airline_iata AS airline_iata,
    airlines_subset::airline AS airline_name,
    cancelled_flights::destination_iata AS destination_iata,
    cancelled_flights::destination_airport AS destination_airport,
    cancelled_flights::destination_city AS destination_city,
    cancelled_flights::flight_date AS flight_date,
    cancelled_flights::cancellation_code AS cancellation_code,
    cancelled_flights::cancellation_rationale AS cancellation_rationale,
    '$userName' AS requesting_user,
    '$wfId' AS workflow_job_id,
    'PIG' AS job_type,
    CurrentTime() AS ts_insert,
    ToString(CurrentTime(), 'yyyy-MM-dd') AS dt_insert;


cancelled_flights_output_ordered = ORDER cancelled_flights_output BY flight_date;

STORE cancelled_flights_output_ordered INTO '$db.$outputTable' USING org.apache.hive.hcatalog.pig.HCatStorer();
