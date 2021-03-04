/*

    parameters:
        - $udf_jar_path: HDFS path of jar defining PIG UDFs
        - $db : database name
        - $airports_table: airport table name
        - $iata_code: airport iata code
        - $flights_table: flights table name
        - $lower_date: lower date (inclusive) (yyyy-MM-dd)
        - $upper_date: upper date (inclusive) (yyyy-MM-dd)
        - $output_table: output Hive table name
 */

REGISTER $udf_jar_path;

-- AIRPORTS table
airports = LOAD '$db.$airports_table' USING org.apache.hive.hcatalog.pig.HCatLoader();
airports_final = FOREACH airports GENERATE

    iata_code,
    airport,
    city;


-- FLIGHTS table
flights = LOAD '$db.$flights_table' USING org.apache.hive.hcatalog.pig.HCatLoader();

flights_filtered = FILTER flights BY
    (origin_airport == '$iata_code' OR destination_airport == '$iata_code') AND
    ToDate(CONCAT(year, month, day), 'yyyyMMdd') >= ToDate('$lower_date', 'yyyy-MM-dd') AND
    ToDate(CONCAT(year, month, day), 'yyyyMMdd') <= ToDate('$upper_date', 'yyyy-MM-dd') AND
    cancellation_reason IS NOT NULL;

cancelled_flights = FOREACH flights_filtered GENERATE

    origin_airport AS origin_iata,
    destination_airport AS destination_iata,
    CONCAT('-', year, month, day) AS flight_date,
    cancellation_reason;

-- resolve origin by iata_code
join_airport_cancelled_flights_origin = JOIN cancelled_flights BY origin_iata, airports_final BY iata_code;
cancelled_flights_origin = FOREACH join_airport_cancelled_flights_origin GENERATE

    cancelled_flights::origin_iata AS origin_iata,
    airports_final::airport AS origin_aiport,
    airports_final::city AS origin_city,
    cancelled_flights::destination_iata AS destination_iata,
    cancelled_flights::flight_date AS flight_date,
    cancelled_flights::cancellation_reason AS cancellation_reason;

-- resolve destination by iata_code
join_airport_cancelled_flights_destination = JOIN cancelled_flights_origin BY destination_iata, airports_final BY iata_code;
cancelled_flights_output = FOREACH join_airport_cancelled_flights_destination GENERATE

    cancelled_flights_origin::origin_iata AS origin_iata,
    cancelled_flights_origin::origin_aiport AS origin_aiport,
    cancelled_flights_origin::origin_city AS origin_city,
    cancelled_flights_origin::destination_iata AS destination_iata,
    airports_final::airport AS destination_airport,
    airports_final::city AS destination_city,
    cancelled_flights_origin::flight_date AS flight_date,
    cancelled_flights_origin::cancellation_reason AS cancellation_code,
    it.luca.lgd.pig.udf.DecodeCancellationReason(cancelled_flights_origin::cancellation_reason) AS cancellation_rationale;

cancelled_flights_output_ordered = ORDER cancelled_flights_output BY ToDate(flight_date, 'yyyy-MM-dd');

STORE cancelled_flights_output_ordered INTO '$db.$output_table' USING org.apache.hive.hcatalog.pig.HCatLoader();
