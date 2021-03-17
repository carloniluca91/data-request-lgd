/*
    parameters:
        - $db : database name
        - $flights: flights table name
        - $airlineIatas: Airline IATA code(s). Comma-separated
        - $startDate: lower date (inclusive) (yyyy-MM)
        - $endDate: upper date (inclusive) (yyyy-MM)
        - $outputTable: output Hive table name
        - $airlines: airlines table name
        - $userName: user that triggered the data request
        - $wfId: parent workflow job id
 */

-- FLIGHTS table
flights = LOAD '$db.$flights' USING org.apache.hive.hcatalog.pig.HCatLoader();