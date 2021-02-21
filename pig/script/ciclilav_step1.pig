/*
    paths:
        - $ciclilavStep1_input: input HDFS path
        - $ciclilavStep1_output: output HDFS path

    parameters:
        - $start_date: start of data request range
        - $end_date: end of data request range
 */

ciclilav_input = LOAD '$ciclilavStep1_input' USING PigStorage(';') AS (
            index: int,
            first_name: chararray,
            last_name: chararray,
            birth_date: chararray
    );

ranged_input = FILTER ciclilav_input BY
    ToDate(birth_date, 'yyyy-MM-dd') >= ToDate('$start_date', 'yyyy-MM-dd') AND
    ToDate(birth_date, 'yyyy-MM-dd') <= ToDate('$end_date', 'yyyy-MM-dd');

STORE ranged_input INTO '$ciclilavStep1_output' USING PigStorage(';');