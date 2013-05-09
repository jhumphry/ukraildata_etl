DROP FUNCTION IF EXISTS iterate_reachable(  timetable varchar,
                        station char(7),
                        depart time,
                        timetable_date date,
                        iteration_limit integer);

CREATE FUNCTION iterate_reachable(  timetable varchar,
                    station char(7),
                    depart time,
                    timetable_date date,
                    iteration_limit integer)
RETURNS TABLE (
        location char(7),
        earliest_arrival time,
        path char(6)[]
        )
AS $IR$
DECLARE
    start_station record;
    next_station record;
    current_arrival time;
BEGIN
    DROP TABLE IF EXISTS temp_i_r;
    CREATE TEMPORARY TABLE temp_i_r (
        location char(7),
        earliest_arrival time,
        path char(6)[],
        processed boolean
        );

    CREATE UNIQUE INDEX idx_temp_i_r ON temp_i_r (location);

    INSERT INTO temp_i_r VALUES (station, depart, NULL, False);

    <<iterations>>
    FOR i IN 1 .. iteration_limit LOOP

        <<over_unprocessed_starting_stations>>
        FOR start_station IN (SELECT * FROM temp_i_r WHERE NOT processed) LOOP

            UPDATE temp_i_r SET processed = TRUE
                WHERE temp_i_r.location = start_station.location;

            <<over_new_connections>>
            FOR next_station IN (   SELECT *
                        FROM mca.get_direct_connections(timetable,
                        start_station.location,
                        start_station.earliest_arrival)
                        UNION
                        SELECT *
                        FROM alf.get_direct_connections(start_station.location,
                        start_station.earliest_arrival,
                        timetable_date)) LOOP

                SELECT temp_i_r.earliest_arrival
                INTO current_arrival
                FROM temp_i_r
                WHERE temp_i_r.location = next_station.location;

                CASE
                    WHEN current_arrival IS NULL AND
                        next_station.earliest_arrival > depart THEN
                        INSERT INTO temp_i_r VALUES (
                                next_station.location,
                                next_station.earliest_arrival,
                                start_station.path || ARRAY[next_station.train_uid],
                                FALSE );

                    WHEN current_arrival > next_station.earliest_arrival AND
                        next_station.earliest_arrival > depart THEN
                        UPDATE temp_i_r
                        SET     earliest_arrival = next_station.earliest_arrival,
                            path = start_station.path || ARRAY[next_station.train_uid],
                            processed = FALSE
                        WHERE temp_i_r.location = next_station.location;

                    ELSE
                        NULL;
                END CASE;
            END LOOP over_new_connections;
        END LOOP over_unprocessed_starting_stations;
    END LOOP iterations;

    RETURN QUERY SELECT temp_i_r.location, temp_i_r.earliest_arrival, temp_i_r.path FROM temp_i_r;
END;

$IR$
LANGUAGE 'plpgsql' ;
