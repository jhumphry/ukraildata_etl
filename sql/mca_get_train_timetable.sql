DROP FUNCTION IF EXISTS mca.get_train_timetable (train_uid character(6), timetable_date date);

CREATE OR REPLACE FUNCTION mca.get_train_timetable (train_uid character(6), timetable_date date)
RETURNS TABLE ( train_uid character(6),
        stp_indicator character(1),
        loc_order integer,
        xmidnight boolean,
        location character(7),
        station_name character(30),
        scheduled_arrival time without time zone,
        scheduled_departure time without time zone,
        scheduled_pass time without time zone,
        platform character(3) ) AS $$

    WITH bs AS (
        SELECT train_uid, date_runs_from, stp_indicator
        FROM mca.basic_schedule
        WHERE   train_uid = $1 AND
            date_runs_from <= $2 AND
            date_runs_to >= $2 AND
            days_run[EXTRACT(ISODOW FROM $2)]
        ORDER BY (stp_indicator COLLATE "C") ASC
        LIMIT 1
        ), locations AS (
        SELECT train_uid, date_runs_from, stp_indicator,
            loc_order, FALSE AS xmidnight, location,
            NULL AS scheduled_arrival, scheduled_departure, NULL AS scheduled_pass,
            platform
        FROM mca.origin_location
        WHERE train_uid = $1 AND date_runs_from <= $2
        UNION
        SELECT train_uid, date_runs_from, stp_indicator,
            loc_order, xmidnight, location,
            scheduled_arrival, scheduled_departure, scheduled_pass,
            platform
        FROM mca.intermediate_location
        WHERE train_uid = $1 AND date_runs_from <= $2
        UNION
        SELECT train_uid, date_runs_from, stp_indicator,
            loc_order, xmidnight, location,
            scheduled_arrival, NULL AS scheduled_departure, NULL AS scheduled_pass,
            platform
        FROM mca.terminating_location
        WHERE train_uid = $1 AND date_runs_from <= $2
        )
    SELECT train_uid, stp_indicator, loc_order, xmidnight, location, station_name, scheduled_arrival,
        scheduled_departure, scheduled_pass, platform
    FROM locations
        INNER JOIN bs USING (train_uid, date_runs_from, stp_indicator)
        LEFT JOIN msn.station_detail AS sd ON (sd.tiploc_code = location)
    ORDER BY loc_order

$$ STABLE LANGUAGE SQL PARALLEL SAFE;
