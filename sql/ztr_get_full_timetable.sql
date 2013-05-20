DROP FUNCTION IF EXISTS ztr.get_full_timetable (timetable_date date);

-- This is simpler than it looks as it is the concatenation of two queries, one for the specified
-- day for all stop locations that are not 'xmidnight' (ie, stops that happen early in the following
-- morning so are out of scope for the specified day) and those for the previous day for those that
-- are 'xmidnight' and so actually take place on the specified day.

CREATE OR REPLACE FUNCTION ztr.get_full_timetable (timetable_date date)
RETURNS TABLE ( train_uid character(6),
        stp_indicator character(1),
        loc_order integer,
        xmidnight boolean,
        location character(7),
        scheduled_arrival time without time zone,
        scheduled_departure time without time zone,
        scheduled_pass time without time zone,
        platform character(3) ) AS $$

    WITH bs AS (
    -- The innermost select gives all the valid schedule details for the day in question
    -- partitions them by train_uid and ranks them by the stp_indicator.
    -- The outermost select discards all but the top-ranked (eg alphabetically first)
    -- stp_indicator, hopefully leaving only one record per train_uid. The date_runs_from
    -- and stp_indicator are still needed to join with the locations.
        SELECT train_uid, date_runs_from, stp_indicator
        FROM (
            SELECT  train_uid,
                date_runs_from,
                stp_indicator,
                RANK() OVER (PARTITION BY train_uid ORDER BY (stp_indicator COLLATE "C") ASC) AS pos
                FROM ztr.basic_schedule
                WHERE   date_runs_from <= $1 AND
                    date_runs_to >= $1 AND
                    days_run[EXTRACT(ISODOW FROM $1)]
            ) AS foobar
        WHERE   pos = 1
    ), bs_p AS (
        SELECT train_uid, date_runs_from, stp_indicator
        FROM (
            SELECT  train_uid,
                date_runs_from,
                stp_indicator,
                RANK() OVER (PARTITION BY train_uid ORDER BY (stp_indicator COLLATE "C") ASC) AS pos
                FROM ztr.basic_schedule
                WHERE   date_runs_from <= ($1 - '1 day'::interval) AND
                    date_runs_to >= ($1 - '1 day'::interval) AND
                    days_run[EXTRACT(ISODOW FROM ($1 - '1 day'::interval))]
            ) AS foobar
        WHERE   pos = 1
    ), locations AS (
    -- The locations CTE just joins together the three location tables and fills in NULLs as appropriate
    -- adding the WHERE condition in help significantly with the run-time.
        SELECT train_uid, date_runs_from, stp_indicator,
            loc_order, FALSE AS xmidnight, sd.tiploc_code AS location,
            NULL AS scheduled_arrival, scheduled_departure, NULL AS scheduled_pass,
            platform
        FROM ztr.origin_location
            INNER JOIN msn.station_detail AS sd ON (LEFT(location,3) = sd._3_alpha_code)
        WHERE   date_runs_from <= $1
        UNION ALL
        SELECT train_uid, date_runs_from, stp_indicator,
            loc_order, xmidnight, sd.tiploc_code AS location,
            scheduled_arrival, scheduled_departure, scheduled_pass,
            platform
        FROM ztr.intermediate_location
            INNER JOIN msn.station_detail AS sd ON (LEFT(location,3) = sd._3_alpha_code)
        WHERE   date_runs_from <= $1
            AND NOT xmidnight
        UNION ALL
        SELECT train_uid, date_runs_from, stp_indicator,
            loc_order, xmidnight, sd.tiploc_code AS location,
            scheduled_arrival, NULL AS scheduled_departure, NULL AS scheduled_pass,
            platform
        FROM ztr.terminating_location
            INNER JOIN msn.station_detail AS sd ON (LEFT(location,3) = sd._3_alpha_code)
        WHERE   date_runs_from <= $1
            AND NOT xmidnight
    ), locations_p AS (
        SELECT train_uid, date_runs_from, stp_indicator,
            loc_order, xmidnight, sd.tiploc_code AS location,
            scheduled_arrival, scheduled_departure, scheduled_pass,
            platform
        FROM ztr.intermediate_location
            INNER JOIN msn.station_detail AS sd ON (LEFT(location,3) = sd._3_alpha_code)
        WHERE   date_runs_from <= ($1 - '1 day'::interval)
            AND xmidnight
        UNION ALL
        SELECT train_uid, date_runs_from, stp_indicator,
            loc_order, xmidnight, sd.tiploc_code AS location,
            scheduled_arrival, NULL AS scheduled_departure, NULL AS scheduled_pass,
            platform
        FROM ztr.terminating_location
            INNER JOIN msn.station_detail AS sd ON (LEFT(location,3) = sd._3_alpha_code)
        WHERE   date_runs_from <= ($1 - '1 day'::interval)
            AND xmidnight
    )
    SELECT train_uid, stp_indicator, loc_order,
        xmidnight, location, scheduled_arrival,
        scheduled_departure, scheduled_pass, platform
    FROM locations
        INNER JOIN bs USING (train_uid, date_runs_from, stp_indicator)
    UNION ALL
    SELECT train_uid, stp_indicator, loc_order,
        xmidnight, location, scheduled_arrival,
        scheduled_departure, scheduled_pass, platform
    FROM locations_p
        INNER JOIN bs_p USING (train_uid, date_runs_from, stp_indicator)
    ORDER BY train_uid, loc_order

$$ LANGUAGE SQL;
