﻿CREATE OR REPLACE FUNCTION mca.get_full_timetable (timetable_date date)
RETURNS TABLE (	train_uid character(6),
		stp_indicator character(1),
		loc_order integer,
		location character(7),
		station_name character(30),
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
			SELECT 	train_uid,
				date_runs_from,
				stp_indicator,
				RANK() OVER (PARTITION BY train_uid ORDER BY (stp_indicator COLLATE "C") ASC) AS pos
				FROM mca.basic_schedule
				WHERE 	date_runs_from <= $1 AND
					date_runs_to >= $1 AND
					days_run[EXTRACT(ISODOW FROM $1)]
			) AS foobar
		WHERE 	pos = 1
		), locations AS (
	-- The locations CTE just joins together the three location tables and fills in NULLs as appropriate
	-- adding the WHERE condition in help significantly with the run-time.
		SELECT train_uid, date_runs_from, stp_indicator, loc_order, LEFT(location, 7) AS location,
			NULL AS scheduled_arrival, scheduled_departure, NULL AS scheduled_pass,
			platform
		FROM mca.origin_location
		WHERE 	date_runs_from <= $1
		UNION
		SELECT train_uid, date_runs_from, stp_indicator, loc_order, LEFT(location, 7) AS location,
			scheduled_arrival, scheduled_departure, scheduled_pass,
			platform
		FROM mca.intermediate_location
		WHERE 	date_runs_from <= $1
		UNION
		SELECT train_uid, date_runs_from, stp_indicator, loc_order, LEFT(location, 7) AS location,
			scheduled_arrival, NULL AS scheduled_departure, NULL AS scheduled_pass,
			platform
		FROM mca.terminating_location
		WHERE 	date_runs_from <= $1
		)
	SELECT train_uid, stp_indicator, loc_order, location, station_name, scheduled_arrival,
		scheduled_departure, scheduled_pass, platform
	FROM locations
		INNER JOIN bs USING (train_uid, date_runs_from, stp_indicator)
		LEFT JOIN msn.station_detail AS sd ON (sd.tiploc_code = location)
	ORDER BY train_uid, loc_order
	
$$ LANGUAGE SQL;