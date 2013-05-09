DROP FUNCTION IF EXISTS alf.get_direct_connections(
			station char(7),
			depart time,
			timetable_date date
			);


-- The Additional Fixed Link data does not give a schedule and the links given
-- are functional between the specified dates and times. Where there is a standard
-- link between two places of a given type, and another given for a particular span
-- of dates I have assumed that the longer is appropriate. I have assumed that all
-- fixed links should be symmetrical, although they are not specified that way in
-- the data.

CREATE FUNCTION alf.get_direct_connections(
			station char(7),
			depart time,
			timetable_date date
			)
RETURNS TABLE (
		location char(7),
		earliest_arrival time,
		train_uid char(6)
		)
AS $B$
DECLARE
    dow Integer;
    origin_3alpha CHAR(3);
BEGIN

    SELECT _3_alpha_code INTO origin_3alpha
	FROM msn.station_detail
	WHERE tiploc_code = station;

    IF origin_3alpha IS NULL THEN
	RAISE NOTICE 'Could not find 3alpha for: %', station;
	RETURN;
    END IF;

    SELECT EXTRACT(isodow FROM timetable_date) INTO STRICT dow;

    RETURN QUERY
    SELECT tiploc_code AS location,
    MAX((depart + link_time * '1 minute'::interval)) AS earliest_arrival,
    LEFT(mode, 6)::char(6) AS train_uid
    FROM    (
	SELECT  mode, destination, link_time,
	    start_time, end_time, start_date,
	    end_date, days_of_week
	FROM alf.alf
	WHERE origin = origin_3alpha
	UNION
	SELECT  mode, origin AS destination, link_time,
	    start_time, end_time, start_date,
	    end_date, days_of_week
	FROM alf.alf
	WHERE destination = origin_3alpha
	) AS a
	INNER JOIN msn.station_detail AS sd
	ON (a.destination = sd._3_alpha_code)
    WHERE   a.days_of_week[dow] AND
	COALESCE(timetable_date BETWEEN a.start_date AND a.end_date, TRUE) AND
	depart BETWEEN a.start_time AND a.end_time AND
	(depart + a.link_time * '1 minute'::interval)::time > depart
    GROUP BY location, train_uid;
END;
$B$
LANGUAGE 'plpgsql' ;
