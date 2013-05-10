DROP FUNCTION IF EXISTS msn.earliest_departure(station char(7), arrival time);

CREATE FUNCTION msn.earliest_departure(station char(7), arrival time)
RETURNS time
AS $ED$
DECLARE
    d time;
BEGIN
	SELECT (arrival + '1 minute'::interval*change_time) INTO d
		FROM msn.station_detail
		WHERE tiploc_code = station;

	CASE 	WHEN d IS NULL THEN
			RETURN arrival;
		WHEN d < arrival THEN
			RETURN arrival;
		ELSE
			RETURN d;
	END CASE;
END
$ED$
LANGUAGE 'plpgsql';
