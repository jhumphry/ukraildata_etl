﻿DROP FUNCTION IF EXISTS util.isochron_latlon(
					station char(7),
					depart time,
					timetable_date date);

CREATE FUNCTION  util.isochron_latlon(
				station char(7),
				depart time,
				timetable_date date)
RETURNS TABLE (
		location char(7),
		delay double precision,
		lattitude double precision,
		longitude double precision
		)
AS $IC$
DECLARE
	i RECORD;
	latlon RECORD;
BEGIN
	FOR i IN (SELECT * FROM util.isochron(station, depart, timetable_date)) LOOP
		IF i.easting > 0 AND i.northing > 0 THEN
			latlon = util.natgrid_en_to_latlon(i.easting, i.northing);
			RETURN QUERY SELECT i.location, i.delay, latlon.lat, latlon.lon;
		END IF;
	END LOOP;
END;
$IC$
LANGUAGE 'plpgsql' PARALLEL UNSAFE;
