DROP FUNCTION IF EXISTS util.isochron(	station char(7),
					depart time,
					timetable_date date);

CREATE FUNCTION  util.isochron(	station char(7),
				depart time,
				timetable_date date)
RETURNS TABLE (
		location char(7),
		delay double precision,
		easting integer,
		northing integer
		)
AS $IC$
DECLARE

BEGIN
	CREATE TEMP TABLE isochron_tt AS
		SELECT * FROM mca.get_full_timetable(timetable_date);

	CREATE INDEX idx_isochron_tt_loc ON isochron_tt (location);
	CREATE INDEX idx_isochron_tt_uid ON isochron_tt (train_uid);
	ANALYZE isochron_tt;

	RETURN QUERY SELECT ir.location,
			EXTRACT(hours from (ir.earliest_arrival - depart)) +
			EXTRACT(minutes from (ir.earliest_arrival - depart)) / 60.0 AS delay,
			rr.easting,
			rr.northing
		FROM util.iterate_reachable(	'isochron_tt',
						station,
						depart,
						'2013-05-09',
						10) AS ir
			INNER JOIN naptan.railreferences AS rr
				ON (ir.location = rr.tiploc)
		ORDER BY ir.earliest_arrival;

	DROP TABLE isochron_tt;
END;
$IC$
LANGUAGE 'plpgsql';
