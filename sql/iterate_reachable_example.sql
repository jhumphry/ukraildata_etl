-- An example of how to use iterate_reachable

DROP TABLE IF EXISTS timetable;

CREATE TEMP TABLE timetable AS SELECT * FROM mca.get_full_timetable('2013-05-09');

--These indexes are crucial for performance
CREATE INDEX idx_timetable_loc ON timetable (location);
CREATE INDEX idx_timetable_uid ON timetable (train_uid);

ANALYZE timetable;

SELECT 	ir.location,
	sd.station_name,
	ir.earliest_arrival,
	ir.earliest_departure,
	ir.path
FROM iterate_reachable('timetable', 'CAMBDGE', '08:00', '2013-05-09', 16) AS ir
	INNER JOIN msn.station_detail AS sd
		ON (ir.location = sd.tiploc_code)
ORDER BY earliest_arrival;
