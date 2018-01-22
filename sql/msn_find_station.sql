DROP FUNCTION IF EXISTS msn.find_station(station varchar);

CREATE FUNCTION msn.find_station(station varchar)
RETURNS char(7)
AS $ED$
DECLARE
    d char(7);
BEGIN
	IF char_length(trim(station)) = 0 THEN
		RETURN NULL;
	END IF;

	SELECT tiploc_code INTO d 
	FROM msn.station_detail
	WHERE tiploc_code = rpad(upper(station),7);

	IF d IS NOT NULL THEN
		RETURN d;
	END IF;
	
	EXECUTE $$
		SELECT tiploc_code
		FROM (
		SELECT station_name, cate_type, tiploc_code
		FROM msn.station_detail
		UNION
		SELECT alias_name AS station_name, cate_type, tiploc_code
		FROM msn.station_detail
			INNER JOIN msn.station_alias USING (station_name)
		) AS foo
		WHERE station_name LIKE '%' || $1 || '%'
		ORDER BY (CASE WHEN cate_type = 9 THEN -1 ELSE cate_type END) DESC
		LIMIT 1;
	$$ INTO d USING upper(station);

	RETURN d;
END
$ED$
STABLE
LANGUAGE 'plpgsql' PARALLEL SAFE;
