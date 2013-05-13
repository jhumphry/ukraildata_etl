DROP FUNCTION IF EXISTS util.get_direct_connections(timetable varchar, station char(7), depart time);

CREATE FUNCTION util.get_direct_connections(timetable varchar, station char(7), depart time)
RETURNS TABLE (
        location char(7),
        earliest_arrival time,
        train_uid char(6)
        )
AS $B$
BEGIN

-- The CTE valid_trains identifies all the train schedules that depart from the given station
-- after the given time. The query valid_routes then makes a list of all the subsequent
-- stations on those train schedules, but not wrapping round across midnight.
-- The window function partitions the results by location and
-- ranks the results by arrival time, with before-midnight trains first.
-- The outer query then only selects the earliest train arrival time for each location.

RETURN QUERY EXECUTE format('
    SELECT location, scheduled_arrival, train_uid
    FROM (
        WITH valid_trains AS(
            SELECT train_uid, loc_order, xmidnight
            FROM %I
            WHERE location = $1
            AND scheduled_departure > $2
            )
        SELECT  location,
            scheduled_arrival,
            valid_trains.train_uid,
            rank() OVER (PARTITION BY location ORDER BY timetable.xmidnight, scheduled_arrival ASC) AS foo
        FROM %I AS timetable
            INNER JOIN valid_trains
                ON (timetable.train_uid = valid_trains.train_uid
                    AND timetable.loc_order > valid_trains.loc_order
                    AND timetable.xmidnight = valid_trains.xmidnight)
        WHERE scheduled_arrival IS NOT NULL ) AS valid_routes
    WHERE foo = 1;', timetable, timetable)
        USING station, depart;
END;
$B$
LANGUAGE 'plpgsql' ;
