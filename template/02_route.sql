CREATE OR REPLACE FUNCTION f_array_remove_elem( -- we need to remove elements by index
    IN arr ANYARRAY, -- in order to avoid removing multiple elements
    IN ix INT,
    OUT new_array ANYARRAY
)
    RETURNS ANYARRAY AS
$$
BEGIN
    IF ix IS NULL THEN
        new_array := arr;
    ELSIF ix IS NOT NULL THEN
        new_array := arr[1:ix - 1] || arr[ix + 1:2147483647];
    END IF;
    RETURN;
END
$$
    LANGUAGE plpgsql;

/**
  Route function. Snaps input points to the network (without radius) and returns a GeoJSON line feature
  indicating the shortest route from the closest point in the network from the starting point to the
  closest route in the network from the end point.

  Accepts an optional array of points that will be avoided during routing. This in combination with
  the snapping introduces some edge cases that are documented "graphically" using ASCII.

  ASCII Legend:
    N:      node
    (N):    snapped node
    N----N: edge
    s:      start_point (snapped to network)
    e:      end_point (snapped to network)
    x:      avoid_point (snapped to network)
 */
DROP FUNCTION IF EXISTS route_ab;
CREATE OR REPLACE FUNCTION route_ab(
    from_point Geometry(Point, 4326),
    to_point Geometry(Point, 4326),
    avoid_points JSON DEFAULT NULL::JSON,
    simplify_factor DOUBLE PRECISION DEFAULT 0.000001,
    cost_column CHARACTER VARYING DEFAULT 'length'::CHARACTER VARYING
)
    RETURNS TABLE
            (
                DISTANCE DOUBLE PRECISION,
                DURATION DOUBLE PRECISION,
                THE_GEOM     GEOMETRY(LineString, 4326)
            )
    LANGUAGE plpgsql
AS
$$
DECLARE

    _start_point                    GEOMETRY(Point, 4326);
    _destination_point              GEOMETRY(Point, 4326);
    _avoid_edge_ids                 BIGINT[];
    _avoid_edge_fractions           DOUBLE PRECISION[];
    _edge_select                    VARCHAR DEFAULT 'SELECT id, source, target, length, geom FROM ${table}';
    _start_nearest_node             RECORD;
    _destination_nearest_node       RECORD;
    _start_nearest_edge             RECORD;
    _destination_nearest_edge       RECORD;
    _start_truncated_idx            INT;
    _destination_truncated_idx      INT;
    _start_fraction                 DOUBLE PRECISION;
    _destination_fraction           DOUBLE PRECISION;
    _start_node                     BIGINT;
    _end_node                       BIGINT;
    _start_node_geom                GEOMETRY(Point, 4326);
    _destination_node_geom          GEOMETRY(Point, 4326);
    _seqs                           BIGINT[];
    _path_seqs                      BIGINT[];
    _nodes                          BIGINT[];
    _edges                          BIGINT[];
    _costs                          DOUBLE PRECISION[];
    _agg_costs                      DOUBLE PRECISION[];
    _route_geometry                 GEOMETRY[];
    _sources                        BIGINT[];
    _targets                        BIGINT[];
    _truncated_edge_ids             BIGINT[];
    _truncated_node_ids             BIGINT[];
    _truncated_geometry             GEOMETRY[];
    _truncated_source_ids           BIGINT[];
    _truncated_target_ids           BIGINT[];
    _return_geom                    GEOMETRY(LineString, 4326);
    _avoid_blocks_same_edge_trivial BOOLEAN DEFAULT FALSE;
    _avoid_blocks_same_node_trivial BOOLEAN DEFAULT FALSE;
    _avoid_edge_ix                  INT;
    _avoid_edge_fraction            DOUBLE PRECISION;
    _avoid_edge_id                  INT;

BEGIN
    _start_point = from_point;
    _destination_point = to_point;
    IF NOT avoid_points::JSON ->> 'type' IS NULL
    THEN
        SELECT ARRAY_AGG(edge_id), ARRAY_AGG(fraction)
        INTO _avoid_edge_ids, _avoid_edge_fractions
        FROM (SELECT l.id AS edge_id, st_linelocatepoint(l.geom, p.geom) AS fraction
              FROM (SELECT dp.path[1] AS id, dp.geom AS geom
                    FROM (SELECT *
                          FROM
                              st_dumppoints(st_transform(
                                      st_setsrid(st_geomfromgeojson(avoid_points), 4326), 3857))) dp) p
                       CROSS JOIN
                   ${table} l
              WHERE st_distance(l.geom, p.geom) =
                    (SELECT MIN(st_distance(l.geom, p.geom))
                     FROM ${table} l)) sq;
        _edge_select := 'SELECT * FROM ${table} WHERE NOT id = ANY(ARRAY[' ||
                        ARRAY_TO_STRING(_avoid_edge_ids, ',') || '])';
    END IF;

    -- closest edge to the start point, we need this to interpolate then and find the "appendix"
    SELECT a.id, a.geom, a.source, a.target
    INTO _start_nearest_edge
    FROM (SELECT * FROM ${table}) a
    ORDER BY a.geom <-> _start_point
    LIMIT 1;

    -- closest edge to the destination_point, we need this to interpolate then and find the "appendix"
    SELECT a.id, a.geom, a.source, a.target
    INTO _destination_nearest_edge
    FROM (SELECT * FROM ${table}) a
    ORDER BY a.geom <-> _destination_point
    LIMIT 1;

    -- start node and destination node vertices from the subset of vertices that are sources or targets in
    -- edges
    SELECT a.*
    INTO _start_nearest_node
    FROM (SELECT v.id, geom
          FROM ${table}_vertices_pgr v
                   JOIN (SELECT id, source, target
                         FROM ${table}) e
                        ON v.id = e.source OR v.id = e.target
          WHERE e.id = _start_nearest_edge.id) a
    ORDER BY a.geom <-> _start_point
    LIMIT 1;

    SELECT a.*
    INTO _destination_nearest_node
    FROM (SELECT v.id, v.geom
          FROM ${table}_vertices_pgr v
                   JOIN (SELECT id, source, target
                         FROM ${table}) e
                        ON v.id = e.source OR v.id = e.target
          WHERE e.id = _destination_nearest_edge.id) a
    ORDER BY a.geom <-> _destination_point
    LIMIT 1;


    -- now we determine how far along the start and destination points are from the nearest edge
    _start_fraction := (SELECT st_linelocatepoint(_start_nearest_edge.geom, _start_point));
    _destination_fraction := (SELECT st_linelocatepoint(_destination_nearest_edge.geom, _destination_point));


    -- Before running pgr_dijkstra, we need to check if the route is trivial. A route is trivial when
    -- 1. Both start and end point snapped to the same edge or
    -- 2. Both start and end point snapped to the same node

    -- If one or multiple avoid points snapped to a start or destination edge
    -- we need to check whether this makes our route non-trivial

    -- Now we have three scenarios:
    --   1. The start and destination snapped to the same edge  N---------s----e----------N
    --   2. They snapped to different edges but the same node  N----------s--(N)--e-------N
    --   3. They snapped to different edges and different nodes, but the edges are adjacent

    -- The third case is tricky, because it's not _really_ trivial: there might be a route over a third edge
    -- that's shorter than just a line between the start and end point that crosses the edges' connecting node.
    -- We need to handle this case differently though because when we truncate the dijkstra result,
    -- the truncated route might be empty. In that case, we need to know whether it's because there is no possible
    -- route or because we truncated it and can simply stitch a route together manually.

    -- Case 1: The start and destination snapped to the same edge N---------s----e----------N
    IF _start_nearest_edge.id = _destination_nearest_edge.id
    THEN
        IF _start_fraction = _destination_fraction THEN
            -- start and destination point snapped to the same location, so there can be no route
            --          (N)-----sd--------N
            RAISE NOTICE 'Cannot compute route, input and output points snapped to same location on graph.';
            RETURN;
        END IF;

        IF NOT avoid_points::JSON ->> 'type' IS NULL THEN
            <<avoid_loop>>
            FOR _avoid_edge_ix IN 1..ARRAY_UPPER(_avoid_edge_ids, 1)
                LOOP
                    _avoid_edge_fraction := _avoid_edge_fractions[_avoid_edge_ix];
                    _avoid_edge_id := _avoid_edge_ids[_avoid_edge_ix];

                    IF LEAST(_start_fraction, _destination_fraction) < _avoid_edge_fraction AND
                       _avoid_edge_fraction < GREATEST(_start_fraction, _destination_fraction) THEN
                        --      (N)---s----x-----e---(N)
                        -- We know that we cannot just return a direct line between the snapped start and
                        -- end points.
                        _avoid_blocks_same_edge_trivial := TRUE;
                        EXIT avoid_loop;
                    END IF;
                END LOOP;
        END IF;

        IF _avoid_blocks_same_edge_trivial THEN
            -- We know the route will not be trivial, but there are two more edge cases that are possible now:
            -- 1.1. The route is not possible because either the start or end points are blocked because there is
            --    another avoid point blocking the "other" direction for either of the points
            -- 1.2. The start and end points snap to the same node: this violates the restriction set by the
            --    avoid_point(s) between start and end point. We need to snap the point closer to the edge's mid-point
            --    to the opposing node.

            <<avoid_loop>>
            FOR _avoid_edge_ix IN 1..ARRAY_UPPER(_avoid_edge_ids, 1)
                LOOP
                    _avoid_edge_fraction := _avoid_edge_fractions[_avoid_edge_ix];
                    _avoid_edge_id := _avoid_edge_ids[_avoid_edge_ix];
                    IF _avoid_edge_fraction < LEAST(_start_fraction, _destination_fraction) OR
                       GREATEST(_start_fraction, _destination_fraction) < _avoid_edge_fraction THEN
                        -- Case 1.1: The route is not possible.
                        --      (N)--x--s--x--e---(N)
                        -- In this example, the start point is blocked by the avoid points.
                        RAISE NOTICE 'Cannot compute route, avoid points lock one of the input points.';
                        RETURN;
                    END IF;
                END LOOP;

            IF _start_nearest_node.id = _destination_nearest_node.id THEN
                -- Case 1.2. Both start and end point snap to the same node, so we need to
                -- snap the one closer to the mid-point to the opposing node
                --          (N)---s---x---e---------------N

                IF ABS(_start_fraction - 0.5) < ABS(_destination_fraction - 0.5) THEN
                    SELECT a.*
                    INTO _start_nearest_node
                    FROM (SELECT v.id, geom
                          FROM ${table}_vertices_pgr v
                                   JOIN (SELECT id, source, target
                                         FROM ${table}) e
                                        ON v.id = e.source OR v.id = e.target
                          WHERE e.id = _start_nearest_edge.id) a
                    ORDER BY a.geom <-> _start_point DESC -- same query as before but reversed ordering
                    LIMIT 1;
                ELSE
                    SELECT a.*
                    INTO _destination_nearest_node
                    FROM (SELECT v.id, v.geom
                          FROM ${table}_vertices_pgr v
                                   JOIN (SELECT id, source, target
                                         FROM ${table}) e
                                        ON v.id = e.source OR v.id = e.target
                          WHERE e.id = _destination_nearest_edge.id) a
                    ORDER BY a.geom <-> _destination_point DESC -- same query as before but reversed ordering
                    LIMIT 1;
                END IF;
            END IF;
        ELSE
            -- The route is trivial, yeah! And since they're on the same edge, we can just connect the snapped
            -- points
            --                  (N)------s---------e-------(N)
            SELECT st_transform(
                           st_simplifypreservetopology(
                                   st_linemerge(
                                           st_collect(sq.geom)
                                       ), simplify_factor
                               ), 4326
                       )
            INTO _return_geom
            FROM (SELECT st_makeline(
                                 st_lineinterpolatepoint(_start_nearest_edge.geom, _start_fraction),
                                 st_lineinterpolatepoint(_start_nearest_edge.geom, _destination_fraction)
                             ) AS geom) sq;
        END IF;
    ELSIF _start_nearest_node.id = _destination_nearest_node.id THEN
        -- Case 2: They snapped to different edges but the same node  N----------s--(N)--e-------N

        IF NOT avoid_points::JSON ->> 'type' IS NULL THEN
            <<avoid_loop>>
            FOR _avoid_edge_ix IN 1..ARRAY_UPPER(_avoid_edge_ids, 1)
                LOOP
                    _avoid_edge_fraction := _avoid_edge_fractions[_avoid_edge_ix];
                    _avoid_edge_id := _avoid_edge_ids[_avoid_edge_ix];

                    IF LEAST(
                               _start_fraction,
                               st_linelocatepoint(_start_nearest_edge.geom, _start_nearest_node.geom)
                           ) < _avoid_edge_fraction AND
                       _avoid_edge_fraction < GREATEST(
                               _start_fraction,
                               st_linelocatepoint(_start_nearest_edge.geom, _start_nearest_node.geom)
                           ) THEN
                        --      N---------s--x--(N)----e----------N
                        _avoid_blocks_same_node_trivial := TRUE;
                        SELECT a.*
                        INTO _start_nearest_node
                        FROM (SELECT v.id, geom
                              FROM ${table}_vertices_pgr v
                                       JOIN (SELECT id, source, target
                                             FROM ${table}) e
                                            ON v.id = e.source OR v.id = e.target
                              WHERE e.id = _start_nearest_edge.id) a
                        ORDER BY a.geom <-> _start_point DESC -- same query as before but reversed ordering
                        LIMIT 1;
                    END IF;
                    -- repeat for end point

                    IF LEAST(
                               _destination_fraction,
                               st_linelocatepoint(_destination_nearest_edge.geom, _destination_nearest_node.geom)
                           ) < _avoid_edge_fraction AND _avoid_edge_fraction < GREATEST(
                            _destination_fraction,
                            st_linelocatepoint(_destination_nearest_edge.geom, _destination_nearest_node.geom)
                        ) THEN
                        _avoid_blocks_same_node_trivial := TRUE;
                        SELECT a.*
                        INTO _destination_nearest_node
                        FROM (SELECT v.id, v.geom
                              FROM ${table}_vertices_pgr v
                                       JOIN (SELECT id, source, target
                                             FROM ${table}) e
                                            ON v.id = e.source OR v.id = e.target
                              WHERE e.id = _destination_nearest_edge.id) a
                        ORDER BY a.geom <-> _destination_point DESC -- same query as before but reversed ordering
                        LIMIT 1;
                    END IF;
                END LOOP;
        END IF;

        IF NOT _avoid_blocks_same_node_trivial THEN
            -- In case 2, the route is trivial if we didn't have to snap either the start or end point
            -- to the opposing node
            SELECT st_transform(st_simplifypreservetopology(st_linemerge(st_collect(sq.geom)), simplify_factor),
                                4326)
            INTO _return_geom
            FROM (SELECT st_makeline(
                                 ARRAY_AGG(
                                         ARRAY [
                                             st_lineinterpolatepoint(_start_nearest_edge.geom, _start_fraction),
                                             _destination_nearest_node.geom,
                                             st_lineinterpolatepoint(_destination_nearest_edge.geom,
                                                                     _destination_fraction)
                                             ]::GEOMETRY[]
                                     )
                             ) AS geom) sq;
        END IF;
    END IF;

    -- Now that we checked the edge cases, the route geometry might already have been populated
    -- in the case of a trivial route
    IF _return_geom IS NULL THEN
        -- run the dijkstra and aggregate the result into arrays
        SELECT ARRAY_AGG(a.seq)      AS seqs,
               ARRAY_AGG(a.path_seq) AS path_seqs,
               ARRAY_AGG(a.node)     AS nodes,
               ARRAY_AGG(a.edge)     AS edges,
               ARRAY_AGG(a.cost)     AS costs,
               ARRAY_AGG(a.agg_cost) AS agg_costs,
               ARRAY_AGG(
                       CASE
                           WHEN a.node = b.source THEN b.geom
                           ELSE st_reverse(b.geom)
                           END
                   )                 AS route_geom,
               ARRAY_AGG(b.source)   AS sources,
               ARRAY_AGG(b.target)   AS targets
        INTO
            _seqs,
            _path_seqs,
            _nodes,
            _edges,
            _costs,
            _agg_costs,
            _route_geometry,
            _sources,
            _targets
        FROM (SELECT *
              FROM pgr_dijkstra(
                                                          'SELECT id, source, target, ' || cost_column ||
                                                          ' AS cost FROM (' || _edge_select || ') as r, ' ||
                                                          '(SELECT ST_Expand(ST_Extent(geom),100) as box FROM ${table} as l1 WHERE l1.source = ' ||
                                                          _start_nearest_node.id ||
                                                          ' OR l1.target = ' || _destination_nearest_node.id ||
                                                          ') as box WHERE r.geom && box.box',
                                                          _start_nearest_node.id,
                                                          _destination_nearest_node.id,
                                                          FALSE
                  )) AS a
                 JOIN ${table} b
                      ON a.edge = b.id;
        IF ARRAY_LENGTH(_seqs, 1) IS NULL THEN
            RAISE NOTICE 'No route found.';
            RETURN;
        END IF;


        -- because there will always be a fraction we want to remove the nearest edges
        -- if they are part of the route edges, they don't necessarily have to be because
        -- the start or destination point may be "further" along then the nearest node
        _start_truncated_idx := (SELECT array_position(_edges, _start_nearest_edge.id));
        _destination_truncated_idx := (SELECT array_position(_edges, _destination_nearest_edge.id));

        _truncated_edge_ids := (SELECT f_array_remove_elem(_edges, _destination_truncated_idx));
        _truncated_edge_ids := (SELECT f_array_remove_elem(_truncated_edge_ids, _start_truncated_idx));

        _truncated_node_ids := (SELECT f_array_remove_elem(_nodes, _destination_truncated_idx));
        _truncated_node_ids := (SELECT f_array_remove_elem(_truncated_node_ids, _start_truncated_idx));

        _truncated_source_ids := (SELECT f_array_remove_elem(_sources, _destination_truncated_idx));
        _truncated_source_ids := (SELECT f_array_remove_elem(_truncated_source_ids, _start_truncated_idx));

        _truncated_target_ids := (SELECT f_array_remove_elem(_targets, _destination_truncated_idx));
        _truncated_target_ids := (SELECT f_array_remove_elem(_truncated_target_ids, _start_truncated_idx));

        _truncated_geometry := (SELECT f_array_remove_elem(_route_geometry, _destination_truncated_idx));
        _truncated_geometry := (SELECT f_array_remove_elem(_truncated_geometry, _start_truncated_idx));


        -- The truncated dijkstra result is not empty, we proceed by stitching together the truncated
        -- route with the start and end fractions

        -- fetch the start node of the truncated route node ids
        _start_node := (SELECT _truncated_node_ids[1]);
        -- and the end node which is either the source or target, i.e. we want the
        -- node that is not included because pgrouting will always only show the start node
        -- of the edges in sequence
        _end_node := (SELECT _truncated_node_ids[ARRAY_LENGTH(_truncated_node_ids, 1)]);
        IF _end_node = _truncated_target_ids[ARRAY_LENGTH(_truncated_target_ids, 1)] THEN
            _end_node := _truncated_source_ids[ARRAY_LENGTH(_truncated_source_ids, 1)];
        ELSIF _end_node = _truncated_source_ids[ARRAY_LENGTH(_truncated_source_ids, 1)] THEN
            _end_node := _truncated_target_ids[ARRAY_LENGTH(_truncated_target_ids, 1)];
        END IF;

        -- grab the geometry of start and destination node again of the truncated route
        _start_node_geom := (SELECT geom FROM ${table}_vertices_pgr WHERE id = _start_node);
        _destination_node_geom := (SELECT geom FROM ${table}_vertices_pgr WHERE id = _end_node);

        SELECT st_transform(st_simplifypreservetopology(st_linemerge(st_collect(sq.geom)), simplify_factor), 4326)
        INTO _return_geom
        FROM (SELECT CASE
                         WHEN st_equals(_start_node_geom, st_startpoint(_start_nearest_edge.geom)) THEN
                             st_linesubstring(_start_nearest_edge.geom, 0, _start_fraction)
                         WHEN st_equals(_start_node_geom, st_endpoint(_start_nearest_edge.geom)) THEN
                             st_linesubstring(_start_nearest_edge.geom, _start_fraction, 1)
                         END AS geom
              UNION
              -- include our truncated geometries inbetween the appendices
              SELECT UNNEST(_truncated_geometry)
              UNION
              -- here we do the same as above for the tail of the route
              SELECT CASE
                         WHEN st_equals(_destination_node_geom, st_startpoint(_destination_nearest_edge.geom)) THEN
                             st_linesubstring(_destination_nearest_edge.geom, 0, _destination_fraction)
                         WHEN st_equals(_destination_node_geom, st_endpoint(_destination_nearest_edge.geom)) THEN
                             st_linesubstring(_destination_nearest_edge.geom, _destination_fraction, 1)
                         END AS geom) sq;

    END IF;


    RETURN QUERY SELECT b.distance, b.duration, b.geom as the_geom
                 FROM (SELECT a.geom,
                              ROUND(st_lengthspheroid(a.geom, 'SPHEROID["WGS 84",6378137,298.257223563]')) AS distance,
                              ROUND(st_lengthspheroid(a.geom, 'SPHEROID["WGS 84",6378137,298.257223563]') /
                                    1.38889)                                                               AS duration -- 5 km/h = 1.38889 m/s
                       FROM (SELECT _return_geom AS geom) a) AS b;

    RETURN;

END
$$;


-- test
SELECT * FROM route_ab(
               st_setsrid(st_makepoint(20.285683, 42.555843), 4326),---83.079682, 42.398064,
               st_setsrid(st_makepoint(20.2940857, 42.5301562), 4326),---83.078242, 42.397758,
               '{}', 0.000001);
