DROP FUNCTION IF EXISTS snap_point_to_network;
CREATE OR REPLACE FUNCTION snap_point_to_network(
    IN point GEOMETRY,
    IN snap_distance DOUBLE PRECISION,
    OUT new_point GEOMETRY
) AS
$$
DECLARE
    _nearest_on_edges    GEOMETRY;
    _nearest_on_vertices GEOMETRY;
BEGIN
    SELECT point INTO new_point;
    -- default: no snapping

    -- First: get the nearest point on the nearest edge
    SELECT st_lineinterpolatepoint(geom, st_linelocatepoint(geom, new_point))
    INTO _nearest_on_edges
    FROM ${table}
    ORDER BY geom <-> new_point
    LIMIT 1;

    -- Then check whether it's within our snapping distance – if true, move _start/_endpoint
    IF st_dwithin(new_point, _nearest_on_edges, snap_distance) THEN
        new_point = _nearest_on_edges;
    END IF;

    -- Now, check whether the original start-/endpoint lies within snapping distance to the next vertex
    -- we use the original start-/endpoint to prevent "over"-snapping
    SELECT geom INTO _nearest_on_vertices FROM ${table}_vertices_pgr ORDER BY geom <-> point LIMIT 1;

    IF st_dwithin(point, _nearest_on_vertices, snap_distance) THEN
        new_point = _nearest_on_vertices; -- if true, move point again
    END IF;

    RETURN;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION snap_point_to_network IS '
Helper function for snapping a point to existing edges or vertices in the cable network.

It first snaps the point to the nearest point on the nearest edge, if it is within the snapping tolerance.
It then checks, whether the point can further be snapped to a vertex, using the same snapping tolerance.
';

CREATE OR REPLACE FUNCTION truncate_line(
    IN line GEOMETRY(linestring)
) RETURNS GEOMETRY(linestring) AS
$$
DECLARE
    _out_line GEOMETRY(linestring);
BEGIN

    SELECT st_removepoint(
                   st_removepoint(
                           line, st_npoints(line) - 1
                       ), 0)
    INTO _out_line;

    RETURN _out_line;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION truncate_line IS '
Removes the first and last points from a line. Does not check whether line consists of less than 3
points.
';

DROP FUNCTION IF EXISTS snap_line_to_network;
CREATE OR REPLACE FUNCTION snap_line_to_network(
    IN line GEOMETRY,
    IN snap_distance DOUBLE PRECISION,
    OUT new_line GEOMETRY(LineString, 4326)
) AS
$$
DECLARE
    _startpoint GEOMETRY;
    _endpoint   GEOMETRY;
BEGIN
    SELECT st_startpoint(line), st_endpoint(line) INTO _startpoint, _endpoint;

    SELECT snap_point_to_network(_startpoint, snap_distance),
           snap_point_to_network(_endpoint, snap_distance)
    INTO _startpoint, _endpoint;
    IF st_numpoints(line) < 3 THEN
        SELECT st_makeline(_startpoint, _endpoint) INTO new_line;
        RETURN;
    ELSIF st_numpoints(line) = 3 THEN
        -- in the case of 3 points, removing two would leave us with a point, so we can't truncate
        SELECT st_makeline(ARRAY [_startpoint, st_pointn(line, 2), _endpoint]) INTO new_line;
        RETURN;
    END IF;

    SELECT st_addpoint(
                   st_addpoint(truncate_line(line), _startpoint, 0),
                   _endpoint)
    INTO new_line;
    RETURN;

END
$$ LANGUAGE plpgsql STABLE;
COMMENT ON FUNCTION snap_line_to_network(GEOMETRY, DOUBLE PRECISION) IS '
Helper function for snapping the start and end point of a LineString to existing edges or vertices.

It first snaps the start and endpoint to the nearest point on the nearest edge, if it is within the snapping tolerance.
It then checks, whether the start/endpoint can further be snapped to a vertex, using the same snapping tolerance.
';

DROP FUNCTION IF EXISTS new_vertex(point GEOMETRY, snapping_tolerance DOUBLE PRECISION, snap BOOL);
CREATE OR REPLACE FUNCTION new_vertex(
    IN point GEOMETRY,
    IN snapping_tolerance DOUBLE PRECISION,
    IN snap BOOL DEFAULT FALSE,
    OUT new_id BIGINT,
    OUT new_geom GEOMETRY
) AS
$$
DECLARE
    _new_point        GEOMETRY;
    _intersected_edge RECORD;
    _split_fraction   DOUBLE PRECISION;
BEGIN
    SELECT point INTO _new_point;

    IF snap THEN
        SELECT snap_point_to_network(point, snapping_tolerance) INTO _new_point;
    END IF;

    INSERT INTO ${table}_vertices_pgr(geom, topo_added)
    VALUES (_new_point, TRUE)
    RETURNING
        id, geom
        INTO new_id, new_geom;

    -- can only be one edge, otherwise there would have been an existing vertex
    SELECT * INTO _intersected_edge FROM ${table} e WHERE st_dwithin(new_geom, e.geom, 0.00001);
    IF _intersected_edge.id IS NOT NULL THEN
        -- we know that there is an edge we might need to split
        -- we only split if it's not within snapping distance to its start or end point
        SELECT st_linelocatepoint(_intersected_edge.geom, new_geom) INTO _split_fraction;
        IF _split_fraction != ANY ('{0,1}'::DOUBLE PRECISION[]) THEN
            -- we split
            PERFORM create_from_existing_edge(
                    st_linesubstring(_intersected_edge.geom, 0, _split_fraction),
                    _intersected_edge.source,
                    new_id,
                    _split_fraction,
                    _intersected_edge.id::BIGINT
                );
            PERFORM create_from_existing_edge(
                    st_linesubstring(_intersected_edge.geom, _split_fraction, 1),
                    new_id,
                    _intersected_edge.target,
                    1 - _split_fraction,
                    _intersected_edge.id::BIGINT
                );
            UPDATE ${table} SET topo_removed = TRUE WHERE id = _intersected_edge.id;
            RETURN;
        ELSE
            -- we don't split
            RETURN;
        END IF;
    ELSE
        -- we do nothing, since that means it's our inserted line's start or end point
        RETURN;
    END IF;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION new_vertex(point GEOMETRY, snapping_tolerance DOUBLE PRECISION, snap BOOL )
    IS '
Helper function that inserts the passed point geometry as a new vertex and returns the new vertex record.
If snap is enabled, the vertex will be snapped to the network.
';

DROP FUNCTION IF EXISTS split_new_edge(new_edge GEOMETRY);
CREATE OR REPLACE FUNCTION split_new_edge(
    IN new_edge GEOMETRY
)
    RETURNS TABLE
            (
                START_FRAC DOUBLE PRECISION,
                END_FRAC   DOUBLE PRECISION,
                GEOM       GEOMETRY(LineString, 4326)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT DISTINCT ON (frac) frac                                                                    AS start_frac,
                                  LEAD(frac, 1, 1) OVER (ORDER BY frac)                                   AS end_frac,
                                  st_linesubstring(new_edge, frac, LEAD(frac, 1, 1) OVER (ORDER BY frac)) AS geom
        FROM (SELECT 0 AS frac
              UNION
              SELECT st_linelocatepoint(new_edge, st_closestpoint(new_edge, e.geom)) AS frac
              FROM ${table} e
              WHERE st_dwithin(new_edge, e.geom, 0.00001)
                AND topo_removed = FALSE) sq;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION split_new_edge(new_edge GEOMETRY) IS '
Helper function that takes a LineString geometry and splits it along each intersecting edge from the edge table.
Returns a table with columns (start fraction, end fraction, geometry), where each record is a substring and the fractions
are relative to the original LineString geometry.
';


DROP FUNCTION IF EXISTS create_from_existing_edge(new_geom GEOMETRY, new_source BIGINT, new_target BIGINT,
                                                  length_fraction DOUBLE PRECISION, old_id BIGINT);
CREATE OR REPLACE FUNCTION create_from_existing_edge(
    IN new_geom GEOMETRY,
    IN new_source BIGINT,
    IN new_target BIGINT,
    IN length_fraction DOUBLE PRECISION,
    IN old_id BIGINT
) RETURNS VOID AS
$$
DECLARE
BEGIN
    INSERT INTO ${table}(source, target, length, geom, topo_added)
    SELECT new_source,
           new_target,
           (length * length_fraction),
           new_geom,
           TRUE
    FROM ${table}
    WHERE id = old_id;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_from_existing_edge IS '
Helper function that inserts a new edge with all attributes from existing edge with old_id, and the new geometry.
In order to recalculate base costs, a length_fraction needs to be passed that specifies the length of the new edge
relative to the old edge.
';

DROP FUNCTION IF EXISTS add_to_topology(new_geom GEOMETRY, snap_recursively BOOL, tolerance DOUBLE PRECISION);
CREATE OR REPLACE FUNCTION add_to_topology(
    IN new_geom GEOMETRY,
    IN snap_at_intersections BOOL DEFAULT FALSE,
    IN tolerance DOUBLE PRECISION DEFAULT 0.00001,
    OUT new_ids BIGINT[]
) AS
$$
DECLARE
    _split_start_frac DOUBLE PRECISION;
    _split_end_frac   DOUBLE PRECISION;
    _split_geom       GEOMETRY(LineString, 4326);
    _start_id         BIGINT;
    _end_id           BIGINT;
    _start_geom       GEOMETRY;
    _end_geom         GEOMETRY;
    _last_end_id      BIGINT;
    _curr_startpoint  GEOMETRY;
    _curr_endpoint    GEOMETRY;
    _new_id           BIGINT;
    _final_geom       GEOMETRY(LineString, 4326);
BEGIN
    -- We start by splitting the passed LineString into chunks along its intersections with existing edges
    FOR _split_start_frac, _split_end_frac, _split_geom IN
        SELECT start_frac, end_frac, geom
        FROM split_new_edge(snap_line_to_network(new_geom, tolerance))
        WHERE st_geometrytype(geom) != 'ST_Point'
        LOOP
            SELECT st_startpoint(_split_geom), st_endpoint(_split_geom) INTO _curr_startpoint, _curr_endpoint;

            -- If the current line segment is the starting one, we create the start point.
            -- For the remaining segments, we can simply create the end point only,
            -- since the start point will have been created in the previous loop.
            IF (_split_start_frac = 1) AND (_split_end_frac = 1) THEN
                -- in this case we (unnecessarily) created a fraction 1 even though
                -- there is already an intersection at our end point. We exit the loop.
                EXIT;
            END IF;

            IF _split_start_frac = 0 THEN
                -- We get the closest vertex if it intersects with our starting point
                SELECT v.id,
                       v.geom
                INTO _start_id, _start_geom
                FROM ${table}_vertices_pgr v
                WHERE st_dwithin(_curr_startpoint, v.geom, 0.00001);

                IF _start_id IS NULL
                THEN
                    -- snapping at intersections does not matter here, our starting point has already been snapped
                    SELECT n.new_id, n.new_geom
                    INTO _start_id, _start_geom
                    FROM new_vertex(_curr_startpoint, tolerance, FALSE) n;
                END IF;
            ELSE
                -- in this case, we know that a vertex exists here (because we have created it in the previous loop)
                -- so we just get it from our end vertices array (is always the last one)
                SELECT id, geom INTO _start_id, _start_geom FROM ${table}_vertices_pgr v WHERE v.id = _last_end_id;
            END IF;

            -- Now, on to the end!
            -- here,  we need to decide whether we want to snap at every intersection (default is only at start/end)
            IF snap_at_intersections AND _split_end_frac != 1 THEN
                SELECT id, geom
                INTO _end_id, _end_geom
                FROM ${table}_vertices_pgr v
                WHERE st_dwithin(_curr_endpoint, v.geom, tolerance);
            ELSE
                -- otherwise we just get any intersecting vertex
                SELECT id, geom
                INTO _end_id, _end_geom
                FROM ${table}_vertices_pgr v
                WHERE st_dwithin(_curr_endpoint, v.geom, 0.00001);
            END IF;

            IF _end_id IS NULL
            THEN
                -- in this case, we either tried to snap our endpoint (with the snap_at_intersections option) and there was no
                -- candidate or we do not want to snap but tried to intersect with an existing vertex, which also failed.
                -- In either case, we create a new vertex
                SELECT n.new_id, n.new_geom
                INTO _end_id, _end_geom
                FROM new_vertex(
                             _curr_endpoint,
                             tolerance,
                             (SELECT CASE WHEN snap_at_intersections AND _split_end_frac != 1 THEN TRUE ELSE FALSE END)
                         ) n;

            END IF;
            -- now we have created all the vertices, and split existing edges along them (in the new_vertex() function)
            -- so all we need to do is INSERT the new edge(geom, source, target) and return the new edge IDs
            IF st_numpoints(_split_geom) < 3 THEN
                SELECT st_makeline(_start_geom, _end_geom) INTO _final_geom;
            ELSIF st_numpoints(_split_geom) = 3 THEN
                SELECT st_makeline(ARRAY [_start_geom, st_pointn(_split_geom, 2), _end_geom]) INTO _final_geom;
            ELSE
                SELECT st_addpoint(
                               st_addpoint(truncate_line(_split_geom), _start_geom, 0),
                               _end_geom)
                INTO _final_geom;
            END IF;
            INSERT INTO ${table}(geom, source, target, topo_added, length)
            VALUES (_final_geom, _start_id, _end_id, TRUE,
                    st_length(_final_geom))
            RETURNING id::BIGINT INTO _new_id;

            -- we preserve the id of the freshly inserted end vertex, so we can use it as a starting point
            -- in the next loop
            SELECT _end_id INTO _last_end_id;

            new_ids = ARRAY_APPEND(new_ids, _new_id);

        END LOOP;
    RETURN;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION add_to_topology(
    new_geom GEOMETRY, snap_recursively BOOL, tolerance DOUBLE PRECISION
    ) IS '
High level function that takes a LineString, snaps its start and end point to the network, detects any intersections
with existing edges and vertices, and inserts it either fully or split (depending on whether it crosses existing vertices
or edges) into the edge table, while simultaneously
  1. creating the respective vertices
  2. splitting any existing edges that cross the passed line at the intersection point.
';

-- Example
/*EXPLAIN ANALYZE WITH newGeom as (SELECT st_setsrid(
                                                             st_geomfromgeojson(
                                                                     '{ "type": "LineString", "coordinates": [ [20.2980120, 42.5339436], [20.2973104, 42.5325649] ] } '
                                                                 ), 4326)::geometry as the_geom)
                SELECT add_to_topology(the_geom, false, 0.00000001) FROM newGeom;
*/

-- clean up any created/split edges/vertices
CREATE OR REPLACE FUNCTION restore_topology() RETURNS VOID AS
$$
BEGIN
    DELETE FROM ${table} WHERE topo_added;
    DELETE FROM ${table}_vertices_pgr WHERE topo_added;
    UPDATE ${table} SET topo_removed = FALSE WHERE topo_removed;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION __test_topology_edit_trigger()
    RETURNS TRIGGER
AS
$$
BEGIN
    -- on insert, take the new geometry, restore the edge and vertex tables, then
    -- perform the add_to_topology function
    -- PERFORM restore_topology();
    PERFORM add_to_topology(new.geom, TRUE);
    RETURN NULL;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION __test_topology_update_trigger()
    RETURNS TRIGGER
AS
$$
DECLARE
    _new_edge_ids      BIGINT[];
    _vertices_to_check BIGINT[];
    _isolated_vertices BIGINT[];
BEGIN
    UPDATE ${table} SET topo_removed = TRUE WHERE id = new.id;

    SELECT add_to_topology(new.geom, TRUE) INTO _new_edge_ids;
    -- identify any nodes that are now left isolated and mark them as removed
    SELECT ARRAY [old.source, old.target]::BIGINT[] INTO _vertices_to_check;
    SELECT ARRAY_AGG(id)
    INTO _isolated_vertices
    FROM (SELECT UNNEST(_vertices_to_check) AS id
          EXCEPT
          SELECT id
          FROM (SELECT v.id
                FROM ${table}_vertices_pgr v,
                     (SELECT * FROM ${table} WHERE topo_removed = FALSE) e
                WHERE v.id = ANY (_vertices_to_check)
                  AND st_dwithin(e.geom, v.geom, 0.00001)) ssq) sq;
    UPDATE ${table}_vertices_pgr SET topo_removed = TRUE WHERE id = ANY (_isolated_vertices);
    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION __test_node_update_trigger() RETURNS TRIGGER AS
$$
DECLARE
    _ref_edges_source       INT[];
    _ref_edges_target       INT[];
    _edge_id                INT;
    _edge_geom              GEOMETRY;
    _final_geom             GEOMETRY;
    _merge_node             BIGINT[];
    _merge_geom             GEOMETRY;
    _mergeable              BOOLEAN DEFAULT FALSE;
    _this_ref_edges         INT[];
    _other_ref_edges        INT[];
    _this_connecting_nodes  BIGINT[];
    _other_connecting_nodes BIGINT[];
BEGIN
    -- Identify all referencing edges
    SELECT ARRAY_AGG(id) INTO _ref_edges_source FROM ${table} e WHERE source = old.id AND topo_removed = FALSE;
    SELECT ARRAY_AGG(id) INTO _ref_edges_target FROM ${table} e WHERE target = old.id AND topo_removed = FALSE;

    -- Check if we can merge with another node:
    -- this is the case if
    -- 1. the modified node is within a small tolerance distance to another node
    -- 2. the nodes' referencing edges are not overlapping
    -- 3. none of the referenced edges' sources or targets overlap

    SELECT ARRAY_AGG(id) INTO _merge_node FROM ${table}_vertices_pgr WHERE st_dwithin(geom, new.geom, 0.00001);

    IF ARRAY_LENGTH(_merge_node, 1) > 1 THEN
        RAISE EXCEPTION 'More than one possible merge node in snapping distance';
    ELSIF ARRAY_LENGTH(_merge_node, 1) = 1 THEN
        -- check 2.
        SELECT ARRAY_AGG(e.id)
        INTO _this_ref_edges
        FROM (SELECT * FROM ${table}_vertices_pgr WHERE id = old.id) v
                 JOIN ${table} e ON v.id = e.source OR v.id = e.target
        GROUP BY v.id;

        SELECT ARRAY_AGG(e.id)
        INTO _other_ref_edges
        FROM (SELECT * FROM ${table}_vertices_pgr WHERE id = _merge_node[1]) v
                 JOIN ${table} e ON v.id = e.source OR v.id = e.target
        GROUP BY v.id;

        SELECT (ARRAY_LENGTH(ARRAY
                                 (
                                     SELECT UNNEST(_this_ref_edges)
                                     INTERSECT
                                     SELECT UNNEST(_other_ref_edges)
                                 ), 1) IS NULL)::BOOL
        INTO _mergeable;
    END IF;

    IF _mergeable THEN
        -- check 3.

        -- get connecting nodes for this node and the merge candidate
        SELECT ARRAY_AGG(v.id)
        INTO _this_connecting_nodes
        FROM (SELECT * FROM ${table} e WHERE id = ANY (_this_ref_edges)) e
                 JOIN ${table}_vertices_pgr v ON e.source = v.id OR e.target = v.id
        WHERE v.id != old.id;
        SELECT ARRAY_AGG(v.id)
        INTO _other_connecting_nodes
        FROM (SELECT * FROM ${table} e WHERE id = ANY (_other_ref_edges)) e
                 JOIN ${table}_vertices_pgr v ON e.source = v.id OR e.target = v.id
        WHERE v.id != _merge_node[1];

        -- check intersection between the two arrays, if there is overlap we don't merge
        SELECT (ARRAY_LENGTH(ARRAY(
                                     SELECT UNNEST(_this_connecting_nodes)
                                     INTERSECT
                                     SELECT UNNEST(_other_connecting_nodes)
                                 ), 1) IS NULL)::BOOL
        INTO _mergeable;

    END IF;
    IF _mergeable THEN
        -- we merge when checks 1., 2. and 3. were successful

        -- instead of moving the edited node, we mark it as deleted and connect
        -- the referencing edges to the merge candidate node instead (remember to update the geom as well!)
        UPDATE ${table}_vertices_pgr SET topo_removed = TRUE WHERE id = old.id;
        SELECT geom INTO _merge_geom FROM ${table}_vertices_pgr WHERE id = _merge_node[1];
        -- update edges
        IF _ref_edges_source IS NOT NULL THEN
            FOREACH _edge_id IN ARRAY _ref_edges_source
                LOOP
                    SELECT geom INTO _edge_geom FROM ${table} WHERE id = _edge_id;
                    IF st_numpoints(_edge_geom) < 3 THEN
                        SELECT st_makeline(_merge_geom, st_endpoint(_edge_geom)) INTO _final_geom;
                    ELSIF st_numpoints(_edge_geom) = 3 THEN
                        SELECT st_makeline(ARRAY [_merge_geom, st_pointn(_edge_geom, 2), st_endpoint(_edge_geom)])
                        INTO _final_geom;
                    ELSE
                        SELECT st_addpoint(
                                       st_removepoint(_edge_geom, 0), _merge_geom, 0
                                   )
                        INTO _final_geom;
                    END IF;

                    UPDATE ${table} SET geom = _final_geom, source = _merge_node[1] WHERE id = _edge_id;
                END LOOP;
        END IF;
        IF _ref_edges_target IS NOT NULL THEN
            FOREACH _edge_id IN ARRAY _ref_edges_target
                LOOP
                    -- for each edge, find out if startpoint or endpoint has changed
                    SELECT geom INTO _edge_geom FROM ${table} WHERE id = _edge_id;
                    IF st_numpoints(_edge_geom) < 3 THEN
                        SELECT st_makeline(st_startpoint(_edge_geom), _merge_geom) INTO _final_geom;
                    ELSIF st_numpoints(_edge_geom) = 3 THEN
                        SELECT st_makeline(ARRAY [st_startpoint(_edge_geom), st_pointn(_edge_geom, 2), _merge_geom ])
                        INTO _final_geom;
                    ELSE
                        SELECT st_addpoint(
                                       st_removepoint(_edge_geom, st_npoints(_edge_geom) - 1), _merge_geom
                                   )
                        INTO _final_geom;
                    END IF;

                    UPDATE ${table} SET geom = _final_geom, target = _merge_node[1] WHERE id = _edge_id;
                END LOOP;
        END IF;
        -- we don't want the update to go through in this case because we've done all updates ourselves
        RETURN NULL;
    END IF;


    IF _ref_edges_source IS NOT NULL THEN

        FOREACH _edge_id IN ARRAY _ref_edges_source
            LOOP
                -- for each edge, find out if startpoint or endpoint has changed
                SELECT geom INTO _edge_geom FROM ${table} WHERE id = _edge_id;
                IF st_numpoints(_edge_geom) < 3 THEN
                    SELECT st_makeline(new.geom, st_endpoint(_edge_geom)) INTO _final_geom;
                ELSIF st_numpoints(_edge_geom) = 3 THEN
                    SELECT st_makeline(ARRAY [new.geom, st_pointn(_edge_geom, 2), st_endpoint(_edge_geom)])
                    INTO _final_geom;
                ELSE
                    SELECT st_addpoint(
                                   st_removepoint(_edge_geom, 0), new.geom, 0
                               )
                    INTO _final_geom;
                END IF;

                UPDATE ${table} SET geom = _final_geom WHERE id = _edge_id;
            END LOOP;
    END IF;
    IF _ref_edges_target IS NOT NULL THEN
        FOREACH _edge_id IN ARRAY _ref_edges_target
            LOOP
                -- for each edge, find out if startpoint or endpoint has changed
                SELECT geom INTO _edge_geom FROM ${table} WHERE id = _edge_id;
                IF st_numpoints(_edge_geom) < 3 THEN
                    SELECT st_makeline(st_startpoint(_edge_geom), new.geom) INTO _final_geom;
                ELSIF st_numpoints(_edge_geom) = 3 THEN
                    SELECT st_makeline(ARRAY [st_startpoint(_edge_geom), st_pointn(_edge_geom, 2), new.geom ])
                    INTO _final_geom;
                ELSE
                    SELECT st_addpoint(
                                   st_removepoint(_edge_geom, st_npoints(_edge_geom) - 1), new.geom
                               )
                    INTO _final_geom;
                END IF;

                UPDATE ${table} SET geom = _final_geom WHERE id = _edge_id;
            END LOOP;
    END IF;
    RETURN new;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_node() RETURNS TRIGGER AS
$$
DECLARE
    _ref_edge_ids INT[];
BEGIN
    -- don't actually delete
    UPDATE ${table}_vertices_pgr SET topo_removed = true WHERE id = OLD.id;
    -- we just need to get ids of referencing edges
    SELECT array_agg(id) INTO _ref_edge_ids FROM ${table} WHERE source = old.id OR target = old.id;
    UPDATE ${table} SET topo_removed = TRUE WHERE id = ANY (_ref_edge_ids);
    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER tr
    BEFORE INSERT
    ON ${table}
    FOR EACH ROW
    WHEN (PG_TRIGGER_DEPTH() = 0)
EXECUTE PROCEDURE __test_topology_edit_trigger();

DROP TRIGGER IF EXISTS tr_update ON ${table};
CREATE OR REPLACE TRIGGER tr_update
    BEFORE UPDATE
    ON ${table}
    FOR EACH ROW
    WHEN (PG_TRIGGER_DEPTH() = 0)
EXECUTE PROCEDURE __test_topology_update_trigger();

CREATE OR REPLACE TRIGGER tr_update_node
    BEFORE UPDATE
    ON ${table}_vertices_pgr
    FOR EACH ROW
    WHEN (PG_TRIGGER_DEPTH() = 0)
EXECUTE PROCEDURE __test_node_update_trigger();

DROP TRIGGER IF EXISTS tr_delete_node ON ${table}_vertices_pgr;
CREATE OR REPLACE TRIGGER tr_delete_node
    BEFORE DELETE
    ON ${table}_vertices_pgr
    FOR EACH ROW
    WHEN (PG_TRIGGER_DEPTH() = 0)
EXECUTE PROCEDURE delete_node();
