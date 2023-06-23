ALTER TABLE ${table}_vertices_pgr
    ADD COLUMN geom Geometry(Point, 4326);

UPDATE ${table}_vertices_pgr
    SET geom = ST_MakePoint(longitude, latitude);

ALTER TABLE ${table}
    ADD COLUMN geom Geometry(LineString, 4326);

UPDATE ${table}
    SET geom = st_geomfromewkt(wkt);

ALTER TABLE ${table}_vertices_pgr DROP COLUMN latitude;
ALTER TABLE ${table}_vertices_pgr DROP COLUMN longitude;

ALTER TABLE ${table} DROP COLUMN wkt;
ALTER TABLE ${table} DROP COLUMN id;
ALTER TABLE ${table} ADD COLUMN id INT GENERATED ALWAYS AS IDENTITY;

CREATE INDEX ${table}_edge_id ON ${table}(id);
CREATE INDEX ${table}_edge_gist ON ${table} USING gist(geom);

CREATE INDEX ${table}_node_id ON ${table}_vertices_pgr(id);
CREATE INDEX ${table}_node_gist ON ${table}_vertices_pgr USING gist(geom);

-- Add column for keeping track of changes
ALTER TABLE ${table} ADD COLUMN topo_added BOOL DEFAULT FALSE;
ALTER TABLE ${table} ADD COLUMN topo_removed BOOL DEFAULT FALSE;

ALTER TABLE ${table}_vertices_pgr ADD COLUMN topo_added BOOL DEFAULT FALSE;
ALTER TABLE ${table}_vertices_pgr ADD COLUMN topo_removed BOOL DEFAULT FALSE;

