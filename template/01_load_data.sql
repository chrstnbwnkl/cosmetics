COPY ${table}_vertices_pgr FROM '/app/data/nodes.csv' CSV DELIMITER ',' HEADER;
COPY ${table} FROM '/app/data/edges.csv' CSV DELIMITER ',' HEADER;

