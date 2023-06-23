# Cosmetics — edit OSM street topologies in PostGIS

A template for loading a noded OSM street network data into PostGIS — and start editing right away

To learn more about the editing capabilities, please check out my 2023 FOSS4G presentation about editing pgRouting topologies.

## Getting started

To start editing your OSM extract of choice, you need to follow these steps:

  1. Clone this repository: `git clone git@github.com:chrstnbwnkl/cosmetics.git`
  2. Make sure you have [osm4routing](https://github.com/Tristramg/osm4routing) installed. For this, install the `rust` programming language and then install using `cargo install osm4routing`
  3. Have an OSM pbf file ready (get yours [here](http://download.geofabrik.de)!)
  4. Run the `setup` script from the project root directory pointing to your OSM file and giving the tables a name: `./setup /path/to/OSM.pbf berlin`

This will create the table creation, import and editing scripts. Now you have two options:
  1. create a PostGIS Docker container using the `docker-compose.yml` (run `docker compose up -d`), the scripts will be loaded automatically
  2. use the created `import.sh` script to load the scripts into any PostgreSQL instance, using the same connection arguments as with `plsql`:
        `./import.sh -h <host> -p <port> -d <db> -U <user>`

After successfully loading the scripts into PostGIS you can connect QGIS to that data base and start editing!

> Note: when editing in QGIS, make sure to set the filter `"topo_removed" = false` for both the edge and node table, so that removed elements are not shown in QGIS. 

## Feedback & Contributing

At the time of initial development this is nothing but a proof of concept, so issues and PR's are welcome! 

