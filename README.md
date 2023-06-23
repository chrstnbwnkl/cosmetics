# Cosmetics — edit OSM street topologies in PostGIS

A template for loading a noded OSM street network data into PostGIS — and start editing right away

To learn more about the editing capabilities, please check out my 2023 FOSS4G presentation about editing pgRouting topologies.

## Getting started

To start editing your OSM extract of choice, you need to follow these steps:

  1. Clone this repository: `git clone <>`
  2. Make sure you have [osm4routing]() installed. For this, install the `rust` programming language and then install using `cargo install osm4routing`
  3. Have an OSM pbf file ready (get yours [here](http://download.geofabrik.de)!)
  4. Run the `setup` script from the project root directory pointing to your OSM file and giving the tables a name: `./setup /path/to/OSM.pbf berlin`

This will create the table creation, import and editing scripts. Now you have two options:
  1. create a PostGIS Docker container using the `docker-compose.yml` (run `docker compose up -d`), the scripts will be loaded automatically
  2. Load the scripts into your own PostGIS instance using `psql`

After successfully loading the scripts into PostGIS you can connect QGIS to that data base and start editing!

## Feedback & Contributing

At the time of initial development this is nothing but a proof of concept, so issues and PR's are welcome! 

