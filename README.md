# postcode

This is a compilation of the scripts for processing and inclusion of variables associated with areas into a dataset.

## How it works

### Census

The data from the census is collected every 10 years. The last census was is 2011.

The census areas are organized as follows (one area is inside the next):
- You have a postcode that is the know delimiter for some houses
- Output Area (oa) is the minimal output from census, it contains >= 40 households (target 125) and >= 100 persons
- Super Output Area are output areas that have multiple things in common
- Lower Layer Super Output Area (LSOA) contains 4~6 OA and ~1.5k persons
- Middle Layer Super Output Area (MSOA) ~7.2k persons
- Local Area District (LAD): Birmingham, Sandwell, Solihull, Bromsgrove, Dudley, Lichfield, etc... (Check: https://en.wikipedia.org/wiki/Districts_of_England)

### The database

The dataset is separated into different schemas:
- public: the main tables extracted which map directly to an area code - here is also a table that maps the different area codes
- census: key information from the census - these variables also map directly to an area code
- raw: information that may require some filtering for use
- compiled: raw tables that have some sort of grouping from the raw tables

Multiple variables that map an area code to key statistics were downloaded.

### Some sources for data

- Census 2011 Key statistics: http://www.nomisweb.co.uk/census/2011/key_statistics
- Crimes: https://data.police.uk
- Income: https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/smallareaincomeestimatesformiddlelayersuperoutputareasenglandandwales
- Postcode lookup: http://geoportal.statistics.gov.uk/items/postcode-to-output-area-hierarchy-with-classifications-august-2018-lookup-in-the-uk

Most of official data are under Open Government License 3.0 (https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)

A copy of the data utilised is available in the _data/_ folder (please note that the police data was extracted only for West Midlands).

## Installation

### Docker image:

docker run --name postcode_mapper_postgresdb --restart unless-stopped -p 5432:5432 postgres:11.2 -d postgres

This will create a container with name postcode_mapper_postgresdb, which will restart automatically and map the port 5432 to the host.

### Create database and schemas:

docker exec -it postcode_mapper_postgresdb

psql

CREATE DATABASE postcode;

USE postcode;

CREATE SCHEMA compiled;

### Python scripts

source venv/bin/activate

pip install psycopg2 pandas numpy sqlalchemy

### Getting it ready

Extract the _data/_ files and move them to the main folder.

Execute the _20190502_postcode_sql.py_ script.

## Other

Some Postgresql operations were done using DBeaver.
