# postcode tools & Integrator

[![DOI](https://zenodo.org/badge/185373533.svg)](https://zenodo.org/badge/latestdoi/185373533)

Author: Victor Roth Cardoso - V.RothCardoso@bham.ac.uk

E-mail me suggestions, comments and issues.

This is a compilation of the scripts for processing and inclusion of variables associated with areas into a dataset.

## Documentation

Documentation can be accessed using pydoc: `python3 -m pydoc`

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
- Index of Multiple Deprivation: https://www.gov.uk/government/statistics/english-indices-of-deprivation-2015

Most of official data are under Open Government License 3.0 (https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)

A copy of the data utilised is available in the _data/_ folder (please note that the police data was extracted only for West Midlands).

## Installation

### Docker image:

`docker run --name postcode_mapper_postgresdb --restart unless-stopped -p 5432:5432 postgres:11.2 -d postgres`

This will create a container with name postcode_mapper_postgresdb, which will restart automatically and map the port 5432 to the host.

### Create database and schemas:

`docker exec -it postcode_mapper_postgresdb`

`psql`

```sql
CREATE DATABASE postcode;
\c postcode;
CREATE SCHEMA compiled;
CREATE SCHEMA raw;
CREATE SCHEMA census2011;
```

### Python scripts

`source venv/bin/activate`

`pip install psycopg2 pandas numpy sqlalchemy`

### Getting it ready

Extract the _data/_ files and move them to the main folder.

Execute the _20190502_postcode_sql.py_ script.

## Creating more extractors

This will look a bit daunting at first, but for any table with format <identifier, variables, date, value>, a generic constructor for time-releated events can be created:

```python
class DBTableVariable(DBTableTimed):
    def __init__(self, table, table_date_variable, variables, engine, rename=True, name=None, mode=None, begin_date=None, end_date=None, ref_date=None, shift=None, first_presence=None):
        query = """
            WITH filtering_part AS (
                SELECT *
                FROM (values {references}) tempT({referencevars})
            ), preselect AS (
                SELECT presence.identifier, {OPERATION}(presence.{DATEVARIABLE}) AS CONDITION_DATE {AS_TERM}
                FROM filtering_part
                LEFT JOIN (
                    SELECT identifier, {DATEVARIABLE}
                    FROM {TABLE}
                ) AS presence ON filtering_part.identifier = presence.identifier
                {WHERE}
                GROUP BY presence.identifier {GROUPBY_TERM}
            ), condition as (
                SELECT DISTINCT preselect.identifier, {OPERATION}(preselect.CONDITION_DATE) AS CONDITION_DATE, count(*) as AMT_MEASURES, {VARIABLELISTAS} {INTERNAL_AS_TERM}
                FROM preselect
                INNER JOIN {TABLE} AS ot ON preselect.identifier = ot.identifier AND preselect.CONDITION_DATE = ot.{DATEVARIABLE}
                GROUP BY preselect.identifier {INTERNAL_GROUPBY_TERM}
            )
            SELECT *
            FROM condition
            WHERE condition.identifier IS NOT NULL
        """
        query = query.replace('{TABLE}', table).replace('{VARIABLELISTAS}', ', '.join(['avg(convert(numeric, "' + i + '")) as "avg_' + i + '"' for i in variables]))
        super().__init__('identifier', query, engine, rename, name, mode, begin_date, end_date, ref_date, shift, first_presence, table_date_variable)

    def _obtain_data(self, mapping):
        AS_TERM = ''
        GROUPBY_TERM = ''
        JOIN_PART = ''
        for we_have, in_dataset in zip(self.reference[1:], self.inputvars[1:]): #this is going to be added in the internal bit
            AS_TERM += ', CONVERT(DATE, {DATASET_NAME}) as {DATASET_NAME}'.format(GIVEN_NAME=in_dataset, DATASET_NAME=we_have)
            GROUPBY_TERM += ', preselect.{DATASET_NAME}'.format(GIVEN_NAME=in_dataset, DATASET_NAME=we_have)
            JOIN_PART += ' AND filtering_part.{GIVEN_NAME} = condition.{DATASET_NAME}'.format(GIVEN_NAME=in_dataset, DATASET_NAME=we_have)
        original = self.query
        self.query = self.query.replace('{INTERNAL_AS_TERM}', AS_TERM).replace('{INTERNAL_GROUPBY_TERM}', GROUPBY_TERM).replace('{JOIN_PART}', JOIN_PART)
        sql_ret = super()._obtain_data(mapping)
        self.query = original
        return sql_ret
```

## Improving it

There are some things missing in this implementation:

- Tests: both for function and returned data.
- `DBTableTimed`: simplify/merge the behaviour for multiple columns with `DBTable`; explicit information about datetime columns.
- Example data for `DBTableTimed` behaviour.
