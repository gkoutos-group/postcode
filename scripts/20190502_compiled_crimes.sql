
create table compiled.crimes_outcome_yearly
as (
	SELECT "LSOA_code" as lsoa, cast(EXTRACT(year from "month") as INT) as year, outcome, count(index)
	FROM raw.crimes_outcomes
	--where "month" between '2018-01-01' and '2018-12-31' and
	group by "LSOA_code", outcome, EXTRACT(year from "month")
);


create table compiled.crimes_street_crime_type_outcome_yearly
as (
	SELECT "LSOA_code" as lsoa, cast(EXTRACT(year from "month") as INT) as year, crime_type, outcome, count(index)
	FROM raw.crimes_street
	--where "month" between '2018-01-01' and '2018-12-31' and
	group by "LSOA_code", crime_type, outcome, EXTRACT(year from "month")
);


create table compiled.crimes_street_crime_type_yearly
as (
	SELECT "LSOA_code" as lsoa, cast(EXTRACT(year from "month") as INT) as year, crime_type, count(index)
	FROM raw.crimes_street
	--where "month" between '2018-01-01' and '2018-12-31' and
	group by "LSOA_code", crime_type, EXTRACT(year from "month")
);

