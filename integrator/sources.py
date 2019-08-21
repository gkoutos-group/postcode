
from integrator.tables import DBTable, DBCategory
from integrator.util import ObtainDataError


class Income(DBTable):
    """
    Income table from postcode information.

    @param engine: an sqlalchemy engine
    """
    def __init__(self, engine):
        super().__init__('msoa', query="""
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(msoa)
                         ), condition as (
                            select *
                            from compiled.income
                         )
                         select condition.* from filtering_part left join condition on filtering_part.msoa = condition.msoa
                         """, engine=engine)


class IndexMultipleDeprivation(DBTable):
    """
    Index of multiple deprivation. It shows different scores for difference aspects of a region. It is mapped with lsoa.

    @param engine: an sqlalchemy engine
    @param mode: 'everything' - all the scores; 'only_scores' - only the main IMD score
    """
    def __init__(self, engine, mode='everything'):
        modes = ['everything', 'only_scores']
        if mode == 'everything':
            query = """
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(lsoa)
                         ), condition as (
                            select *
                            from public.indexmultipledeprivation
                         )
                         select condition.* from filtering_part left join condition on filtering_part.lsoa = condition.lsoa"""
        elif mode == 'only_scores':
            query = """
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(lsoa)
                         ), condition as (
                            select lsoa, "IOMDIS" as IMD
                            from public.indexmultipledeprivation
                         )
                         select condition.* from filtering_part left join condition on filtering_part.mapping = condition.lsoa"""
        else:
            raise ObtainDataError('Invalid mode for "{}", please select one of: "{}"'.format(self.__class__.__name__, '", "'.join(modes)))
        super().__init__('lsoa', query=query, engine=engine)


class CrimesOutcome(DBTable):
    """
    Outcomes of crimes associated with lsoa.

    @param engine: and sqlalchemy engine
    """
    def __init__(self, engine):
        super().__init__('lsoa', query="""
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(lsoa)
                         ), condition as (
                            select *
                            from compiled.crimes_outcomes_yearly
                         )
                         select condition.* from filtering_part left join condition on filtering_part.lsoa = condition.lsoa
                         """, engine=engine)


class CrimesStreet(DBTable):
    """
    Crimes in streets associated with lsoa.

    @param engine: and sqlalchemy engine
    """
    def __init__(self, engine):
        super().__init__('lsoa', query="""
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(lsoa)
                         ), condition as (
                            select *
                            from compiled.crimes_street_type_yearly
                         )
                         select condition.* from filtering_part left join condition on filtering_part.lsoa = condition.lsoa
                         """, engine=engine)



class Crime(DBCategory):
    """
    Grouped variables for crime.
    """
    def get_tables(engine):
        """
        get_tables yields CrimesOutcome and CrimesStreet
        @param engine: an sqlalchemy engine
        """
        yield CrimesOutcome(engine)
        yield CrimesStreet(engine)


class Census11(DBCategory):
    """
    Returns all the data extractor for census variables.
    """
    query_format = """with filtering_part as (
                            select *
                            from (values {references}) tempT(oa)
                         ), condition as (
                            select *
                            from census2011.{table}
                         )
                         select condition.* from filtering_part left join condition on filtering_part.oa = condition.oa"""
    options = ['adults_not_employment_etc', 'age_structure', 'car_etc', 'census_industry', 'communal_etc', 'country_birth', 'dwellings_etc', 'economic_etc', 'ethnic_group', 'health_unpaid_care', 'hours_worked', 'household_composition', 'household_language', 'living_arrangements', 'lone_parents_household_etc', 'marital_and_civil_partnership_status', 'national_identity', 'nssec_etc', 'occupation_sex', 'passports_held', 'qualifications_students', 'religion', 'rooms_etc', 'tenure', 'usual_resident_population']
    def get_tables(engine):
        """
        get_tables yields quite a few options enumerated in Census11.options

        @param engine: an sqlalchemy engine
        """
        for t in Census11.options:
            yield DBTable('oa', Census11.query_format.replace('{table}', t), engine=engine, name='Census11_' + t)
 
