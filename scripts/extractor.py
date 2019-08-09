

from sqlalchemy import create_engine
import pandas as pd
import numpy as np


class ObtainDataError(Exception):
    pass


class DBTable:
    def __init__(self, reference, query, engine=None, rename=True, name=None):
        self.reference = reference
        self.engine = engine
        self.query = query
        self._number_cols = 0
        self.rename = rename
        if name == None:
            name = self.__class__.__name__
        self.name = name
        
    
    def format_for_query(self, values):
        values = [str(i) for i in set(values)]
        return self.query.format(references="('" + "'), ('".join(values) + "')", references_l="('" + "', '".join(values) + "')")
    
    def obtain_data(self, mapping):
        """
        Obtain data for a set of mapping values
        """
        if self.engine is None:
            raise ObtainDataError('Engine is not defined')
        sql=self.format_for_query(mapping)
        sql_ret = pd.read_sql_query(sql, con=self.engine)
        if self._number_cols is 0:
            self._number_cols = len(sql_ret.columns.values) - 1
        elif self._number_cols != len(sql_ret.columns.values) - 1:
            raise ObtainDataError('Invalid number of columns returned. Deactivate {}'.format(self.__class__.__name__))
        if len(sql_ret) > len(mapping):
            raise ObtainDataError('Invalid number of rows returned. We have more rows returned ({}) than requested ({}). Deactivate {}'.format(len(sql_ret), len(mapping), self.__class__.__name__))
        sql_ret.drop(columns=[self.reference], inplace=True)
        sql_ret.rename(columns={'mapping': self.reference}, inplace=True)
        if self.rename:
            sql_ret.rename(columns=lambda x: self.name + '.' + x if x != self.reference else x, inplace=True)
        return sql_ret


class Income(DBTable):
    def __init__(self, engine):
        super().__init__('msoa', query="""
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(mapping)
                         )
                         select * from filtering_part left join compiled.income on filtering_part.mapping = compiled.income.msoa
                         """, engine=engine)


class CrimesOutcome(DBTable):
    def __init__(self, engine):
        super().__init__('lsoa', query="""
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(mapping)
                         )
                         select * from filtering_part left join compiled.crimes_outcomes_yearly on filtering_part.mapping = compiled.crimes_outcomes_yearly.lsoa
                         """, engine=engine)


class IndexMultipleDeprivation(DBTable):
    def __init__(self, engine, mode='everything'):
        modes = ['everything', 'only_scores']
        if mode == 'everything':
            query = """
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(mapping)
                         )
                         select * from filtering_part left join public.indexmultipledeprivation on filtering_part.mapping = public.indexmultipledeprivation.lsoa"""
        elif mode == 'only_scores':
            query = """
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(mapping)
                         )
                         select * from filtering_part left join (select lsoa, "IOMDIS" as IMD from public.indexmultipledeprivation) as imdt on filtering_part.mapping = imdt.lsoa"""
        else:
            raise ObtainDataError('Invalid mode for "{}", please select one of: "{}"'.format(self.__class__.__name__, '", "'.join(modes)))
        super().__init__('lsoa', query=query, engine=engine)


class CrimesStreet(DBTable):
    def __init__(self, engine):
        super().__init__('lsoa', query="""
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(mapping)
                         )
                         select * from filtering_part left join compiled.crimes_street_type_yearly on filtering_part.mapping = compiled.crimes_street_type_yearly.lsoa
                         """, engine=engine)


class DBCategory:
    def get_tables(engine=None):
        yield
        

class Crime(DBCategory):
    def get_tables(engine):
        yield CrimesOutcome(engine)
        yield CrimesStreet(engine)


class Census11(DBCategory):
    query_format = """with filtering_part as (
                            select *
                            from (values {references}) tempT(mapping)
                         )
                         select * from filtering_part left join census2011.{table} on filtering_part.mapping = census2011.{table}.oa"""
    options = ['adults_not_employment_etc', 'age_structure', 'car_etc', 'census_industry', 'communal_etc', 'country_birth', 'dwellings_etc', 'economic_etc', 'ethnic_group', 'health_unpaid_care', 'hours_worked', 'household_composition', 'household_language', 'living_arrangements', 'lone_parents_household_etc', 'marital_and_civil_partnership_status', 'national_identity', 'nssec_etc', 'occupation_sex', 'passports_held', 'qualifications_students', 'religion', 'rooms_etc', 'tenure', 'usual_resident_population']
    def get_tables(engine):
        for t in Census11.options:
            yield DBTable('oa', Census11.query_format.replace('{table}', t), engine=engine, name='Census11_' + t)
    


class PostcodeMapping(DBTable):
    """
    This contains the functions for the different areas of mapping
    """
    AVAILABLE = ['pc', 'oa', 'lsoa', 'msoa', 'lad']
    def matching_source(what_we_have, what_is_needed):
        matching_we_have = list(set(what_we_have).intersection(set(PostcodeMapping.AVAILABLE)))
        indexes_we_have = [PostcodeMapping.AVAILABLE.index(i) for i in matching_we_have]
        what_is_needed = list(what_is_needed)
        index_is_needed = [PostcodeMapping.AVAILABLE.index(i) for i in what_is_needed]
        if min(indexes_we_have) < min(index_is_needed):
            return matching_we_have[indexes_we_have.index(min(indexes_we_have))]
        raise ObtainDataError('Not possible to find any/all the columns "{}". We were looking with these matching columns "{}" (out of "{}").'.format('", "'.join(what_is_needed), '", "'.join(matching_we_have), '", "'.join(what_we_have)))
    
    def __init__(self, from_variable, to_variables, engine, rename=False):
        # first check for invalid column specification
        if type(from_variable) is not str or from_variable not in PostcodeMapping.AVAILABLE:
            raise ObtainDataError('"{}" is not a str or not one of "{}".'.format(str(from_variable), '", "'.join(PostcodeMapping.AVAILABLE)))
        if type(to_variables) is str:
            to_variables = [to_variables]
        if type(to_variables) is not list or any([type(i) is not str for i in to_variables]):
            raise ObtainDataError('to_variables must be str or a list of str')
        # check for not available columns
        not_in_list = [i for i in to_variables if i not in PostcodeMapping.AVAILABLE]
        if len(not_in_list) > 0:
            raise ObtainDataError('Some variables ("{}") we have no information, expected one of "{}".'.format('", "'.join(not_in_list), '", "'.join(PostcodeMapping.AVAILABLE)))
        # check for invalid scale: always go from smaller to bigger
        org = PostcodeMapping.AVAILABLE.index(from_variable)
        if any([PostcodeMapping.AVAILABLE.index(i) <= org for i in to_variables]):
            raise ObtainDataError('Trying to get a variable ("{}") that is more specific or in the same level as the query one ("{}").'.format('", "'.format(to_variables), from_variable))
        super().__init__(from_variable, query="""
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(mapping)
                         ),
                         new_variables as (
                            select distinct "{from_variable}", "{to_variables}" from public.postcode_lookup11 where "{from_variable}" in {references_l}
                         )
                         select * from filtering_part left join new_variables on filtering_part.mapping = new_variables.{from_variable}
                         """.replace('{from_variable}', from_variable).replace('{to_variables}', '","'.join(to_variables)), engine=engine, rename=False)


class DataCollector:
    """
    Main class for data collection, this class will handle all the others
    """
    def __init__(self, database_file_handler, sources=None, reference_check_engine=None):
        self.database_file_handler = database_file_handler
        self.sources = sources
        self.checked = False
        self.reference_check_engine = reference_check_engine
        
    def reference_check(self, columns):
        missing_references = list(set([i.reference for i in self.sources]) - set(columns))
        if len(missing_references) > 0: #if there are some reference columns which we dont have
            match = PostcodeMapping.matching_source(columns, missing_references) #let's try to find them
            if match: #if we can match add to the database operations
                self.sources.insert(0, PostcodeMapping(match, missing_references, self.reference_check_engine))
            else: #we have something missing
                raise ObtainDataError('There are missing references! - "{}"'.format('", "'.join(missing_references)))
        
    def collect(self):
        # work in chunks
        # first add the new columns for a database
        # add the values
        for chunk in self.database_file_handler():
            if not self.checked:
                self.reference_check(chunk.columns.values)
                self.checked = True
            for d in self.sources:
                ndf = d.obtain_data(chunk[d.reference])
                ndf[d.reference] = ndf[d.reference].astype('str')
                chunk = chunk.merge(ndf, on=d.reference, how='left')
            yield chunk


