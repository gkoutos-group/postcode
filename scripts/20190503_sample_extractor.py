

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
    def __init__(self, database_file_handler, sources=None):
        self.database_file_handler = database_file_handler
        self.sources = sources
        self.checked = False
        
    def reference_check(self, columns):
        missing_references = list(set([i.reference for i in self.sources]) - set(columns))
        if len(missing_references) > 0: #if there are some reference columns which we dont have
            match = PostcodeMapping.matching_source(columns, missing_references) #let's try to find them
            if match: #if we can match add to the database operations
                self.sources.insert(0, PostcodeMapping(match, missing_references, engine))
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


engine = create_engine('postgresql://postgres@localhost:5432/postcode')

test_list = ['huehuehue', 'E02004295', 'E02004320', 'E02004321', 'E02004322', 'E02004296', 'E02004306', 'E02004323', 'E02004308', 'E02004309', 'E02004307', 'E02004324', 'E02004310', 'E02004311', 'E02004311', 'huehuehue']

test_list = ['huehuehue', 'E00070659', 'E00070660', 'E00070943', 'E00070949', 'E00070950', 'E00070951', 'E00070955', 'E00070956', 'E00070944', 'E00070945', 'E00070946', 'E00070947', 'E00070948', 'E00070952', 'E00070953', 'E00070954', 'E00071184', 'E00071185', 'E00071190', 'E00071191', 'E00071194', 'E00071194', 'huehuehue']

test_list = ['B152TT']

e = Income(engine)
a = e.obtain_data(test_list)
#print(a)

j = PostcodeMapping('oa', 'msoa', engine)
j.obtain_data(test_list)


df = {'pc': test_list, 'kek': [i for i in range(len(test_list))]}
df = pd.DataFrame(data=df)

def db_get():
    for i, j in df.groupby(np.arange(len(df))//4096):
        yield j

def db_get2(FILE):
    return pd.read_csv(FILE, chunksize=4096)

d = DataCollector(db_get, sources=[Income(engine)] + [i for i in Crime.get_tables(engine)] + [i for i in Census11.get_tables(engine)])
e = pd.concat([i for i in d.collect()]) #only use concat if the data can fit the memory
print(e.head())
print(e.columns.values)
print(e['pc'])



"""
-> identify the current mapping columns
-> compute dependency graph from requested tables

-> extract frmo db:
    -> other mappings
    -> by table, all variables



oa | colA | colB | colC | colD

oa | colA | colB | colC | colD | msoa

oa | colA | colB | colC | colD | lsoa
"""

