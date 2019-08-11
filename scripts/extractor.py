

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import time


class ObtainDataError(Exception):
    pass


class DBTable:
    def __init__(self, reference, query, engine=None, rename=True, name=None):
        self.reference = reference
        self.engine = engine
        self.query = query
        self._number_cols = 0
        self._columns = []
        self.rename = rename
        if name == None:
            name = self.__class__.__name__
        self.name = name

    def __str__(self):
        return '{}: {}'.format(self.name, self.reference)

    def __repr__(self):
        return '<' + self.__str__() + ' @ ' + str(hex(id(self))) + '>'
        
    def _format_for_query(self, values):
        values = [str(i) for i in set(values)]
        return  "('" + "'), ('".join(values) + "')"

    def _obtain_pre_checks(self, mapping):
        if self.engine is None:
            raise ObtainDataError('Engine is not defined')

    def _obtain_post_checks(self, mapping, sql_ret):
        if self._number_cols is 0: # if not data was extracted before let's define the number of columns and the order
            self._number_cols = len(sql_ret.columns.values) - 1
            self._columns = sql_ret.columns.values
        elif self._number_cols != len(sql_ret.columns.values) - 1: #if there was a query before and the number of columns now is different it means there is something wrong
            raise ObtainDataError('Invalid number of columns returned. Deactivate {}'.format(self.__class__.__name__))
        elif sum([i != j for i, j in zip(self._columns, sql_ret.columns.values)]) > 0: #check the columns names
            raise ObtainDataError('The columns returned differ between iteractions. Deactivate {}'.format(self.__class__.__name__))
        if len(sql_ret) > len(mapping): #check number of rows
            raise ObtainDataError('Invalid number of rows returned. We have more rows returned ({}) than requested ({}). Deactivate {}'.format(len(sql_ret), len(mapping), self.__class__.__name__))

    def _obtain_data(self, mapping):
        sql = self.query.format(references=self._format_for_query(mapping), references_l=self._format_for_query(mapping))
        return pd.read_sql_query(sql, con=self.engine)
    
    def obtain_data(self, mapping):
        """
        Obtain data for a set of mapping values
        """
        self._obtain_pre_checks(mapping) #pre-checks related to input or current state
        sql_ret = self._obtain_data(mapping)
        self._obtain_post_checks(mapping, sql_ret) #post-checks related to the output
        if self.rename: # add names associated with this class
            sql_ret.rename(columns=lambda x: self.name + '.' + x if x != self.reference and type(self.reference) is list and x not in self.reference else x, inplace=True)
        return sql_ret


class DBTableTimed(DBTable):
    # the modes contain the possible variables: begin_date, end_date, ref_date, delay
    _VALID_MODES = {None: [False, False, False, False],
                   'between': [True, True, False, None],
                   'outside': [True, True, False, None],
                   'before': [False, False, True, None],
                   'after': [False, False, True, None]
                   }

    def __init__(self, reference, query, engine=None, rename=True, name=None, mode=None, begin_date=None, end_date=None, ref_date=None, delay=None, first_presence=None, table_date_variable=None):
        super().__init__(reference, query, engine, rename, name)
        self.mode = mode
        self.begin_date = begin_date
        self.end_date = end_date
        self.ref_date = ref_date
        self.delay = str(int(delay)) if delay else None
        self.reference = [reference]
        self.inputvars = [reference]
        self.first_presence = first_presence
        self.table_date_variable = table_date_variable
        self._check_settings()

    def _check_settings(self):
        def _check_columns(begin_date, end_date, ref_date, delay, mode_setup):
            """
            Check for wrong variables passage given the mode
            """
            missing_columns = list()
            extra_columns = list()
            for st, nd in zip([('begin_date', begin_date), ('end_date', end_date), ('ref_date', ref_date), ('delay', delay)], mode_setup):
                i, j = st
                if nd is False and j is None:
                    continue
                elif nd is False and j is not None:
                    extra_columns.append('{}": "{}'.format(i, j))
                elif nd is True and j is None:
                    missing_columns.append('{}": "{}'.format(i, j))
                elif nd is True and j is not None:
                    self.reference.append(j)
                    self.inputvars.append(i)
                elif nd is None and j is not None:
                    self.reference.append(j)
                    self.inputvars.append(i)
            if len(extra_columns) > 0:
                EXTRA_COLUMNS = '"' + '", "'.join(extra_columns) + '"'
            else:
                EXTRA_COLUMNS = ''
            if len(missing_columns) > 0:
                raise ObtainDataError('Missing values for "{}": "{}"{EXTRA_COLUMNS}'.replace('{EXTRA_COLUMNS}', EXTRA_COLUMNS).format(self.__class__.__name__, '", "'.join(missing_columns)))
            elif len(extra_columns) > 0:
                raise ObtainDataError('Extra columns for "{}". Extra columns {} were ignored.'.format(self.__class__.__name__, EXTRA_COLUMNS))

        if self.mode in self._VALID_MODES:
            _check_columns(self.begin_date, self.end_date, self.ref_date, self.delay, self._VALID_MODES[self.mode])
        else:
            raise ObtainDataError('Mode invalid for "{}". Expected "{}"'.format(self.__class__.__name__, '", "'.join([str(i) for i in self._VALID_MODES[self.mode]])))


    def _format_for_query_multiple(self, values):
        if isinstance(values, pd.core.frame.DataFrame):
            return ', '.join([ "('" + "', '".join([str(i) if not isinstance(i, pd.Series.dt) else i.strftime("'%Y-%m-%d'") for i in els ]) + "')" for els in zip(*[values[r] for r in self.reference])])
        elif isinstance(values, pd.core.series.Series):
            values = [str(i) for i in set(values)]
            return  "('" + "'), ('".join(values) + "')"

    def _obtain_data(self, mapping):
        references = self._format_for_query_multiple(mapping)
        referencevars = ','.join(self.inputvars)
        ## the rules
        if self.mode is None:
            WHERE_CLAUSE = ''
        elif self.mode == 'after':
            WHERE_CLAUSE = 'WHERE {DATEVARIABLE} > DATEADD(DAY, {DELAY}, filtering_part.ref_date)'
        elif self.mode == 'before':
            WHERE_CLAUSE = 'WHERE {DATEVARIABLE} < DATEADD(DAY, {DELAY}, filtering_part.ref_date)'
        elif self.mode == 'between':
            WHERE_CLAUSE = 'WHERE {DATEVARIABLE} BETWEEN DATEADD(DAY, {DELAY}, filtering_part.begin_date) AND DATEADD(DAY, {DELAY}, filtering_part.end_date)'
        elif self.mode == 'outside':
            WHERE_CLAUSE = 'WHERE {DATEVARIABLE} < filtering_part.begin_date or {DATEVARIABLE} > filtering_part.end_date'
        op = 'min' if self.first_presence is True else 'max'
        sql = self.query.replace('{OPERATION}', op).format(references=references, referencevars=referencevars, WHERE=WHERE_CLAUSE.replace('{DELAY}', self.delay if self.delay else '0').replace('{DATEVARIABLE}', self.table_date_variable), DATEVARIABLE=self.table_date_variable)
        return pd.read_sql_query(sql, con=self.engine)


class Income(DBTable):
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


class CrimesOutcome(DBTable):
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


class IndexMultipleDeprivation(DBTable):
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


class CrimesStreet(DBTable):
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
                            from (values {references}) tempT(oa)
                         ), condition as (
                            select *
                            from census2011.{table}
                         )
                         select condition.* from filtering_part left join condition on filtering_part.oa = condition.oa"""
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
                            from (values {references}) tempT({from_variable})
                         ),
                         new_variables as (
                            select distinct "{from_variable}", "{to_variables}" from public.postcode_lookup11 where "{from_variable}" in {references_l}
                         )
                         select new_variables.* from filtering_part left join new_variables on filtering_part.{from_variable} = new_variables.{from_variable}
                         """.replace('{from_variable}', from_variable).replace('{to_variables}', '","'.join(to_variables)), engine=engine, rename=False)


class DataCollector:
    """
    Main class for data collection, this class will handle all the others
    """
    def __init__(self, database_file_handler, sources=None, reference_check_engine=None, verbose=False):
        self.database_file_handler = database_file_handler
        self.sources = sources
        self.checked = False
        self.reference_check_engine = reference_check_engine
        self.verbose = verbose
        
    def reference_check(self, columns):
        def _flatten(ll):
            ret = list()
            for i in ll:
                if type(i) is list:
                    ret += i
                else:
                    ret.append(i)
            return ret
        missing_references = list(set(_flatten([i.reference for i in self.sources])) - set(columns))
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
            start_chunk = time.time()
            if not self.checked:
                self.reference_check(chunk.columns.values)
                self.checked = True
            for d in self.sources:
                start_time = time.time()
                ndf = d.obtain_data(chunk[d.reference])
                internal_time = time.time()
                if type(d.reference) is list: #TODO maybe another more elegant way?
                    chunk = chunk.merge(ndf, on=d.reference[0], how='left', copy=False)
                else:
                    chunk = chunk.merge(ndf, on=d.reference, how='left', copy=False)
                if self.verbose:
                    print("|- Internal processing took {}s".format(time.time() - internal_time))
                    print("|- Source '{}' took {}s".format(d.__class__.__name__, time.time() - start_time))
            if self.verbose:
                print("Chunk took {}s".format(time.time() - start_chunk))
            yield chunk


