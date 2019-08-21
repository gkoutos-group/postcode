

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import time


class ObtainDataError(Exception):
    pass


class DBTable:
    """
    This is a generic extractor for a table. This needs to be derived to create an extractor for another table.

    @param reference: reference variable (to be used from a reference pandas data frame)
    @param query: query to be run against the engine
    @param engine: an sqlalchemy engine
    @param rename: if we should rename the returned table (instead of the class name) to name
    @param name: name to show on the returned data
    """
    def __init__(self, reference, query, engine=None, rename=True, name=None):
        self.reference = reference
        self.engine = engine
        self.query = query
        self._number_cols = 0
        self._columns = []
        self.rename = rename
        if name is None: #if there is no name we aare going to use the default behaviour
            name = self.__class__.__name__
        self.name = name

    def __str__(self):
        """
        Format the str call to "{given name}: {references}"
        """
        return '{}: {}'.format(self.name, self.reference)

    def __repr__(self):
        """
        The implicit name of the object changed to "<str() @ addr>"
        """
        return '<' + self.__str__() + ' @ ' + str(hex(id(self))) + '>'
        
    def _format_for_query(self, values):
        """
        Prepares the values to fit the SQL query
        """
        values = [str(i) for i in set(values) if str(i) != ''] #XXX the if is a precaution against NULL values
        return  "('" + "'), ('".join(values) + "')"

    def _obtain_pre_checks(self, mapping):
        """
        Some pre-checks before execution.
        """
        if self.engine is None:
            raise ObtainDataError('Engine is not defined')

    def _obtain_post_checks(self, mapping, sql_ret):
        """
        Checks after the data collection.
        """
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
        """
        Main iteraction loop, format the query and collects the data
        """
        sql = self.query.format(references=self._format_for_query(mapping), references_l=self._format_for_query(mapping))
        return pd.read_sql_query(sql, con=self.engine)
    
    def obtain_data(self, mapping):
        """
        Obtain data for a set of mapping values. Pre-checks, collection and post-checks are executed in order.
        """
        self._obtain_pre_checks(mapping) #pre-checks related to input or current state
        sql_ret = self._obtain_data(mapping)
        self._obtain_post_checks(mapping, sql_ret) #post-checks related to the output
        if self.rename: # add names associated with this class
            sql_ret.rename(columns=lambda x: self.name + '.' + x if (type(self.reference) is not list and x != self.reference) or (type(self.reference) is list and x not in self.reference) else x, inplace=True)
        return sql_ret


class DBTableTimed(DBTable):
    """
    Definition for the extraction of data related to some time point or interval.

    The internal variable list (self.reference) expects the elements self.reference[1:] to be dates.

    @param reference: reference variable
    @param query: the query to be adjusted
    @param engine: an sqlalchemy engine
    @param rename: 
    @param name: 
    @param mode: 4 modes are possible: 'between' a range of dates; 'outside' a range of dates; 'before' a reference date; 'after' a reference date
    @param begin_date: for modes 'between' and 'outside': this is the variable name that indicates the starting date range
    @param end_date: for modes 'between' and 'outside': this is the variable name that indicates the ending date range
    @param ref_date: for modes 'before' and 'after': this is the variable name that indicates the reference date used
    @param delay: a negative or positive number to indicate the shift (in days) for the above dates
    @param first_presence: by default operates on the maximal date, otherwise the minimal
    @param table_date_variable: the table with the data
    """
    # the modes contain the possible variables: begin_date, end_date, ref_date, delay
    _VALID_MODES = {None: [False, False, False],
                   'between': [True, True, False],
                   'outside': [True, True, False],
                   'before': [False, False, True],
                   'after': [False, False, True]
                   }

    def __init__(self, reference, query, engine=None, rename=True, name=None, mode=None, begin_date=None, end_date=None, ref_date=None, delay=None, first_presence=None, table_date_variable=None):
        super().__init__(reference, query, engine, rename, name)
        self.mode = mode
        self.begin_date = begin_date
        self.end_date = end_date
        self.ref_date = ref_date
        self.delay = str(int(delay)) if delay else '0'
        self.reference = [reference]
        self.inputvars = [reference]
        self.first_presence = first_presence
        self.table_date_variable = table_date_variable
        self._check_settings()

    def _check_settings(self):
        """
        On startup check for the valid extraction modes
        """
        def _check_columns(begin_date, end_date, ref_date, delay, mode_setup):
            """
            Check for wrong variables passage given the mode
            """
            missing_columns = list()
            extra_columns = list()
            for st, nd in zip([('begin_date', begin_date), ('end_date', end_date), ('ref_date', ref_date)], mode_setup):
                i, j = st
                if nd is False and j is None: #the options are from the _VALID_MODES settings
                    continue
                elif nd is False and j is not None:
                    extra_columns.append('{}": "{}'.format(i, j))
                elif nd is True and j is None:
                    missing_columns.append('{}": "{}'.format(i, j))
                elif nd is True and j is not None:
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
        """
        Formats the values to the multiple references required.
        """
        if isinstance(values, pd.core.frame.DataFrame):
            return ', '.join([ "('" + "', '".join([str(i) if not isinstance(i, pd.Series.dt) else i.strftime("'%Y-%m-%d'") for i in els ]) + "')" for els in zip(*[values[r] for r in self.reference])])
        elif isinstance(values, pd.core.series.Series):
            values = [str(i) for i in set(values) if str(i) != ''] # copy-pasta of super class behaviour
            return  "('" + "'), ('".join(values) + "')"

    def _obtain_data(self, mapping):
        """
        When obtaining data using time reference we need to correct some terms in the query.
        """
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
        AS_TERM = ''
        GROUPBY_TERM = ''
        for we_have, in_dataset in zip(self.reference[1:], self.inputvars[1:]): #this is going to be added for the passage back
            AS_TERM += ', filtering_part.{GIVEN_NAME} AS {DATASET_NAME}'.format(GIVEN_NAME=in_dataset, DATASET_NAME=we_have)
            GROUPBY_TERM += ', filtering_part.{GIVEN_NAME}'.format(GIVEN_NAME=in_dataset)
        sql = self.query.replace('{AS_TERM}', AS_TERM).replace('{GROUPBY_TERM}', GROUPBY_TERM).replace('{OPERATION}', op).format(references=references, referencevars=referencevars, WHERE=WHERE_CLAUSE.replace('{DELAY}', self.delay if self.delay else '0').replace('{DATEVARIABLE}', self.table_date_variable), DATEVARIABLE=self.table_date_variable)
        return pd.read_sql_query(sql, con=self.engine, parse_dates=self.reference[1:])


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


class DBCategory:
    """
    A groupper class for different sources of data.
    """
    def get_tables(engine=None):
        """
        get_tables yields different sources

        @param engine: an sqlalchemy engine
        """
        yield
        

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
    

class DBMapping(DBTable):
    """
    Generic class for mapping of one variable to another using a reference table.

    @param from_variable: the source variable(s) used
    @param to_variable: the target variable(s) needed
    @param engine: an sqlalchemy engine
    @param table: referece table
    """
    AVAILABLE = []
    def matching_source(cls, what_we_have, what_is_needed):
        """
        This function checks if we can match the needed variables using only the variables we have.

        @param what_we_have: the source variables we will use
        @param what_is_needed: the target variables needed from this table
        """
        matching_we_have = list(set(what_we_have).intersection(set(cls.AVAILABLE)))
        indexes_we_have = [cls.AVAILABLE.index(i) for i in matching_we_have]
        what_is_needed = list(what_is_needed)
        index_is_needed = [cls.AVAILABLE.index(i) for i in what_is_needed]
        if min(indexes_we_have) < min(index_is_needed):
            return matching_we_have[indexes_we_have.index(min(indexes_we_have))]
        raise ObtainDataError('Not possible to find any/all the columns "{}". We were looking with these matching columns "{}" (out of "{}").'.format('", "'.join(what_is_needed), '", "'.join(matching_we_have), '", "'.join(what_we_have)))

    def __init__(self, from_variable, to_variables, engine, table=None):
        if table is None or type(table) is not str:
            raise ObtainDataError('DBMapping requires a table. Please re-implement {}'.format(self.__class__.__name__))
        # first check for invalid column specification
        if type(from_variable) is not str or from_variable not in self.AVAILABLE:
            raise ObtainDataError('"{}" is not a str or not one of "{}".'.format(str(from_variable), '", "'.join(self.AVAILABLE)))
        if type(to_variables) is str:
            to_variables = [to_variables]
        if type(to_variables) is not list or any([type(i) is not str for i in to_variables]):
            raise ObtainDataError('to_variables must be str or a list of str')
        # check for not available columns
        not_in_list = [i for i in to_variables if i not in self.AVAILABLE]
        if len(not_in_list) > 0:
            raise ObtainDataError('Some variables ("{}") we have no information, expected one of "{}".'.format('", "'.join(not_in_list), '", "'.join(self.AVAILABLE)))
        # check for invalid scale: always go from smaller to bigger
        org = self.AVAILABLE.index(from_variable)
        if any([self.AVAILABLE.index(i) <= org for i in to_variables]):
            raise ObtainDataError('Trying to get a variable ("{}") that is more specific or in the same level as the query one ("{}").'.format('", "'.format(to_variables), from_variable))
        super().__init__(from_variable, query="""
                         with filtering_part as (
                            select *
                            from (values {references}) tempT({from_variable})
                         ),
                         new_variables as (
                            select distinct "{from_variable}", "{to_variables}" from {table} where "{from_variable}" in ({references_l})
                         )
                         select new_variables.* from filtering_part left join new_variables on filtering_part.{from_variable} = new_variables.{from_variable}
                         """.replace('{table}', table).replace('{from_variable}', from_variable).replace('{to_variables}', '","'.join(to_variables)), engine=engine, rename=False)


class PostcodeMapping(DBMapping): #XXX if mapping one bigger to smaller there might be issues!
    """
    This contains the functions for the different areas of mapping using the postcode are mapping. The mappings are: pc, oa, lsoa, msoa, lad

    @param from_variable: the source variables
    @param to_variables: the target variables
    @param engine: an sqlalchemy engine
    """
    AVAILABLE = ['pc', 'oa', 'lsoa', 'msoa', 'lad']
    def __init__(self, from_variable, to_variables, engine):
        table = 'public.postcode_lookup11'
        super().__init__(from_variable, to_variables, engine, table)


class DataCollector:
    """
    Main class for data collection, this class will handle all the others.

    @param database_file_handler: this is a function that yields data blocks
    @param sources: the difference data sources used
    @param reference_sources (list of DBMapping classes): these are the classes that map different reference variables
    @param reference_engines (list of sqlalchemy engines): the engines to be used by the respective list of reference sources
    @param verbose: output some (minimal) verbose information
    """
    def __init__(self, database_file_handler, sources=None, reference_sources=None, reference_engines=None, verbose=False):
        self.database_file_handler = database_file_handler
        if type(sources) is not list: #we are avoiding issues here
            sources = [sources]
        self.sources = sources
        self.checked = False
        self.verbose = verbose
        if reference_sources and reference_engines: #if we possibly doing mapping we need the same number of reference mappers and engine to use with them
            if len(reference_sources) != len(reference_engines):
                raise ObtainDataError('The references sources and engines have different length.')
            self._reference_engines = {i: j for i, j in zip(reference_sources, reference_engines)}
        else:
            self._reference_engines = None
        self._build_reference_graph(reference_sources)

    def _build_reference_graph(self, reference_sources):
        """
        This method builds a graph for future matching of columns.

        @param reference_sources: the references used for dependency resolution
        """
        graph = dict()
        if not reference_sources:
            self.reference_graph = None
            return
        for i in reference_sources:
            for a in i.AVAILABLE:
                if a not in graph:
                    graph[a] = {'neighbours': dict()}
                for b in i.AVAILABLE:
                    if a == b:
                        continue
                    if b not in graph[a]['neighbours']:
                        graph[a]['neighbours'][b] = list()
                    graph[a]['neighbours'][b].append(i)
        self.reference_graph = graph

    def _minimum_mapping(self, from_variables, to_variables):
        """
        This creates reference variables mapping using the minimum number of jumps possible.

        @param from_variables: the source variables
        @param to_variables: the variables we need
        """
        graph = self.reference_graph
        cur_elements = [(i, []) for i in from_variables]
        target_paths = dict() #this will map target-paths
        if graph:
            while len(cur_elements) > 0:
                ce, cpath = cur_elements.pop(0)
                if ce not in graph or 'visited' in graph[ce]:
                    continue
                else:
                    graph[ce]['visited'] = True
                for i in graph[ce]['neighbours'].keys():
                    if i in to_variables:
                        if i not in target_paths:
                            target_paths[i] = list()
                        for m in graph[ce]['neighbours'][i]:
                            target_paths[i].append((i, [(ce, m)] + cpath))
                    else:
                        for m in graph[ce]['neighbours'][i]:
                            cur_elements.append((i, [(ce, m)] + cpath))
        we_got = set([i for i in target_paths.keys()]).intersection(set(to_variables))
        if len(we_got) < len(to_variables):
            raise ObtainDataError('Not possible to map all the variables. We got "{}". We need "{}".'.format('", "'.join(we_got), '", "'.join(to_variables)))
        # TODO: the next step can be made more efficient by obtaining the minimum cut, in here we are using the shortest path instead
        currently_available = set()
        target_summarized = dict() #format from_variables, to_variables, method
        for target, options in target_paths.items():
            op_len = sorted([(len(i[1]), i) for i in options])
            target_path = op_len[0][1] # let's use the smallest one
            cur_source = target_path[1][0][0]
            cur_method = target_path[1][0][1]
            for i in range(0, len(target_path[1]) - 1):
                cur_source = target_path[1][i+1][0]
                cur_method = target_path[1][i+1][1]
                cur_target = target_path[1][i][0]
                target_summarized[len(target_summarized)] = {'source_variables': [cur_source], 'target_variables': [cur_target], 'method': cur_method}
                cur_method = target_path[1][i][1]
                cur_source = target_path[1][i][0]
            cur_target = target
            target_summarized[len(target_summarized)] = {'source_variables': [cur_source], 'target_variables': [cur_target], 'method': cur_method}
        # the values are compiled to reduce the number of queries
        compiled_targets = dict()
        for i in target_summarized.values():
            query = (i['source_variables'][0], i['method'])
            if query in compiled_targets:
                compiled_targets[query] += i['target_variables']
            else:
                compiled_targets[query] = i['target_variables']
        return compiled_targets

    def reference_check(self, columns):
        """
        Checks if the needed references can be found.

        @param columns: the columns we have currently
        """
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
            compiled_targets = self._minimum_mapping(columns, missing_references)
            for source, target_cols in compiled_targets.items():
                source_cols, source_method = source
                self.sources.insert(0, source_method(source_cols, target_cols, self._reference_engines[source_method]))

    def collect(self):
        """
        Collects the data each by chunk.

        For each chunk:
        1. add new columns from the dependency checks
        2. add new columns requested
        """
        for chunk in self.database_file_handler():
            start_chunk = time.time()
            # if it is a first run we need a column dependency check
            if not self.checked:
                dependency_check = time.time()
                self.reference_check(chunk.columns.values)
                self.checked = True
                if self.verbose:
                    print('|- Dependency resolution in {}s'.format(time.time() - dependency_check))
            # for each source of data (this will include dependencies)
            for d in self.sources:
                start_time = time.time()
                ndf = d.obtain_data(chunk[d.reference]) #obtain the data
                internal_time = time.time()
                # print(chunk.columns.values)
                # print(chunk.head())
                # print(ndf.columns.values)
                # print(ndf.head())
                chunk = chunk.merge(ndf, on=d.reference, how='left', copy=False) #merge the data using the reference variables
                if self.verbose:
                    print("|- Internal processing took {}s".format(time.time() - internal_time))
                    print("|- Source '{}' took {}s".format(d, time.time() - start_time))
            if self.verbose:
                print("Chunk took {}s".format(time.time() - start_chunk))
            yield chunk

