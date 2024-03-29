
"""
Definition of main types of extractors
"""


import pandas as pd
from integrator.util import ObtainDataError
import os

class DataSource:
    """
    This class provides the abstraction for data extraction connectors
    """
    def __init__(self, reference, name=None, rename=True):
        if name is None:
            name = self.__class__.__name__ #if there is no name use the default one
        self.name = name
        self.reference = reference
        self.rename = rename

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

    def obtain_data(self, mapping):
        pass

    def _post_op(self, df):
        if self.rename:
            df.rename(columns=lambda x: self.name + '.' + x if (not isinstance(self.reference, list) and x != self.reference) or (isinstance(self.reference, list) and x not in self.reference) else x, inplace=True)
        return df

    
class CSVTable(DataSource):
    loaded_files = dict() # file -> [([fields], pd)]
    
    @classmethod
    def get_file(cls, target_file, delimiter, columns, encoding=None, low_memory=True):
        if isinstance(columns, str):
            columns = [columns]
        if target_file in cls.loaded_files:
            for fields, df in cls.loaded_files[target_file]:
                if len(set(columns).intersection(set(fields))) == len(columns):
                    return df
        else:
            cls.loaded_files[target_file] = list()
        print('Loading file "{}" with columns "{}", this might take some time.'.format(target_file, '", "'.join(columns)))
        _df = pd.read_csv(target_file, delimiter=delimiter, index_col=False, usecols=columns, encoding=encoding, low_memory=low_memory)
        cls.loaded_files[target_file].append((columns, _df))
        return _df
    
    @classmethod
    def is_loaded(cls, target_file, delimiter, columns, encoding=None):
        if isinstance(columns, str):
            columns = [columns]
        if target_file in cls.loaded_files:
            for fields, df in cls.loaded_files:
                if len(set(columns).intersection(set(fields))) == len(columns):
                    return True
        return False
    
    def __init__(self, reference, target_file, target_columns=None, delimiter=',', encoding=None, name=None, low_memory=True):
        super().__init__(reference=reference, name=name)

        self.encoding = encoding
        self.delimiter = delimiter
        self.target_file = target_file
        if isinstance(target_columns, str):
            target_columns = [target_columns]
        self.target_columns = target_columns
        self.low_memory = low_memory

        if not os.path.exists(target_file):
            raise ObtainDataError('File "{}" does not exists.'.format(target_file))

        _testdf = pd.read_csv(self.target_file, delimiter=self.delimiter, nrows=0, encoding=self.encoding, low_memory=low_memory)

        if reference not in _testdf.columns.values:
            raise ObtainDataError('Reference column "{}" not found.'.format(reference))

        _invalid_columns = list()
        for i in self.target_columns:
            if i not in _testdf.columns.values:
                _invalid_columns.append(i)
        if len(_invalid_columns) > 0:
            raise ObtainDataError('Not possible to find columns "{}".'.format('", "'.join(_invalid_columns)))

        if self.target_columns is None:
            self.target_columns = _testdf.columns.values
        self._df = CSVTable.get_file(target_file, delimiter=delimiter, columns=[self.reference] + self.target_columns, encoding=self.encoding, low_memory=self.low_memory)

    def _post_op(self, df):
        return super()._post_op(df)

    def obtain_data(self, mapping, warning=True):
        if warning:
            print("TODO: this call does not perform any check")
        return self._post_op(self._df.loc[self._df[self.reference].isin(mapping)])


class DBTable(DataSource):
    """
    This is a generic extractor for a table. This needs to be derived to create an extractor for another table.

    @param reference: reference variable (to be used from a reference pandas data frame)
    @param query: query to be run against the engine
    @param engine: an sqlalchemy engine
    @param rename: if we should rename the returned table (instead of the class name) to name
    @param name: name to show on the returned data
    """
    def __init__(self, reference, query, engine=None, rename=True, name=None):
        super().__init__(reference=reference, name=name, rename=rename)
        self.engine = engine
        self.query = query
        self._number_cols = 0
        self._columns = []
        self._query_sql = None

       
    def _format_for_query(self, values):
        """
        Prepares the values to fit the SQL query
        """
        return self._format_for_query_multiple(values)

    def _format_for_query_multiple(self, values):
        """
        Formats the values to the multiple references required.
        """
        if isinstance(values, pd.core.frame.DataFrame):
            return ', '.join([ "('" + "', '".join([str(i) if not isinstance(i, pd.Series.dt) else i.strftime("'%Y-%m-%d'") for i in els ]) + "')" for els in zip(*[values[r] for r in self.reference])])
        elif isinstance(values, pd.core.series.Series):
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
        self._query_sql = self.query.format(references=self._format_for_query(mapping), references_l=self._format_for_query(mapping), referencevars=', '.join(self.reference))
        return pd.read_sql_query(self._query_sql, con=self.engine)
    
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
        self.reference = reference if type(reference) is list else [reference]
        self.inputvars = reference if type(reference) is list else [reference]
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
                raise ObtainDataError('Missing values for "{}": "{}"{EXTRA_COLUMNS}. Check the "mode" and other fields required.'.replace('{EXTRA_COLUMNS}', EXTRA_COLUMNS).format(self.__class__.__name__, '", "'.join(missing_columns)))
            elif len(extra_columns) > 0:
                raise ObtainDataError('Extra columns for "{}". Extra columns {} were ignored. Check the "mode" and other fields required.'.format(self.__class__.__name__, EXTRA_COLUMNS))

        if self.mode in self._VALID_MODES:
            _check_columns(self.begin_date, self.end_date, self.ref_date, self.delay, self._VALID_MODES[self.mode])
        else:
            raise ObtainDataError('"mode" option invalid for "{}". Expected "{}".'.format(self.__class__.__name__, '", "'.join([str(i) for i in self._VALID_MODES[self.mode]])))

    def _obtain_data(self, mapping):
        """
        When obtaining data using time reference we need to correct some terms in the query.
        """
        references = self._format_for_query(mapping)
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
        self._query_sql = self.query.replace('{AS_TERM}', AS_TERM).replace('{GROUPBY_TERM}', GROUPBY_TERM).replace('{OPERATION}', op).format(references=references, referencevars=referencevars, WHERE=WHERE_CLAUSE.replace('{DELAY}', self.delay if self.delay else '0').replace('{DATEVARIABLE}', self.table_date_variable), DATEVARIABLE=self.table_date_variable)
        return pd.read_sql_query(self._query_sql, con=self.engine, parse_dates=self.reference[1:]) # XXX: the dates would be better in a specific column (avoiding the conversion of wrong columns)


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

