

from sqlalchemy import create_engine
import pandas as pd
import numpy as np


class ObtainDataError(Exception):
    pass


class DBTable:
    def __init__(self, reference, query, engine=None):
        self.reference = reference
        self.engine = engine
        self.query = query
        self._number_cols = 0
    
    def format_for_query(self, values):
        return self.query.format(references="('" + "'), ('".join(values) + "')")
    
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
        if len(sql_ret) != len(mapping):
            raise ObtainDataError('Invalid number of rows returned. Deactivate {}'.format(self.__class__.__name__))
        sql_ret.drop(columns=[self.reference], inplace=True)
        sql_ret.rename(columns={'mapping': self.reference}, inplace=True)
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


class Crimes(DBTable):
    def __init__(self, engine):
        super().__init__('lsoa', query="""
                         with filtering_part as (
                            select *
                            from (values {references}) tempT(mapping)
                         )
                         select * from filtering_part left join compiled.crimes_outcomes_yearly on filtering_part.mapping = compiled.crimes_outcomes_yearly.lsoa
                         """, engine=engine)


class DataCollector:
    def __init__(self, database_file_handler, sources=None):
        self.database_file_handler = database_file_handler
        self.sources = sources
        self.checked = False
        
    def reference_check(self, columns):
        missing_references = list(set([i.reference for i in self.sources]) - set(columns))
        if len(missing_references) > 0:
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

e = Income(engine)
e.obtain_data(['huehuehue', 'E02004295', 'E02004320', 'E02004321', 'E02004322', 'E02004296', 'E02004306', 'E02004323', 'E02004308', 'E02004309', 'E02004307', 'E02004324', 'E02004310', 'E02004311'])

df = {'msoa': ['huehuehue', 'E02004295', 'E02004320', 'E02004321', 'E02004322', 'E02004296', 'E02004306', 'E02004323', 'E02004308', 'E02004309', 'E02004307', 'E02004324', 'E02004310', 'E02004311'], 'kek': [i for i in range(14)]}
df = pd.DataFrame(data=df)

def db_get():
    for i, j in df.groupby(np.arange(len(df))//4096):
        yield j

def db_get2(FILE):
    return pd.read_csv(FILE, chunksize=4096)

d = DataCollector(db_get, sources=[Income(engine)])
e = [i for i in d.collect()]




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

