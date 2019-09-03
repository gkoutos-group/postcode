
"""
Definition of mapping classes
"""

from .tables import DBTable
from .util import ObtainDataError


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


