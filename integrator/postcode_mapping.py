

from integrator.mapping import DBMapping


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

