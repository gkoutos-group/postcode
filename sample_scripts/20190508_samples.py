
# possibly need to install:
# pip install SQLAlchemy
# pip install psycopg2

# load tool functions
from extractor import *

# sql connection
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres@localhost:5432/postcode')

# sample data
test_list = ['huehuehue', 'E02004295', 'E02004320', 'E02004321', 'E02004322', 'E02004296', 'E02004306', 'E02004323', 'E02004308', 'E02004309', 'E02004307', 'E02004324', 'E02004310', 'E02004311', 'E02004311', 'huehuehue']

test_list = ['huehuehue', 'E00070659', 'E00070660', 'E00070943', 'E00070949', 'E00070950', 'E00070951', 'E00070955', 'E00070956', 'E00070944', 'E00070945', 'E00070946', 'E00070947', 'E00070948', 'E00070952', 'E00070953', 'E00070954', 'E00071184', 'E00071185', 'E00071190', 'E00071191', 'E00071194', 'E00071194', 'huehuehue']

test_list = ['B152TT']

# a data extractor source for income data
e = Income(engine)
# obtain data for some samples
a = e.obtain_data(test_list)
#print(a)

# mapping function between oa and msoa
j = PostcodeMapping('oa', 'msoa', engine)
# transform between oa and msoa for test_list
j.obtain_data(test_list)


# in your data frame you have the postcode ("pc"), and any other variables
df = {'pc': test_list, 'kek': [i for i in range(len(test_list))]}
df = pd.DataFrame(data=df)

# if you want to personalise the batch operations on the data:
def db_get():
    for i, j in df.groupby(np.arange(len(df))//4096):
        yield j

# batch operations reading from csv:
def db_get2(FILE):
    return pd.read_csv(FILE, chunksize=4096)

# this is the good stuff
d = DataCollector(db_get, # could call with "df" directly
                  sources=[Income(engine)] + [i for i in Crime.get_tables(engine)] + [i for i in Census11.get_tables(engine)], 
                  reference_sources=[PostcodeMapping], # source for the automatic identification of mappings
                  reference_engines=[engine]) # match the source with the sql engine
e = pd.concat([i for i in d.collect()]) # this completes the df back; only use concat if the data can fit the memory
print(e.head())
print(e.columns.values)
print(e['pc'])
