
from extractor import *

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


d = DataCollector(db_get, sources=[Income(engine)] + [i for i in Crime.get_tables(engine)] + [i for i in Census11.get_tables(engine)], reference_sources=[PostcodeMapping], reference_engines=[engine])
e = pd.concat([i for i in d.collect()]) #only use concat if the data can fit the memory
print(e.head())
print(e.columns.values)
print(e['pc'])
