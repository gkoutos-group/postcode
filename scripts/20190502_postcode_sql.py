

def create_tables():
    """
    This function is not needed given that the copy function used will create these tables
    """
    import psycopg2
    conn = psycopg2.connect("host=localhost post=5432 dbname=postcode user=postgres")
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE crimes_outcomes(
        id CHAR(65) PRIMARY KEY,
        month DATE,
        longitude REAL,
        latitude REAL,
        LSOA_code CHAR(10),
        outcome TEXT);
        
    CREATE TABLE crimes_stopsearch(
        id SERIAL PRIMARY KEY,
        type TEXT,
        date DATE,
        longitude REAL,
        latitude REAL,
        age TEXT,
        self_defined_ethnicity TEXT,
        officer_ethnicity TEXT,
        object_of_search TEXT,
        outcome text,
        outcome_to_search BOOLEAN,
        removal_of_inner_clothing BOOLEAN);
        
    CREATE TABLE crimes_street(
        id CHAR(65) PRIMARY KEY,
        month DATE,
        longitude REAL,
        latitude REAL,
        LSOA_code CHAR(10),
        crime_type TEXT,
        outcome TEXT);
    """)
    conn.commit()
    conn.close()

#note that there could be some repeated Crime ID


import re
import pandas as pd
from glob import glob
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres@localhost:5432/postcode')

def crimes():
    pattern = re.compile('[\W_]+')
    files = glob("crime/**/*.csv", recursive=True)
    outcomes = [pd.read_csv(i, index_col=None, header=0) for i in files if "outcomes" in i]
    stopsearch = [pd.read_csv(i, index_col=None, header=0) for i in files if "stop-and-search" in i]
    street = [pd.read_csv(i, index_col=None, header=0) for i in files if "street" in i]
    #crime outcomes database
    df_outcomes = pd.concat(outcomes, axis=0, ignore_index=True)
    df_outcomes.rename(columns={'Month':'month',
                                'Longitude': 'longitude',
                                'Latitude': 'latitude',
                                'LSOA code': 'lsoa',
                                'Outcome type': 'outcome',
                                'Crime ID': 'count'}, inplace=True)
    df_outcomes.drop(columns=['Reported by', 'Falls within', 'Location', 'LSOA name'], inplace=True) #'Crime ID'
    df_outcomes.to_sql(name='crimes_outcomes', con=engine, schema='raw', method='multi', index=False, index_label='lsoa')
    # get the year from the year-month
    df_outcomes['year'] =  df_outcomes['month'].str.extract(r'^([\d]+)-')[0] # pd.to_datetime(df_outcomes['month'], format='%Y-%m')
    df_outcomes.drop(columns=['latitude', 'longitude', 'month'], inplace=True)
    #the next line will count the total of cases by year
    total = df_outcomes.groupby(['lsoa', 'year'], as_index=False).count()
    total.drop(columns=['outcome'], inplace=True)
    total['year'] = 'Total-' + total['year']
    total = total.pivot_table(index='lsoa', columns='year', values='count')
    #here we are counting by type and year
    df_outcomes = df_outcomes.groupby(['lsoa', 'outcome', 'year'], as_index=False).count()
    # pivot for compiled
    df_outcomes = df_outcomes.pivot_table(index='lsoa', columns=['outcome', 'year'], values='count').rename(columns=lambda x: pattern.sub('', x.lower().replace('without', 'w i t h o u t').replace('with', 'w i t h').replace('median', 'm e d i a n').replace('mean', 'm e a n').replace('lot', 'l o t').replace('little', 'l i t t l e').title().replace('Value', '').replace('Measures', '')))
    #concatenate the multi-level columns
    df_outcomes.columns = ['%s%s' % (a, '-%s' % b if b else '') for a, b in df_outcomes.columns]
    #join the columns
    df_outcomes = df_outcomes.join(total)
    df_outcomes.fillna(0, inplace=True)
    # send to database
    df_outcomes.to_sql(name='crimes_outcomes_yearly', con=engine, schema='compiled', method='multi', index_label='lsoa')
    # stop and search database
    df_stopsearch = pd.concat(stopsearch, axis=0, ignore_index=True)
    df_stopsearch.rename(columns={'Type': 'type',
                                'Date': 'date',
                                'Latitude': 'latitude',
                                'Longitude': 'longitude',
                                'Age range': 'age',
                                'Self-defined ethnicity': 'self_defined_ethnicity',
                                'Officer-defined ethnicity': 'officer_ethnicity',
                                'Object of search': 'object_of_search',
                                'Outcome': 'outcome',
                                'Outcome linked to object of search': 'outcome_to_search',
                                'Removal of more than just outer clothing': 'removal_of_inner_clothing'}, inplace=True)
    #the next operations are ignored due to not being useful for next steps
    df_stopsearch.drop(columns=['Part of a policing operation', 'Policing operation', 'Gender', 'Legislation'], inplace=True)
    df_stopsearch.to_sql(name='crimes_stopsearch', con=engine, schema='raw', method='multi', index=False, index_label='lsoa')
    #df_stopsearch['date'] = pd.to_datetime(df_stopsearch['date']) #, format='%Y-%m-%dT%H:%M:%S+%Z:00')
    # this table is not worked much. it is sent to the raw schema
    # street crimes database
    #for this database we are following an approach close to the crimes outcomes
    df_street = pd.concat(street, axis=0, ignore_index=True)
    # send the raw database
    df_street.rename(columns={'Month': 'month',
                            'Longitude': 'longitude',
                            'Latitude': 'latitude',
                            'LSOA code': 'lsoa',
                            'Crime type': 'crime_type',
                            'Crime ID': 'count',
                            'Last outcome category': 'outcome'}, inplace=True)
    df_street.drop(columns=['Reported by', 'Falls within', 'Location', 'LSOA name', 'Context'], inplace=True)
    df_street.to_sql(name='crimes_street', con=engine, schema='raw', method='multi', index=False, index_label='lsoa')
    df_street['year'] =  df_street['month'].str.extract(r'^([\d]+)-')[0]
    df_street.drop(columns=['latitude', 'longitude', 'outcome', 'month'], inplace=True)
    #the next line will count the total of cases by year
    total = df_street.groupby(['lsoa', 'year'], as_index=False).count()
    total.drop(columns=['crime_type'], inplace=True)
    total['year'] = 'Total-' + total['year']
    total = total.pivot_table(index='lsoa', columns='year', values='count')
    #here we are counting by type and year
    df_street = df_street.groupby(['lsoa', 'crime_type', 'year'], as_index=False).count()
    # pivot for compiled
    df_street = df_street.pivot_table(index='lsoa', columns=['crime_type', 'year'], values='count').rename(columns=lambda x: pattern.sub('', x.lower().replace('without', 'w i t h o u t').replace('with', 'w i t h').replace('median', 'm e d i a n').replace('mean', 'm e a n').replace('lot', 'l o t').replace('little', 'l i t t l e').title().replace('Value', '').replace('Measures', '')))
    #concatenate the multi-levle columns
    df_street.columns = ['%s%s' % (a, '-%s' % b if b else '') for a, b in df_street.columns]
    #join the columns
    df_street = df_street.join(total)
    df_street.fillna(0, inplace=True)
    # send to database
    df_street.to_sql(name='crimes_street_type_yearly', con=engine, schema='compiled', method='multi', index_label='lsoa')
    

def postcode_lookup():
    dfp = pd.read_csv('Postcode_Lookup_in_the_UK.csv', index_col=None, header=0, low_memory=False)
    #pcd7, pcd8, pcds: postcodes
    #dointr: possibly start of postcode
    #doterm: possibly end of postcode
    #usertype
    #oa11cd: output area
    #lsoa11cd: lower super output area
    #msoa11cd: middle super output area
    #ladcd: local authority district
    #lsoa11nm: lower super output area
    #msoa11nm: middle super output area
    #ladnm, ladnmw: local authority district
    #FID: unique identifier
    #cd: code
    #nm: name
    dfp.rename(columns={'oa11cd':'oa', 'lsoa11cd': 'lsoa', 'msoa11cd': 'msoa', 'ladcd': 'lad', 'pcd7': 'postcode'}, inplace=True)
    dfp.set_index('FID', inplace=True)
    dfp.to_sql(name="postcode_lookup11", con=engine, schema='public', method='multi', index_label='FID', chunksize=(2**5)*(2**10)) #this table is quite massive, it needs to be optimized or have a bit of patience (~5min with 8096 chunksize)


def census():
    pattern = re.compile('[a-z\W_]+')
    # the census files downloaded were from http://nomisweb.co.uk/ and the region of west midlands - if the region is increased a chunksize will be required in the to_sql call!
    census_files = glob("census2011/*.csv")
    for f in census_files:
        name = f[f.find('/')+1:f.find('.csv')]
        print(name)
        df = pd.read_csv(f, index_col=None, header=0)
        df.drop(columns=['geography'], inplace=True)
        original = df.columns.values #note that for printing the relation it needs to be after the column dropping
        n = len(df.columns.values)
        df.rename(columns=lambda x: pattern.sub('', x.lower().replace('without', 'w i t h o u t').replace('with', 'w i t h').replace('median', 'm e d i a n').replace('mean', 'm e a n').replace('lot', 'l o t').replace('little', 'l i t t l e').title().replace('Value', '').replace('Measures', '')), inplace=True)
        #print(df.columns.values)
        df.rename(columns={'D':'date', 'GC': 'oa'}, inplace=True)
        print('\n'.join(['"{}","{}"'.format(i,j) for i,j in zip(original, df.columns.values)]))
        k = [len(i) for i in df.columns.values]
        if max(k) > 60:
            print(k)
            print(df.columns.values)
            break
        #due to some issues some variables were changed to avoid problems: little, lot, mean, median
        if len(set(df.columns.values)) != len(df.columns.values) or n != len(df.columns.values):
            print(n, df.columns.values, len(df.columns.values), len(set(df.columns.values)))
            break
        df.to_sql(name='{}'.format(name), con=engine, schema='census2011', method='multi')


def income():
    files = ['income/1netannualincome.csv', 'income/1netannualincomeahc.csv', 'income/1netannualincomebhc.csv', 'income/1totalannualincome.csv']
    skips = [2, 4, 4, 2]
    names = ['net_annual_income', 'net_inc_aft_housing', 'net_inc_bef_housing', 'total_income']
    dfs = list()
    for i in range(len(files)):
        df = pd.read_csv(files[i], skiprows=skips[i], header=0, index_col=None, encoding='latin1')
        df.drop(columns=['MSOA name', 'Local authority code', 'Local authority name', 'Region code', 'Region name'], inplace=True)
        unnamed_cols = [i for i in df.columns.values if 'Unnamed' in i]
        if len(unnamed_cols) > 0:
            df.drop(columns=unnamed_cols, inplace=True)
            print("dropped", unnamed_cols)
        df.columns = ['msoa', names[i], names[i] + '_upper_ci', names[i] + '_lower_ci', names[i] + '_ci']
        dfs.append(df)
    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.merge(dfs[i], on='msoa', how='outer')
        #print(df.columns.values)
        #print(df.head())
        #print(len(df))
    df.set_index('msoa', inplace=True)
    df.to_sql(name='income', con=engine, schema='compiled', method='multi', index_label='msoa')

