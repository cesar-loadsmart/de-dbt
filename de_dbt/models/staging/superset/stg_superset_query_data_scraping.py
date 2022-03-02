#!/usr/bin/env python
# coding: utf-8
import re
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import BOOLEAN
from sqlalchemy.types import BIGINT

def char_len(x, fixed_n):
    '''set string x to fixed_n character, prepend with 'xxx' if short'''
    if len(x) > fixed_n: 
        return x[:fixed_n] 
    return x 

def main():
    
    data=ref("stg_superset_query_sql")
    data['sql'] = data['sql'].astype(str)

    print ('After ingested dtypes: \n', data.dtypes)

    print('Dataframe shape: ', data.shape)
    
    data['tables']= data.apply(lambda row: table_scrapper(str(row['sql'])), axis=1)

    print('Successfull scraped')

    #validation
    results = []
    for index, record in data.iterrows():
        el_list = str(record['tables']).split('; ')
        result = True
        for word in el_list:
            if word not in record['sql']:
                result = False
                break
        results.append(result)
    print('Applying validation')
    data.insert(3, 'validation', results)

    #data = data.drop(columns=['sql'])

    data['sql'] = data['sql'].apply(lambda x: char_len(x,65000))

    data['tables'] = data['tables'].apply(lambda x: char_len(x,65000))  

    print('After scraped and validation dtypes: \n', data.dtypes)
    print('New Dataframe shape: ', data.shape)

    print("Uploading\n", data.head())

    write_to_source(data, 'odinprep_dbt_qa', 'stg_superset_tables_scraped_by_query', dtype={
        'id':BIGINT, 'sql':VARCHAR(65535), 'tables':VARCHAR(65535), 'validation':BOOLEAN})

def table_scrapper(sql):
    sql = re.sub(r"(\sjoin\s)|(\sfrom\s)", ' <<key>>', sql) #Inserting key for split
    sql = re.sub(r"(group by\s)|(where\s)|(limit\s)|(order by\s)|(having\s)|(right\s)|(left\s)|(inner\s)|(\son\s)|(union\s)|(select\s)|(\sas\s)|(full\s)|(and\s)", ' ', sql)#Removing reserved words
    sql = sql.replace('\n', ' ').replace('\r', ' ').replace('\t',' ' ) #Removing break lines strings and tab
    sql_splited = sql.split('<<key>>') #spliting using the key
    final_tables =[]
    try:
        for element in sql_splited[1:]: #sql_splited holds the Select with the columns
            element = element.replace('  ',' ').split(' ')
            element =list(filter(lambda word:word != '', element))
            if not (')' in element[0] or '(' in element[0] or '*' in element[0]):
                final_tables.append(element[0].replace('--', ''))
    except: 
        final_tables = ['scrapping invalid']
    final_tables = list( dict.fromkeys(final_tables)) # Removing duplicates
    return ('; ').join(final_tables)

main()
