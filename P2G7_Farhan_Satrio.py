'''
=================================================
Graded Challenge 7

Nama  : Farhan Satrio
Batch : FTDS-023-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. 
Adapun dataset yang dipakai adalah dataset mengenai Draft NBA dari tahun 1990 sampai 2001.
=================================================
'''

import pandas as pd
import numpy as np
import psycopg2 as db
from elasticsearch import Elasticsearch

#Menyambungkan ke Postgree dan meload data
def get_data_from_postgresql():
    conn_string="dbname='postgres' host='localhost' user='postgres' password='farhansatrio1'"
    conn=db.connect(conn_string)
    df = pd.read_sql("SELECT * from TABLE_GC7", conn)
    print(df)
    print("-------Data Saved------")
    
    return df

# Cleaning Data
def clean_data(df):
    #Drop data yang duplicate
    df.drop_duplicates(inplace= True)
    
    df.replace('NA', np.nan, inplace=True)
    #Drop data yang mengandung NA
    df.dropna(inplace= True)
    
    df.columns = df.columns.str.strip()
    
    df['rk'] = df['rk'].astype(int)
    df['pk'] = df['pk'].astype(int)
    df['tm'] = df['tm'].astype(str)
    df['player'] = df['player'].astype(str)
    df['college'] = df['college'].astype(str)
    df['yrs'] = df['yrs'].astype(int)
    df['g'] = df['g'].astype(int)
    df['totmp'] = df['totmp'].astype(int)
    df['totpts'] = df['totpts'].astype(int)
    df['tottrb'] = df['tottrb'].astype(int)
    df['totast'] = df['totast'].astype(int)
    df['FG%'] = df['FG%'].astype(float)
    df['3P%'] = df['3P%'].astype(float)
    df['FT%'] = df['FT%'].astype(float)
    df['ws'] = df['ws'].astype(float)
    df['WS/48'] = df['WS/48'].astype(float)
    df['bpm'] = df['bpm'].astype(float)
    df['vorp'] = df['vorp'].astype(float)
    df['draftyr'] = df['draftyr'].astype(int)
    df['mpg'] = df['mpg'].astype(float)
    df['ppg'] = df['ppg'].astype(float)
    df['rpg'] = df['rpg'].astype(float)
    df['apg'] = df['apg'].astype(float)
    df['playerurl'] = df['playerurl'].astype(str)
    df['draftyear'] = df['draftyear'].astype(int)
    
    
    df.columns = df.columns.str.lower()
    
    return df
# Transfer data elastic search
def elastic_transfer():
    es = Elasticsearch("http://localhost:9200")
    df_clean = pd.read_csv('P2G7_Farhan_Satrio_data_clean.csv')
    for i, r in df_clean.iterrows():
        doc = r.to_json()  
        res = es.index(index="gc7_farhan", doc_type="doc", body=doc)  
        print(res)
        
# Running Script
if __name__ == "__main__":
    df = get_data_from_postgresql()  
    df_clean = clean_data(df)
    df_clean.to_csv('P2G7_Farhan_Satrio_data_clean.csv', index=False) 
    elastic_transfer()  
        