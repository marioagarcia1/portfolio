#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import cx_Oracle
import time
import sys
import os
import datetime as dt

def df_wos_sd_sum_pub_info(df):
    
    cols = ['CONTENT_PAGES' if 'content' in i else i.upper() for i in df.columns]
    
    df.columns = cols

    select_columns = [  'ID',
                        'VOL',
                        'ISSUE',
                        'PUBTYPE',
                        'PUBYEAR',
                        'PUBMONTH',
                        'COVERDATE',
                        'HAS_ABSTRACT',
                        'JOURNAL_OAS_GOLD',
                        'HAS_CITATION_CONTEXT',
                        'PART_NO',
                        'SUPPLEMENT',
                        'SPECIAL_ISSUE',
                        'CONTENT_PAGES',
                        'PAGE_COUNT',
                        'SORTDATE',
                        'EARLY_ACCESS_YEAR',
                        'EARLY_ACCESS_DATE',
                        'EARLY_ACCESS_MONTH']

    df = df[select_columns]
    
    df = df.astype(str)

    df = df.replace({'None':''})

    df = df.replace({'nan':''})

    dt_carga = dt.datetime.now()

    dt_carga = dt_carga.strftime("%Y-%m-%d %H:%M:%S")

    df['DT_CARGA'] = dt_carga

    df['ID'] = df['ID'].astype(int)

    df['PUBYEAR'] = df['PUBYEAR'].astype(int)

    df['PAGE_COUNT'] = df['PAGE_COUNT'].astype(int)

    df['EARLY_ACCESS_YEAR'] = df['EARLY_ACCESS_YEAR'].apply(lambda row: row.split('.')[0]  if row != '' else None)

    df.rename(columns={'ID': 'ID_WOS'}, inplace=True)

    return df
    

if __name__ == '__main__':
    
    os.chdir(os.path.join(os.getcwd(), '..'))

    sys.path.insert(0, os.getcwd())

    from conf import * 
    
    from functions import connection, ingestao_bd, converte_tipos
        
    init = time.time()

    coluna = 'sd_summary_pub_info'

    caminho = f'{pasta_dados}/{coluna[3:]}/arquivos_wos_{coluna}.parquet'

    tabela = pd.read_parquet(caminho)
    
    tabela = df_wos_sd_sum_pub_info(tabela)
    
    table_name = 'WOS_SD_SUM_PUB_INFO'
    
    query_cria_tabela = f"""
                            CREATE TABLE {table_name} (
                                                        "ID_WOS" NUMBER,
                                                        "VOL" VARCHAR2(50), 
                                                        "ISSUE" VARCHAR2(50), 
                                                        "PUBTYPE" VARCHAR2(50), 
                                                        "PUBYEAR" NUMBER, 
                                                        "PUBMONTH" VARCHAR2(50), 
                                                        "COVERDATE" VARCHAR2(50), 
                                                        "HAS_ABSTRACT" VARCHAR2(50), 
                                                        "JOURNAL_OAS_GOLD" VARCHAR2(50), 
                                                        "HAS_CITATION_CONTEXT" VARCHAR2(50), 
                                                        "PART_NO" VARCHAR2(50), 
                                                        "SUPPLEMENT" VARCHAR2(50), 
                                                        "SPECIAL_ISSUE" VARCHAR2(50), 
                                                        "CONTENT_PAGES" VARCHAR2(50), 
                                                        "PAGE_COUNT" NUMBER, 
                                                        "SORTDATE" VARCHAR(10), 
                                                        "EARLY_ACCESS_YEAR" NUMBER, 
                                                        "EARLY_ACCESS_DATE" VARCHAR(10), 
                                                        "EARLY_ACCESS_MONTH" VARCHAR(10),
                                                        "DT_CARGA" VARCHAR2(30))"""

    
    try:
        
        ingestao_bd(tabela, table_name, query_cria_tabela)
        
        print('Ingestão efetuada com sucesso')
    
    except Exception as e:
        
        print(f'Problema na ingestão da tabela {table_name}. Erro {e}')
    
    final = time.time()

    print(f'tempo total de execução {final - init} s')

