#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import cx_Oracle
import time
import sys
import os
import datetime as dt

def df_wos_sd_sum_publishers(df):
    

    rename_columns = {'ROLE'.lower():'ATTRIBUTION',
                      'SEQ_NO'.lower():'SEQ_NO',
                      'address_spec|addr_no'.lower():'ADDR_NO',
                      'FULL_NAME'.lower():'FULL_NAME',
                      'DISPLAY_NAME'.lower():'DISPLAY_NAME',
                      'UNIFIED_NAME'.lower():'UNIFIED_NAME',
                      'address_spec|city'.lower():'CITY',
                      'address_spec|full_address'.lower():'FULL_ADDRESS',
                      'id': 'ID'}


    select_columns = ['ID',
                      'ATTRIBUTION',
                      'SEQ_NO',
                      'ADDR_NO',
                      'FULL_NAME',
                      'DISPLAY_NAME',
                      'UNIFIED_NAME',
                      'CITY',
                      'FULL_ADDRESS']

    df.rename(columns = rename_columns, inplace = True)

    df = df[select_columns]
    
    df = df.astype(str)
    
    df = df.replace({'None':''})

    df = df.replace({'nan':''})

    dt_carga = dt.datetime.now()

    dt_carga = dt_carga.strftime("%Y-%m-%d %H:%M:%S")

    df['DT_CARGA'] = dt_carga

    df['ID'] = df['ID'].astype(int)

    df['SEQ_NO'] = df['SEQ_NO'].astype(int)

    df['ADDR_NO'] = df['ADDR_NO'].astype(int)

    df.rename(columns={'ID': 'ID_WOS'}, inplace=True)

    return df

if __name__ == '__main__':
    
    os.chdir(os.path.join(os.getcwd(), '..'))

    sys.path.insert(0, os.getcwd())

    from conf import * 
    
    from functions import connection, ingestao_bd, converte_tipos
        
    init = time.time()

    coluna = 'sd_summary_publishers'

    caminho = f'{pasta_dados}/{coluna[3:]}/arquivo_wos_{coluna}.parquet'

    tabela = pd.read_parquet(caminho)
    
    tabela = df_wos_sd_sum_publishers(tabela)
    
    table_name = 'WOS_SD_SUM_PUBLISHERS'
    
    query_cria_tabela = f"""
                            CREATE TABLE {table_name} (
                                                        ID_WOS NUMBER,
                                                        ATTRIBUTION VARCHAR2(50) ,
                                                        SEQ_NO NUMBER,
                                                        ADDR_NO NUMBER,
                                                        FULL_NAME VARCHAR2(150) ,
                                                        DISPLAY_NAME VARCHAR2(150) ,
                                                        UNIFIED_NAME VARCHAR2(150) ,
                                                        CITY VARCHAR2(50) ,
                                                        FULL_ADDRESS VARCHAR2(256),
                                                        DT_CARGA VARCHAR2(30))"""

    
    try:
        
        ingestao_bd(tabela, table_name, query_cria_tabela)
        
        print('Ingestão efetuada com sucesso')
    
    except Exception as e:
        
        print(f'Problema na ingestão da tabela {table_name}. Erro {e}')
    
    final = time.time()

    print(f'tempo total de execução {final - init} s')

