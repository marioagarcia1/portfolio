#!/usr/bin/env python
# coding: utf-8

# In[2]:

import pandas as pd
import cx_Oracle
import time
import sys
import os
import datetime as dt
        
def df_wos_sd_contributors(df):
    
    cols = [i.upper() for i in df.columns]
    
    df.columns = cols
    
    select_columns =   ['ID',
                        'R_ID',
                        'ROLE',
                        'SEQ_NO',
                        'ORCID_ID',
                        'FULL_NAME',
                        'LAST_NAME',
                        'FIRST_NAME',
                        'DISPLAY_NAME']
                                                     
    df = df[select_columns]
    
    df = df.drop_duplicates()
    
    df = df.astype(str)
    
    df = df.replace({'None':''})

    df = df.replace({'nan':''})

    dt_carga = dt.datetime.now()

    dt_carga = dt_carga.strftime("%Y-%m-%d %H:%M:%S")

    df['DT_CARGA'] = dt_carga

    df['ID'] = df['ID'].astype(int)
    
    df['SEQ_NO'] = df['SEQ_NO'].astype(int)

    df.rename(columns={'ID': 'ID_WOS'}, inplace=True)

    return df    

if __name__ == '__main__':
    
    os.chdir(os.path.join(os.getcwd(), '..'))

    sys.path.insert(0, os.getcwd())

    from conf import * 
    
    from functions import connection, ingestao_bd, converte_tipos
        
    init = time.time()

    coluna = 'sd_contributors'

    caminho = f'{pasta_dados}/{coluna[3:]}/arquivo_wos_{coluna}.parquet'

    tabela = pd.read_parquet(caminho)
    
    tabela = df_wos_sd_contributors(tabela)
    
    table_name = 'WOS_SD_CONTRIBUTORS'
    
    query_cria_tabela = f"""
                            CREATE TABLE {table_name} (
                                                        "ID_WOS" NUMBER, 
                                                        "R_ID" VARCHAR2(50), 
                                                        "ROLE" VARCHAR2(50), 
                                                        "SEQ_NO" NUMBER, 
                                                        "ORCID_ID" VARCHAR2(50), 
                                                        "FULL_NAME" VARCHAR2(100), 
                                                        "LAST_NAME" VARCHAR2(100), 
                                                        "FIRST_NAME" VARCHAR2(100), 
                                                        "DISPLAY_NAME" VARCHAR2(100),
                                                        "DT_CARGA" VARCHAR2(30))"""

    
    try:
        
        ingestao_bd(tabela, table_name, query_cria_tabela)
        
        print('Ingestão efetuada com sucesso')
    
    except Exception as e:
        
        print(f'Problema na ingestão da tabela {table_name}. Erro {e}')
    
    
    final = time.time()

    print(f'tempo total de execução {final - init} s')

