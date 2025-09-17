#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import cx_Oracle
import time
import sys
import os
import datetime as dt

def df_wos_sd_fullrecord_metadata_languages(df):
    
    df = df.astype(str)

    cols = [i.upper().replace('-','_') for i in df.columns]
    
    df.columns = cols
    
    select_columns =   ['ID','TYPE', 'CONTENT'] 
    
    df = df[select_columns]
    
    df = df.drop_duplicates()
    
    df = df.replace({'None':''})

    df = df.replace({'nan':''})

    dt_carga = dt.datetime.now()

    dt_carga = dt_carga.strftime("%Y-%m-%d %H:%M:%S")

    df['DT_CARGA'] = dt_carga
    
    df['ID'] = df['ID'].astype(int)

    df.rename(columns={'ID': 'ID_WOS'}, inplace=True)

    return df
    

if __name__ == '__main__':
    
    os.chdir(os.path.join(os.getcwd(), '..'))

    sys.path.insert(0, os.getcwd())

    from conf import * 
    
    from functions import connection, ingestao_bd, converte_tipos
        
    init = time.time()

    coluna = 'sd_fullrecord_metadata_normalized_languages'

    caminho = f'{pasta_dados}/{coluna[3:]}/arquivo_wos_{coluna}.parquet'

    tabela = pd.read_parquet(caminho)
    
    tabela = df_wos_sd_fullrecord_metadata_languages(tabela)
    
    table_name = 'WOS_SD_FR_NORM_LANG'
    
    query_cria_tabela = f"""
                            CREATE TABLE {table_name} (
                                                        "ID_WOS" NUMBER, 
                                                        "TYPE" VARCHAR2(50),
                                                        "CONTENT" VARCHAR2(250),
                                                        "DT_CARGA" VARCHAR2(30))"""
    
    
    try:
        
        ingestao_bd(tabela, table_name, query_cria_tabela)
        
        print('Ingestão efetuada com sucesso')
    
    except Exception as e:
        
        print(f'Problema na ingestão da tabela {table_name}. Erro {e}')
    
    final = time.time()

    print(f'tempo total de execução {final - init} s')

