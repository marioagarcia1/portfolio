#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import cx_Oracle
import time
import sys
import os
import datetime as dt

def df_wos_sd_item(df):
    
    df = df.astype(str)
    
    cols = [i.upper().replace(':','_') for i in df.columns]
    
    df.columns = cols

    select_columns =   ['ID', 
                        'COLL_ID', 
                        'XSI_TYPE', 
                        'XMLNS_XSI', 
                        'BIB_ID', 
                        'TYPE', 
                        'BIB_PAGECOUNT_CONTENT', 
                        'KEYWORD',
                        'AVAIL',
                        'IDS_CONTENT']


    df = df[select_columns]
    
    df = df.drop_duplicates()
    
    df = df.replace({'None':'', 'nan':'', '\"': ''}, regex = True)
    
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

    coluna = 'sd_item'

    caminho = f'{pasta_dados}/{coluna[3:]}/arquivo_wos_{coluna}.parquet'

    tabela = pd.read_parquet(caminho)
    
    tabela = df_wos_sd_item(tabela)
    
    table_name = 'WOS_SD_ITEM'
    
    query_cria_tabela = f"""
                            CREATE TABLE {table_name} (
                                                        ID_WOS NUMBER,
                                                        COLL_ID VARCHAR2(250),
                                                        XSI_TYPE VARCHAR2(250),
                                                        XMLNS_XSI VARCHAR2(250),
                                                        BIB_ID VARCHAR2(250),
                                                        TYPE VARCHAR2(250),
                                                        BIB_PAGECOUNT_CONTENT VARCHAR2(250),
                                                        KEYWORD VARCHAR2(500),
                                                        AVAIL VARCHAR2(250),
                                                        IDS_CONTENT VARCHAR2(250),
                                                        DT_CARGA VARCHAR2(30))"""
        
    try:
        
        ingestao_bd(tabela, table_name, query_cria_tabela)
        
        print('Ingestão efetuada com sucesso')
    
    except Exception as e:
        
        print(f'Problema na ingestão da tabela {table_name}. Erro {e}')
    
    final = time.time()

    print(f'tempo total de execução {final - init} s')

