#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import cx_Oracle
import time
import sys
import os
import datetime as dt

def df_wos_sd_fullrecord_metadata_addresses_nm(df):
    
    df = df.astype(str)

    cols = [i.upper().replace('-','_') for i in df.columns]
    
    df.columns = cols
    
    select_columns =   ['ID',
                        'ROLE',
                        'SEQ_NO',
                        'ADDR_NO',
                        'REPRINT',
                        'ORCID_ID',
                        'DAISNG_ID',
                        'FULL_NAME',
                        'LAST_NAME',
                        'FIRST_NAME',
                        'CLAIM_STATUS',
                        'DISPLAY_NAME',
                        'WOS_STANDARD',
                        'NONCORE_ENDYEAR',
                        'NONCORE_STARTYEAR',
                        'DATA_ITEM_ID',
                        'MIDDLE_NAME',
                        'SUFFIX']

    df = df[select_columns]
    
    df = df.drop_duplicates()
    
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
    
    from functions import connection, ingestao_bd, converte_tipos, deleta_tabela
        
    init = time.time()

    coluna = 'sd_fullrecord_metadata_addresses'
    
    arquivos = [arquivo for arquivo in os.listdir(f'{pasta_dados}/{coluna[3:]}') if '_nm.parquet' in arquivo]
    
    table_name = 'WOS_SD_FR_ADD_NM'
    
    query_cria_tabela = f"""
                            CREATE TABLE {table_name} (
                                                        ID_WOS NUMBER,
                                                        ROLE VARCHAR2(50),
                                                        SEQ_NO NUMBER,
                                                        ADDR_NO NUMBER,
                                                        REPRINT VARCHAR2(10),
                                                        ORCID_ID VARCHAR2(50),
                                                        DAISNG_ID VARCHAR2(50),
                                                        FULL_NAME VARCHAR2(1000),
                                                        LAST_NAME VARCHAR2(400),
                                                        FIRST_NAME VARCHAR2(400),
                                                        CLAIM_STATUS VARCHAR2(50),
                                                        DISPLAY_NAME VARCHAR2(1000),
                                                        WOS_STANDARD VARCHAR2(400),
                                                        NONCORE_ENDYEAR VARCHAR2(50),
                                                        NONCORE_STARTYEAR VARCHAR2(50),
                                                        DATA_ITEM_ID VARCHAR2(1000),
                                                        MIDDLE_NAME VARCHAR2(50),
                                                        SUFFIX VARCHAR2(50),
                                                        DT_CARGA VARCHAR2(30))"""
    
    # deleta_tabela(table_name, query_cria_tabela)
    
    for arquivo in arquivos:
        
        init2 = time.time()
        
        tabela = pd.read_parquet(f'{pasta_dados}/{coluna[3:]}/{arquivo}')
    
        tabela = df_wos_sd_fullrecord_metadata_addresses_nm(tabela)
       
        try:

            ingestao_bd(tabela, table_name, query_cria_tabela)

            print('Ingestão efetuada com sucesso')

        except Exception as e:

            print(f'Problema na ingestão da tabela {table_name}. Erro {e}')
            
        final2 = time.time()

        print(f'tempo total de execução arquivo {arquivo}: {final2 - init2} s. Aguardando 10 s para processar o novo arquivo')
        
        time.sleep(10)
    
    final = time.time()

    print(f'tempo total de execução {final - init} s')

