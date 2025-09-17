#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import cx_Oracle
import time
import sys
import os
import datetime as dt


def df_wos_sd_fullrecord_metadata_reprint_addresses_address(df):

    cols = [i.upper() for i in df.columns]
    
    df.columns = cols
    
    select_columns =   ['ID',
                        'CITY',
                        'STATE',
                        'ADDR_NO',
                        'COUNTRY',
                        'FULL_ADDRESS',
                        'ZIP_CONTENT',
                        'ZIP_LOCATION',
                        'SUBORGANIZATIONS',
                        'NM_PREFERENCIAL',
                        'NM_N_PREFERENCIAL',
                        'STREET']

    df = df[select_columns]
    
    df = df.astype(str)

    df = df.drop_duplicates()

    df = df.replace({'None':''})

    df = df.replace({'nan':''})

    dt_carga = dt.datetime.now()

    dt_carga = dt_carga.strftime("%Y-%m-%d %H:%M:%S")

    df['DT_CARGA'] = dt_carga

    df['ID'] = df['ID'].astype(int)

    df['ADDR_NO'] = df['ADDR_NO'].astype(int)

    df.rename(columns={'ID': 'ID_WOS'}, inplace=True)

    return df

if __name__ == '__main__':
    
    os.chdir(os.path.join(os.getcwd(), '..'))

    sys.path.insert(0, os.getcwd())

    from conf import * 
    
    from functions import connection, ingestao_bd, converte_tipos, deleta_tabela
        
    init = time.time()

    coluna = 'sd_fullrecord_metadata_reprint_addresses'
    
    arquivos = [arquivo for arquivo in os.listdir(f'{pasta_dados}/{coluna[3:]}') if '_address.parquet' in arquivo]
    
    table_name = 'WOS_SD_FR_RPT_ADD_ADD'
    
    query_cria_tabela = f"""
                        CREATE TABLE {table_name} (
                                                    ID_WOS NUMBER,
                                                    CITY VARCHAR2(500),
                                                    STATE VARCHAR2(500),
                                                    ADDR_NO NUMBER,
                                                    COUNTRY VARCHAR2(500),
                                                    FULL_ADDRESS VARCHAR2(500),
                                                    ZIP_CONTENT VARCHAR2(500),
                                                    ZIP_LOCATION VARCHAR2(500),
                                                    SUBORGANIZATIONS VARCHAR2(500),
                                                    NM_PREFERENCIAL VARCHAR2(500),
                                                    NM_N_PREFERENCIAL VARCHAR2(500),
                                                    STREET VARCHAR2(500),
                                                    DT_CARGA VARCHAR2(30))"""
    
    # deleta_tabela(table_name, query_cria_tabela)

    for arquivo in arquivos:
        
        init2 = time.time()
        
        tabela = pd.read_parquet(f'{pasta_dados}/{coluna[3:]}/{arquivo}')

        tabela = df_wos_sd_fullrecord_metadata_reprint_addresses_address(tabela)
    
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
