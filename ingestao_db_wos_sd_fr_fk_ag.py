#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import cx_Oracle
import time
import sys
import os
import datetime as dt

def wos_sd_fullrecord_metadata_fund_ack_grants_tratado(df):

    df['ID_WOS'] = df['ID_WOS'].astype(int)

    df['FL_VALIDA_STRING_FAPESP'] = df['FL_VALIDA_STRING_FAPESP'].astype(int)

    df['FL_VALIDA_STRING_PREF_FAPESP'] = df['FL_VALIDA_STRING_PREF_FAPESP'].astype(int)

    df['FL_GRANT_ID_FORMATS'] = df['FL_GRANT_ID_FORMATS'].astype(int)
    
    df['FL_ALGORITMO_SAGE_CRAB'] = df['FL_ALGORITMO_SAGE_CRAB'].astype(int)

    df['PUB_INFO_PUBYEAR'] = df['PUB_INFO_PUBYEAR'].astype(int)

    dt_carga = dt.datetime.now()

    dt_carga = dt_carga.strftime("%Y-%m-%d %H:%M:%S")

    df['DT_CARGA'] = dt_carga

    # df = df[(df['ID_WOS'] > 1) & (df['ID_WOS'] < 3)]    

    return df
    
if __name__ == '__main__':
    
    os.chdir(os.path.join(os.getcwd(), '..'))

    sys.path.insert(0, os.getcwd())

    from conf import * 
    
    from functions import connection, ingestao_bd, converte_tipos
        
    init = time.time()

    coluna = 'sd_fullrecord_metadata_fund_ack_grants_tratado'

    caminho = f'{pasta_dados}/{coluna[3:]}/arquivo_wos_{coluna[3:]}.parquet'

    tabela = pd.read_parquet(caminho)

    tabela = wos_sd_fullrecord_metadata_fund_ack_grants_tratado(tabela)
    
    table_name = 'WOS_SD_FR_FK_AG'
    
    query_cria_tabela = f"""
                            CREATE TABLE {table_name} (
                                                        ID_WOS NUMBER,
                                                        ACCESSION_NUMBER VARCHAR2(50),
                                                        GRANT_ID_ORI VARCHAR2(250),
                                                        GRANT_AGENCY_PREF VARCHAR2(1000),
                                                        GRANT_AGENCY_N_PREF VARCHAR2(1000),
                                                        GRANT_AGENCY_STANDARD_FAPESP VARCHAR2(1000),
                                                        GRANT_SOURCE VARCHAR2(250),
                                                        PUB_INFO_COVERDATE VARCHAR2(50),
                                                        PUB_INFO_PUBYEAR NUMBER,
                                                        DOI VARCHAR2(500),
                                                        GRANT_ID_REPLACED VARCHAR2(150),
                                                        GRANT_ID_FORMATED VARCHAR2(150),
                                                        FL_VALIDA_STRING_FAPESP NUMBER,
                                                        FL_VALIDA_STRING_PREF_FAPESP NUMBER,
                                                        FL_GRANT_ID_FORMATS NUMBER,
                                                        FL_ALGORITMO_SAGE_CRAB NUMBER,
                                                        DT_CARGA VARCHAR2(30))"""

    try:
        
        ingestao_bd(tabela, table_name, query_cria_tabela)
        
        print('Ingestão efetuada com sucesso')
    
    except Exception as e:
        
        print(f'Problema na ingestão da tabela {table_name}. Erro {e}')
    
    
    final = time.time()

    print(f'tempo total de execução {final - init} s')

