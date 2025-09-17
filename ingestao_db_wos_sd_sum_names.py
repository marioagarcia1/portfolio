#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import pandas as pd
import cx_Oracle
import time
import os
import sys
import os
import datetime as dt

def deleta_tabela(table_name,query_cria_tabela):
    
    conn = connection(servidor_oracle,
                      sid,
                      port,
                      user_oracle,
                      password_oracle)
    
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM user_tables WHERE table_name = '{table_name.upper()}'")
    
    table_exists = cursor.fetchone()[0] > 0
    
    if table_exists:
        
        cursor.execute(f'DROP TABLE {table_name}')
        
        conn.commit()
        
        print('Tabela excluída com sucesso!!!')
    
    cursor.execute(query_cria_tabela)
    
    print('Tabela criada com sucesso!!!')
    
    conn.close()
    
        
def df_wos_sd_summary_names(df):
    
    df = df.astype(str)
    
    cols = [i.upper().replace('|','_').replace('-','_') for i in df.columns]
    
    df.columns = cols

    select_columns =   ['ID',
                        'R_ID',
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
                        'DATA_ITEM_IDS',
                        'PREFERRED_NAME_FULL_NAME',
                        'PREFERRED_NAME_LAST_NAME',
                        'PREFERRED_NAME_FIRST_NAME',
                        'PREFERRED_NAME_MIDDLE_NAME',
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
    df['DAISNG_ID'] = df['DAISNG_ID'].astype(int)
    df['NONCORE_ENDYEAR'] = df['NONCORE_ENDYEAR'].astype(int)
    df['NONCORE_STARTYEAR'] = df['NONCORE_STARTYEAR'].astype(int)

    df.rename(columns={'ID': 'ID_WOS'}, inplace=True)

    return df


if __name__ == '__main__':
    
    os.chdir(os.path.join(os.getcwd(), '..'))

    sys.path.insert(0, os.getcwd())

    from conf import * 
    
    from functions import connection, ingestao_bd, converte_tipos
        
    init = time.time()    

    coluna = 'sd_summary_names'

    arquivos = os.listdir(f'{pasta_dados}/{coluna[3:]}')
    
    table_name = 'WOS_SD_SUM_NAME'
    
    query_cria_tabela = f"""
                                CREATE TABLE {table_name} (ID_WOS NUMBER,
                                                            R_ID VARCHAR2(50),
                                                            ROLE VARCHAR2(50),
                                                            SEQ_NO NUMBER,
                                                            ADDR_NO VARCHAR2(500),
                                                            REPRINT VARCHAR2(10),
                                                            ORCID_ID VARCHAR2(50),
                                                            DAISNG_ID NUMBER,
                                                            FULL_NAME VARCHAR2(1000),
                                                            LAST_NAME VARCHAR2(400),
                                                            FIRST_NAME VARCHAR2(400),
                                                            CLAIM_STATUS VARCHAR2(10),
                                                            DISPLAY_NAME VARCHAR2(1000),
                                                            WOS_STANDARD VARCHAR2(1000),
                                                            NONCORE_ENDYEAR NUMBER,
                                                            NONCORE_STARTYEAR NUMBER,
                                                            DATA_ITEM_IDS VARCHAR2(250),
                                                            PREFERRED_NAME_FULL_NAME VARCHAR2(250),
                                                            PREFERRED_NAME_LAST_NAME VARCHAR2(250),
                                                            PREFERRED_NAME_FIRST_NAME VARCHAR2(250),
                                                            PREFERRED_NAME_MIDDLE_NAME VARCHAR2(250),
                                                            SUFFIX VARCHAR2(100),
                                                            DT_CARGA VARCHAR2(30))"""
    
    # deleta_tabela(table_name, query_cria_tabela)
    
    for arquivo in arquivos:
        
        init2 = time.time()

        tabela = pd.read_parquet(f'{pasta_dados}/{coluna[3:]}/{arquivo}')

        tabela = df_wos_sd_summary_names(tabela)
            
        try:

            ingestao_bd(tabela, table_name, query_cria_tabela)

            print(f'Ingestão do arquivo {arquivo} efetuada com sucesso')

        except Exception as e:

            print(f'Problema na ingestão do arquivo {arquivo}. Erro {e}')
               
        final2 = time.time()

        print(f'tempo total de execução arquivo {arquivo}: {final2 - init2} s. Aguardando 10 s para processar o novo arquivo')
        
        time.sleep(10)
        

    final = time.time()

    print(f'tempo total de execução arquivo {arquivo}: {final - init} s')

