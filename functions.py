

# In[ ]:


import pandas as pd
from conf import * 
import cx_Oracle
import time
import os
import subprocess
import numpy as np
from datetime import datetime

def connection(server, sid, port, user, password):
    
    dsn_tns = cx_Oracle.makedsn(host = server,
                                port = port,
                                sid  = sid)
            
    conn = cx_Oracle.connect(dsn=dsn_tns, 
                             user=user,
                             password = password)
    
    return conn

def converte_tipos_ingestao(tupla):

    tuple_list_fix = []

    for i in tupla:

        registro = []

        for j in i:

            if isinstance(j,np.int32):

                registro.append(int(j))
                
            else:

                registro.append(j)

        tuple_list_fix.append(tuple(registro))

    return tuple_list_fix

def ingestao_bd(df, table_name, query_create_table):
    
    records = df.to_records(index=False)

    colunas = '("'+'","'.join(df.columns) + '")'

    tuple_list = [tuple(record) for record in records]

    tuple_list = converte_tipos_ingestao(tuple_list)
    
    lista_valores = ' ,'.join([f':{tuple_list[0].index(i) + 1}' for i in tuple_list[0]])
    
    lista_valores = f'({lista_valores})'
        
    conn = connection(servidor_oracle,
                      sid,
                      port,
                      user_oracle,
                      password_oracle)
    
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM user_tables WHERE table_name = '{table_name.upper()}'")
    
    table_exists = cursor.fetchone()[0] > 0

    # excessoes = ['WOS_SD_SUM_NAME_2', 
    #              'WOS_SD_FR_ADD_NM2', 
    #              'WOS_SD_FR_ADD_ADD2', 
    #              'WOS_SD_FR_RPT_ADD_ADD2', 
    #              'WOS_SD_FR_RPT_ADD_NM2']
    
    if not table_exists:

        cursor.execute(query_create_table)
    
    # if table_exists and  table_name not in excessoes:
        
    #     cursor.execute(f'DROP TABLE {table_name}')
        
    #     conn.commit()
        
    #     print(f'tabela deletada {table_name}!!!')
    
    # if table_name not in excessoes:
        
    #     cursor.execute(query_create_table)
        
    batch_size = 5000  # Tamanho do lote


    
    for i in range(0, len(tuple_list), batch_size):
        
        batch = tuple_list[i:i+batch_size]

        drop_values = [(int(i[0]),) for i in batch]

        drop_values = list(set(drop_values))
        
        tentativas = 0
        
        while True:
            
            try:
                
                # cursor.executemany(f"delete from {table_name} where ID_WOS = :1", drop_values)

                cursor.executemany(f"insert into {table_name} {colunas} values {lista_valores}", batch)

                conn.commit()

                print(f'inserido {i+batch_size} registros!!!')
                
                break
            
            except Exception as e:
                
                tentativas += 1
                
                print(f'Tentativa {tentativas} de inserir o range {i + batch_size}. Erro: {e}')
                
                time.sleep(5)
                
                if tentativas == 6:
                    
                    print(f'Problema ao inserir o range {i + batch_size}')
                    
                    break

    conn.close()
    
def converte_tipos(colunas, tabela):
    
    conn = connection(servidor_oracle,
                      sid,
                      port,
                      user_oracle,
                      password_oracle)
    
    cursor = conn.cursor()
    
    for col in colunas:
        
        try:
        
            cursor.execute(f"ALTER TABLE {tabela} ADD {col[0]}2 {col[2]}")

            cursor.execute(f"UPDATE {tabela} SET {col[0]}2 = TO_{col[2]}({col[0]},'{col[1]}')")

            cursor.execute(f"ALTER TABLE {tabela} DROP COLUMN {col[0]}")

            cursor.execute(f"ALTER TABLE {tabela} RENAME COLUMN {col[0]}2 TO {col[0]}")

            conn.commit()
            
            print(f"coluna {col[0]} modificada!!!")
            
        except Exception as e:
            
            print(f"Erro modificar a coluna {col[0]}. Erro {e}")
        
    
    conn.close()

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


def executa_script(diretorio):

    os.chdir(diretorio)

    arquivos = [f"python {file}" for file in os.listdir(os.getcwd())]
    
    arquivos_cp = arquivos.copy()

    arquivos_aux = []
    
    for i in arquivos_cp:
    
        if '_add_add' in i or '_sum_names' in i or '_tratado' in i:
        
            aux = arquivos.pop(arquivos.index(i))
            
            arquivos_aux.append(aux)
    
    arquivos.extend(arquivos_aux)
    
    logs = []

    for arquivo in arquivos:
        print(f"Executando o comando: {arquivo}")
        process = subprocess.Popen(arquivo, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell = True)
        saida = process.communicate() + (f'arquivo: {arquivo[7:]}',)
        process.wait()
        print(f"Execução concluída!!!")
        logs.append(saida)
        
    print('arquivos executados')
    
    return logs
