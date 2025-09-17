#!/usr/bin/env python
# coding: utf-8

# In[1]:


from functions import executa_script
import os
import subprocess
import time
from datetime import date
import pandas as pd

diretorio = 'ingestao_db'

init = time.time()

log_exec = executa_script(diretorio)

end = time.time()

print(f'tempo total de execução: {end - init} s')

log_final = []

data_processamento = date.today()

for i in log_exec:
    
    if 'ERRO' in i[0].upper() or 'PROBLEMA' in i[0].upper() or i[1] != '':
        
        result = {'data processamento': data_processamento.strftime("%Y-%m-%d %H:%M:%S") ,
                  'arquivo': i[2].split(':')[1],
                  'status': 'ERRO',
                  'log': f"{i[0]}   {i[1]}"}
    else:
        
        result = {'data processamento': data_processamento.strftime("%Y-%m-%d %H:%M:%S") ,
                  'arquivo': i[2].split(':')[1],
                  'status': 'SUCESSO',
                  'log': f"ARQUIVO PROCESSADO COM SUCESSO!!!"}
        
    log_final.append(result)

os.chdir(os.path.join(os.getcwd(), '..'))

df = pd.DataFrame(log_final)

df.to_csv(f"logs/log_ingestion_execucao_wos_{data_processamento.strftime('%Y%m%d%H%M%S')}.csv", sep = ';', index = False)

