#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import mysql.connector
import time
import datetime
from conf import *
import os
import cx_Oracle
from functions import *

# Conectando ao banco de dados
def abre_conexao():
        cnx = mysql.connector.connect(
                                        host=servidor,
                                        user=usuario,
                                        password=senha,
                                        database=banco,
                                        ssl_disabled=True)
        return cnx

def fecha_conexao(conexao):
    conexao.close()
    
def extrai_dados(valor, conexao):
    try:
        # Criando um cursor para executar as consultas
        cursor = conexao.cursor()

        # Executando uma consulta SELECT
        query = f"""    
                    SELECT  id,
                            jdoc -> '$.UID' as UID,
                            jdoc -> '$.r_id_disclaimer' as r_id_disclaimer,
                            jdoc -> '$.dates.date_modified' as dates_date_modified,
                            jdoc -> '$.dates.date_created' as dates_date_created,
                            jdoc -> '$.static_data.summary.pub_info' as sd_summary_pub_info,
                            jdoc -> '$.static_data.summary.names' as sd_summary_names,
                            jdoc -> '$.static_data.summary.doctypes' as sd_summary_doctypes,
                            jdoc -> '$.static_data.summary.publishers' as sd_summary_publishers,
                            jdoc -> '$.static_data.summary.EWUID' as sd_summary_EWUID,
                            jdoc -> '$.static_data.summary.titles' as sd_summary_titles,                
                            jdoc -> '$.static_data.item."xsi:type"' as sd_item_xsi_type,
                            jdoc -> '$.static_data.item.coll_id' as sd_item_coll_id,
                            jdoc -> '$.static_data.item.ids' as sd_item_ids,
                            jdoc -> '$.static_data.item."xmlns:xsi"' as sd_item_xmlns_xsi,
                            jdoc -> '$.static_data.item.bib_pagecount' as sd_item_bib_pagecount,
                            jdoc -> '$.static_data.item.keywords_plus' as sd_item_keywords_plus,
                            jdoc -> '$.static_data.item.bib_id' as sd_item_bib_id,
                            jdoc -> '$.static_data.fullrecord_metadata.addresses' as sd_fullrecord_metadata_addresses,
                            jdoc -> '$.static_data.fullrecord_metadata.category_info' as sd_fullrecord_metadata_category_info,
                            jdoc -> '$.static_data.fullrecord_metadata.normalized_languages' as sd_fullrecord_metadata_normalized_languages,
                            jdoc -> '$.static_data.fullrecord_metadata.languages' as sd_fullrecord_metadata_languages,
                            jdoc -> '$.static_data.fullrecord_metadata.keywords' as sd_fullrecord_metadata_keywords,
                            jdoc -> '$.static_data.fullrecord_metadata.refs' as sd_fullrecord_metadata_refs,
                            jdoc -> '$.static_data.fullrecord_metadata.reprint_addresses' as sd_fullrecord_metadata_reprint_addresses,
                            jdoc -> '$.static_data.fullrecord_metadata.abstracts' as sd_fullrecord_metadata_abstracts,
                            jdoc -> '$.static_data.fullrecord_metadata.fund_ack' as sd_fullrecord_metadata_fund_ack,
                            jdoc -> '$.static_data.fullrecord_metadata.normalized_doctypes' as sd_fullrecord_metadata_normalized_doctypes,
                            jdoc -> '$.static_data.contributors.count' as sd_contributors_count,
                            jdoc -> '$.static_data.contributors.contributor' as sd_contributors_contributor,
                            jdoc -> '$.dynamic_data.citation_related.tc_list_cc' as dd_citation_related_tc_list_cc,
                            jdoc -> '$.dynamic_data.citation_related.citation_topics' as dd_citation_related_citation_topics,
                            jdoc -> '$.dynamic_data.citation_related.tc_list' as dd_citation_related_tc_list,
                            jdoc -> '$.dynamic_data.cluster_related.identifiers.identifier' as dd_cluster_related_identifiers_identifier,
                            jdoc -> '$.dynamic_data.wos_usage.last180days' as dd_wos_usage_last180days,
                            jdoc -> '$.dynamic_data.wos_usage.alltime' as dd_wos_usage_alltime,
                            jdoc -> '$.dynamic_data.citation_related.SDG' as dd_citation_related_SDG
                    FROM bv.wos_json 
                    WHERE id = {valor}"""
        
        cursor.execute(query)

        dados = [row for row in cursor.fetchall()]
        
        cursor.close()
        
        return dados

    except:
        print(f"Erro na consulta do registro id = {valor}")
        
        raise

def registro_maximo(conexao):
    
    try:
        cursor = conexao.cursor()
        
        query = "SELECT max(id) as id_max FROM bv.wos_json"
        
        cursor.execute(query)
        
        maximo = [row[0] for row in cursor][0]
        
        cursor.close()
        
        return maximo

    except:
        print(f"Erro na consulta do registro máximo id")
        
        raise

def registros_atualizar(conexao, data_filtro):
    
    try:
        cursor = conexao.cursor()
        
        query = f"SELECT id FROM bv.wos_json where modified_time >= '{data_filtro}'"
        
        cursor.execute(query)
        
        maximo = [row[0] for row in cursor]
        
        cursor.close()
        
        return maximo

    except:
        print(f"Erro na consulta do registro máximo id")
        
        raise


if __name__ == '__main__':
    
    if not(os.path.exists(pasta_dados)): 
        os.mkdir(pasta_dados)
    
    inicia_conexao = abre_conexao()

    dt_ultima_carga = '2023-06-06 23:35:22'

    lista_ids_atualizar = registros_atualizar(inicia_conexao, dt_ultima_carga)

    print(f'arquivos a atualizar: {lista_ids_atualizar}')
    
    colunas = ['id',
               'UID',
               'r_id_disclaimer',
               'dates_date_modified',
               'dates_date_created',
               'sd_summary_pub_info',
               'sd_summary_names',
               'sd_summary_doctypes',
               'sd_summary_publishers',
               'sd_summary_EWUID',
               'sd_summary_titles',                
               'sd_item_xsi_type',
               'sd_item_coll_id',
               'sd_item_ids',
               'sd_item_xmlns_xsi',
               'sd_item_bib_pagecount',
               'sd_item_keywords_plus',
               'sd_item_bib_id',
               'sd_fullrecord_metadata_addresses',
               'sd_fullrecord_metadata_category_info',
               'sd_fullrecord_metadata_normalized_languages',
               'sd_fullrecord_metadata_languages',
               'sd_fullrecord_metadata_keywords',
               'sd_fullrecord_metadata_refs',
               'sd_fullrecord_metadata_reprint_addresses',
               'sd_fullrecord_metadata_abstracts',
               'sd_fullrecord_metadata_fund_ack',
               'sd_fullrecord_metadata_normalized_doctypes',                            
               'sd_contributors_count',
               'sd_contributors_contributor',
               'dd_citation_related_tc_list_cc',
               'dd_citation_related_citation_topics',
               'dd_citation_related_tc_list',
               'dd_cluster_related_identifiers_identifier',
               'dd_wos_usage_last180days',
               'dd_wos_usage_alltime',
               'dd_citation_related_SDG'
              ]
    
    batch_size = 5000  # Tamanho do lote
    
    for id in range(0, len(lista_ids_atualizar), batch_size):
        
        batch = lista_ids_atualizar[id:id+batch_size]
    
        inicio = time.time()

        for reg in batch:
            data = extrai_dados(reg,inicia_conexao)

            df = pd.DataFrame(data,columns = colunas)

            df.to_csv(f'arquivos_mod_wos_{id+batch_size}.csv',sep='|',mode='a', index=False, header=False)

        df = pd.read_csv(f'arquivos_mod_wos_{id+batch_size}.csv',sep = '|', header = None, names = colunas)

        df.to_parquet(f'{pasta_dados}/arquivos_wos_{id+batch_size}.parquet')

        os.remove(f'arquivos_mod_wos_{id+batch_size}.csv')

        fim = time.time()
        
        print(f"Tempo de execução na consulta: {fim - inicio} s")

    fecha_conexao(inicia_conexao)
# %%
