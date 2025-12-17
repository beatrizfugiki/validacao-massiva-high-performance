import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas_gbq
import os
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÃO E CONSTANTES
# ==============================================================================

# Data de Referência para execução
DATE_REF = "2025-07-24"
data = datetime.strptime(DATE_REF, "%Y-%m-%d")
YYMM = data.strftime("%y%m")  # Formato: AAMM

# Configurações do Google Cloud Platform (GENÉRICAS)
PROJECT_ID = "your-gcp-project-id" 
DATASET_ID = "production_dataset"

# Tabela de Destino no BigQuery (Output)
DESTINATION_TABLE = f"{PROJECT_ID}.{DATASET_ID}.ACCESS_VALIDATION_OUTPUT"

# Diretório local para salvar backup (Cria uma pasta 'results' onde o script rodar)
OUTPUT_DIR = os.path.join(os.getcwd(), 'results')
os.makedirs(OUTPUT_DIR, exist_ok=True)
CSV_OUTPUT_PATH = os.path.join(OUTPUT_DIR, f'validation_results_{DATE_REF}.csv')

# URL do site alvo para validação (Exemplo Genérico)
BASE_URL_TEMPLATE = "https://api.target-site-example.com/check-offers/{}"

print(f"--- Iniciando Processo de Validação Massiva: {YYMM} ---")

# ==============================================================================
# 1. Identificação Dinâmica da Tabela de Origem
# ==============================================================================
print(f"Buscando tabela de origem no BigQuery para o período '{YYMM}'...")

try:
    # Query nos metadados para encontrar a tabela correta do mês
    schema_query = f"""
    SELECT table_name
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'CAMPAIGN_BASE_{YYMM}_%' 
      AND table_name LIKE '%_TARGET_AUDIENCE'
    """
    
    tables_df = pandas_gbq.read_gbq(schema_query, project_id=PROJECT_ID)

    if len(tables_df) == 0:
        print(f"ERRO: Nenhuma tabela encontrada com o padrão esperado para {YYMM}.")
        exit()
    
    table_name = tables_df['table_name'].iloc[0]
    source_table_full_path = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    print(f"Tabela de origem identificada: {source_table_full_path}")

except Exception as e:
    print(f"ERRO CRÍTICO na descoberta da tabela: {e}")
    exit()

# ==============================================================================
# 2. ETL: Extração e Tratamento de Dados
# ==============================================================================

try:
    # Query SQL: Join da base diária com histórico de clientes 
    # Nomes de colunas e tabelas foram anonimizados para proteção de dados.
    query = f"""
    WITH recent_customers AS (
      -- Seleciona o ID de cliente mais recente para cada documento nas tabelas históricas
      SELECT
        customer_id,      -- ID Interno do Cliente
        document_id,      -- CPF/CNPJ
        phone_number,     -- Número do Telefone
        ROW_NUMBER() OVER(PARTITION BY document_id, phone_number ORDER BY _TABLE_SUFFIX DESC) as rn
      FROM
        `{PROJECT_ID}.{DATASET_ID}.CUSTOMERS_HISTORY_202*`
      WHERE
        LENGTH(_TABLE_SUFFIX) = 3
    )
    SELECT
        base.phone_number AS NUM_TELEFONE,
        base.document_id AS CPF,
        base.segment_name AS SEGMENTO,
        recentes.customer_id AS CUSTCODE
    FROM
        `{source_table_full_path}` AS base
    LEFT JOIN
        (SELECT * FROM recent_customers WHERE rn = 1) AS recentes
    ON
        base.document_id = recentes.document_id
        AND base.phone_number = recentes.phone_number
    """
    
    print("\nExecutando query de extração no BigQuery...")
    df_from_bq = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
    print(f"Linhas carregadas: {len(df_from_bq)}")

    # Limpeza de Dados 
    # Remove caracteres não numéricos do telefone
    df_from_bq['NUM_TELEFONE'] = df_from_bq['NUM_TELEFONE'].astype(str).str.replace(r'\D', '', regex=True)
    
    # Seleciona colunas de interesse
    df_processed = df_from_bq[['NUM_TELEFONE', 'CPF', 'CUSTCODE', 'SEGMENTO']].copy()

except Exception as e:
    print(f"ERRO durante o processo de ETL: {e}")
    exit()

# Remove duplicatas para evitar chamadas redundantes à API
df_unique = df_processed.drop_duplicates(subset=['NUM_TELEFONE']).dropna(subset=['NUM_TELEFONE'])
print(f"\nRegistros únicos para processar: {len(df_unique)}")

# ==============================================================================
# 3. LÓGICA DE VALIDAÇÃO (Requisições HTTP)
# ==============================================================================

def verify_access(phone, cpf, custcode, segment):
    """
    Verifica se um número específico possui oferta disponível via requisição HTTP.
    Retorna o status e os metadados do cliente.
    """
    url = BASE_URL_TEMPLATE.format(phone)
    headers = {
        "User-Agent": "Mozilla/5.0 (Compatible; Python Bot/1.0)"
    }
    
    flag_available = None
    
    try:
        # Usa HEAD request para performance (baixa apenas o cabeçalho)
        response = requests.head(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            flag_available = 1 # Oferta Disponível
        elif response.status_code == 302:
            flag_available = 0 # Redirecionamento (Sem Oferta)
        else:
            flag_available = f"Status Inesperado ({response.status_code})"
            
    except requests.RequestException as e:
        flag_available = f"Erro de Requisição: {str(e)}"
        
    return {
        'NUM_TELEFONE': phone,
        'FLAG_OFERTA_DISPONIVEL': flag_available,
        'CPF': cpf,
        'CUSTCODE': custcode,
        'SEGMENTO': segment
    }

# ==============================================================================
# 4. EXECUÇÃO PARALELA (Multithreading)
# ==============================================================================

results = []
MAX_WORKERS = 50 # Ajustar conforme capacidade de CPU e Rede

print(f"\nIniciando validação paralela com {MAX_WORKERS} workers...")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Mapeia futures para rastrear execução
    futures = {
        executor.submit(verify_access, row['NUM_TELEFONE'], row['CPF'], row['CUSTCODE'], row['SEGMENTO']): row
        for index, row in df_unique.iterrows()
    }
    
    # Itera conforme as threads concluem, mostrando barra de progresso
    for future in tqdm(as_completed(futures), total=len(futures), desc="Validando Acessos"):
        results.append(future.result())

# ==============================================================================
# 5. EXPORTAÇÃO E CARGA (Load)
# ==============================================================================

df_final_results = pd.DataFrame(results)
df_final_results['DATA_VERIFICACAO'] = DATE_REF

# 5.1 Backup Local
print(f"\nSalvando backup local em: {CSV_OUTPUT_PATH}")
df_final_results.to_csv(CSV_OUTPUT_PATH, index=False, sep=';', encoding='utf-8')

# 5.2 Upload para Nuvem (BigQuery)
print("Enviando resultados processados para o BigQuery...")
try:
    pandas_gbq.to_gbq(
        df_final_results,
        DESTINATION_TABLE,
        project_id=PROJECT_ID,
        if_exists="append"
    )
    print(f"Sucesso! Dados anexados à tabela {DESTINATION_TABLE}.")
except Exception as e:
    print(f"Falha no upload para nuvem: {e}")


print("\n--- Processo Finalizado ---")
