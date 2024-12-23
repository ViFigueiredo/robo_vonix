import requests # type: ignore
import json
import pyodbc # type: ignore
import os
from dotenv import load_dotenv # type: ignore
import schedule # type: ignore
import time

# Carrega as variáveis do arquivo .env
load_dotenv()

# Configuração do SQL Server
db_server = os.getenv('db_server')
db_name = os.getenv('db_name')
db_username = os.getenv('db_username')
db_password = os.getenv('db_password')
table_name = os.getenv('table_name')

# Função para converter valores numéricos em texto
def number_to_text(value):
    return str(value)

def executar_script():
    # Conexão com o SQL Server
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db_name};UID={db_username};PWD={db_password}')
    cursor = conn.cursor()
    
    pagina = 1 # Paginação da api
    total_registros_baixados = 0

    headers = {
        'Content-Type': 'application/json',
        'api-key': os.getenv('api_key')
    }

    # Loop para consultar a API e inserir no banco
    while True:
        # Endpoint da API
        url = f"https://grupoavantti.api.vonixcc.com.br/cc/summaries/calls?timestamp=1730419200&page={pagina}"

        print(f"\nConsultando a página {pagina}...")
        response = requests.post(url, headers=headers)
        
        print(f"Status da resposta: {response.status_code}")
        if response.status_code != 200:
            print(f"Erro ao consultar API. Código de status: {response.status_code}")
            print("Resposta completa:")
            print(response.text)
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON:")
            print(response.text)
            break

        # Pega o total de registros e os resultados da página
        total_registros_api = data.get("total", 0)
        resultados = data["meta"].get("count")
        
        if not resultados:
            print(f"Nenhum dado encontrado na página {pagina}. Finalizando...")
            break

        print(f"Dados encontrados na página {pagina}: {len(resultados)} registros.")

        # Adiciona o número de registros encontrados na página atual ao total
        total_registros_baixados += len(resultados)

        for registro in resultados:
            values = (
                registro.get("cnpj"),
            )

            # Tentar inserir os valores no SQL Server com tratamento de erro
            try:
                cursor.execute(f"""
                    INSERT INTO {table_name} (
                        cnpj,
                    ) VALUES (?)
                    """, values)
                conn.commit()
            except pyodbc.DataError as e:
                print(f"\nErro ao tentar inserir os dados na tabela '{table_name}'")
                print(f"Dados: {values}")
                print(f"Detalhes do erro: {e}")

        # Se o número total de registros baixados for igual ao total informado pela API, interrompa
        if total_registros_baixados >= total_registros_api:
            print(f"Todos os {total_registros_api} registros foram baixados.")
            break

        pagina += 1

    conn.close()
    print("Processamento concluído.")

# executar_script()

# Agendar a execução diária às 21h
srtTime = "21:00"
schedule.every().day.at(srtTime).do(executar_script)

# Loop para manter o script em execução
print(f"Agendamento iniciado. Aguardando para executar às {srtTime} diariamente...")
while True:
    schedule.run_pending()
    time.sleep(1)