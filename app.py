import requests  # type: ignore
import json
import pyodbc  # type: ignore
import os
from dotenv import load_dotenv  # type: ignore
import schedule  # type: ignore
import time

# Carrega as variáveis do arquivo .env
load_dotenv()

# Validação das variáveis de ambiente
REQUIRED_ENV_VARS = ['db_server', 'db_name', 'db_username', 'db_password', 'table_name', 'api_key']
for var in REQUIRED_ENV_VARS:
    if not os.getenv(var):
        raise ValueError(f"Variável de ambiente obrigatória '{var}' não está definida.")

# Configuração do SQL Server
db_server = os.getenv('db_server')
db_name = os.getenv('db_name')
db_username = os.getenv('db_username')
db_password = os.getenv('db_password')
table_name = os.getenv('table_name')
api_key = os.getenv('api_key')

# Função principal
def executar_script():
    try:
        conn = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db_name};UID={db_username};PWD={db_password}'
        )
        cursor = conn.cursor()
    except pyodbc.Error as e:
        print(f"Erro ao conectar ao SQL Server: {e}")
        return
    
    timestamp = 1730419200,
    pagina = 1  # Paginação da API
    total_registros_baixados = 0

    while True:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }

        params = {
            'timestamp': timestamp,
            'page': pagina
        }

        url = f"https://grupoavantti.api.vonixcc.com.br/cc/summaries/calls"

        # print(headers['Authorization'])

        print(f"\nConsultando a página {pagina}...")

        try:
            response = requests.get(url, headers=headers, params=params )
        except requests.RequestException as e:
            print(f"Erro ao realizar requisição para a API: {e}")
            break

        print(f"Status da resposta: {response.status_code}")

        if response.status_code != 200:
            print(f"Erro na API. Código de status: {response.status_code}")
            print("Resposta completa:", response.text)
            break

        try:
            data = response.json()

        except json.JSONDecodeError:
            print("Erro ao decodificar JSON:", response.text)
            break

        total_registros_api = data.get("meta", {}).get("count", 0)
        resultados = data.get("data")
        
        if not resultados:
            print(f"Nenhum dado encontrado na página {pagina}. Finalizando...")
            break

        print(f"Dados encontrados: {resultados} registros.")

        total_registros_baixados += len(resultados)

        for registro in data.get("data", []):
            values = (
                registro.get("id"),
                registro.get("direction"),
                registro.get("callerNumber"),
                registro.get("callerInfo"),
                registro.get("status"),
                registro.get("reason"),
                registro.get("ani"),
                registro.get("ringSecs"),
                registro.get("holdSecs"),
                registro.get("talkSecs"),
                registro.get("queueId"),
                registro.get("queueName"),
                registro.get("agentId"),
                registro.get("agentName"),
                registro.get("trunkingId"),
                registro.get("trunkingName"),
                registro.get("localityId"),
                registro.get("callTypeId"),
                registro.get("callTypeName"),
                registro.get("hangupCauseId"),
                registro.get("createdAt"),
                registro.get("answerAt"),
                registro.get("hangupAt"),
                registro.get("agentOffers"),
                registro.get("initialPosition"),
                registro.get("abandonKey"),
                registro.get("abandonPosition"),
                registro.get("transferredTo"),
                registro.get("bridgedCallId"),
                registro.get("dnid"),
                registro.get("ivrSecs"),
                registro.get("uraId"),
                registro.get("nodeId"),
                # registro.get("callsRatings"),
                # registro.get("profilers"),
                # registro.get("trees"),
                # registro.get("tags"),
                registro.get("profiledAt"),
                # registro.get("contact")
            )

            try:
                cursor.execute(f"""
                    INSERT INTO {table_name} (
                        id, direction, callerNumber, callerInfo, status, reason, ani,
                        ringSecs, holdSecs, talkSecs, queueId, queueName, agentId,
                        agentName, trunkingId, trunkingName, localityId, callTypeId,
                        callTypeName, hangupCauseId, createdAt, answerAt, hangupAt,
                        agentOffers, initialPosition, abandonKey, abandonPosition,
                        transferredTo, bridgedCallId, dnid, ivrSecs, uraId, nodeId,
                        profiledAt
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, values)
                conn.commit()
            except pyodbc.DataError as e:
                print(f"Erro ao inserir dados na tabela '{table_name}': {e}")

        if total_registros_baixados >= total_registros_api:
            print(f"Todos os {total_registros_api} registros foram baixados.")
            break

        pagina += 1

    conn.close()

    print("Processamento concluído.")

executar_script()

# Agendar execução
# srtTime = "08:42"
# schedule.every().day.at(srtTime).do(executar_script)

# print(f"Agendamento iniciado. Aguardando para executar às {srtTime} diariamente...")
# while True:
#     schedule.run_pending()
#     time.sleep(60)
