import requests
import json
import csv
import io
import os
import time
from requests.exceptions import Timeout

# Carregar o JSON com as cidades do Brasil
with open("municipios.json", "r", encoding="utf-8-sig") as f:
    cidades = json.load(f)

# URL da API da NASA
url = "https://power.larc.nasa.gov/api/temporal/daily/point"

# Parâmetros comuns para todas as requisições
params_comuns = {
    "parameters": "T2M,T2M_MAX,T2M_MIN,ALLSKY_SFC_UV_INDEX,ALLSKY_SFC_UVA,ALLSKY_SFC_UVB,PRECTOTCORR",
    "community": "ag",
    "start": "20010101",
    "end": "20211231",
    "format": "CSV"
}

# Nome do arquivo CSV final
arquivo_csv = "climate_raw.csv"

# Nome do arquivo de log
arquivo_log = "log_processamento.json"

# Função para carregar o log de processamento
def carregar_log():
    if os.path.exists(arquivo_log):
        with open(arquivo_log, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    return {}

# Função para salvar o log de processamento
def salvar_log(log):
    with open(arquivo_log, "w", encoding="utf-8-sig") as f:
        json.dump(log, f, ensure_ascii=False, indent=4)

# Carregar o log de processamento
log_processamento = carregar_log()

# Identificar a próxima cidade a ser processada
cidades_restantes = [cidade for cidade in cidades if str(cidade["codigo_ibge"]) not in log_processamento]

# Abrir o arquivo CSV para escrita (modo append se já existir)
modo_csv = 'a' if os.path.exists(arquivo_csv) else 'w'
with open(arquivo_csv, modo_csv, newline='', encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    primeira_cidade = modo_csv == 'w'

    # Iterar sobre as cidades restantes
    for cidade in cidades_restantes:
        codigo_ibge = cidade["codigo_ibge"]
        nome_cidade = cidade["nome"]
        latitude = cidade["latitude"]
        longitude = cidade["longitude"]

        # Atualizar os parâmetros com a latitude e longitude da cidade atual
        params = params_comuns.copy()
        params["latitude"] = latitude
        params["longitude"] = longitude

        # Fazer a requisição à API com timeout
        try:
            print(f"Processando {nome_cidade}...")
            response = requests.get(url, params=params, timeout=60)  # Timeout de 30 segundos

            # Verificar se a requisição foi bem-sucedida
            if response.status_code == 200:
                # Ler a resposta como CSV
                resposta_csv = io.StringIO(response.text)
                reader = csv.reader(resposta_csv)

                try:
                    # Obter o cabeçalho da resposta
                    cabeçalho = next(reader)

                    # Escrever o cabeçalho apenas se for a primeira cidade
                    if primeira_cidade:
                        writer.writerow(["codigo_ibge", "nome_cidade"] + cabeçalho)
                        primeira_cidade = False

                    # Escrever as linhas de dados com codigo_ibge e nome_cidade
                    for linha in reader:
                        if linha:  # Ignorar linhas vazias
                            writer.writerow([codigo_ibge, nome_cidade] + linha)

                    # Marcar a cidade como processada no log
                    log_processamento[str(codigo_ibge)] = {"nome": nome_cidade, "status": "sucesso"}
                    salvar_log(log_processamento)

                except StopIteration:
                    print(f"Resposta vazia para a cidade {nome_cidade}")
                    log_processamento[str(codigo_ibge)] = {"nome": nome_cidade, "status": "resposta_vazia"}
                    salvar_log(log_processamento)
            else:
                print(f"Falha na requisição para {nome_cidade}: {response.status_code}")
                log_processamento[str(codigo_ibge)] = {"nome": nome_cidade, "status": "falha", "codigo_status": response.status_code}
                salvar_log(log_processamento)

        except Timeout:
            print(f"Timeout na requisição para {nome_cidade}")
            log_processamento[str(codigo_ibge)] = {"nome": nome_cidade, "status": "timeout"}
            salvar_log(log_processamento)
            break  # Para o loop e permite reiniciar a partir daqui

        except Exception as e:
            print(f"Erro ao processar {nome_cidade}: {e}")
            log_processamento[str(codigo_ibge)] = {"nome": nome_cidade, "status": "erro", "mensagem": str(e)}
            salvar_log(log_processamento)
            break  # Para o loop e permite reiniciar a partir daqui

        # Pequeno delay para evitar sobrecarga na API
        time.sleep(1)

print("Processamento concluído ou pausado. Arquivo CSV salvo como 'climate_raw.csv'. Reinicie para continuar.")