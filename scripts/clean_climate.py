import pandas as pd
import hashlib
from collections import Counter

def clean_large_climate_csv(input_path, output_path):
    current_city = None
    current_lat = None
    current_lon = None
    columns = []
    capture_data = False

    with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8') as outfile:
        # Escrevendo o header final
        outfile.write('cidade,latitude,longitude,YEAR,DOY,T2M,T2M_MAX,T2M_MIN,ALLSKY_SFC_UV_INDEX,ALLSKY_SFC_UVA,ALLSKY_SFC_UVB,PRECTOTCORR\n')

        for line in infile:
            line = line.strip()
            if not line:
                continue  # Ignora linhas vazias

            parts = line.split(',')

            # Atualiza a cidade (linha que tenha pelo menos código_ibge e cidade)
            if len(parts) >= 2:
                possible_city = parts[1].strip()
                if possible_city and possible_city != current_city:
                    current_city = possible_city

            # Captura Latitude e Longitude
            if "Location:" in line and "latitude" in line and "longitude" in line:
                try:
                    location_part = line.split("Location:")[1]
                    lat_str = location_part.split('latitude')[1].split('longitude')[0].strip()
                    lon_str = location_part.split('longitude')[1].strip()
                    current_lat = lat_str
                    current_lon = lon_str
                except Exception as e:
                    print(f"Erro ao extrair latitude/longitude na linha: {line}, erro: {e}")

            # Detecta o início do header das colunas de dados
            if len(parts) >= 3 and parts[2].strip() == 'YEAR':
                columns = parts[2:]  # Captura só os nomes das colunas de dados
                capture_data = True
                continue

            # Coleta os dados
            if capture_data and not line.startswith('-') and len(parts) >= 3:
                data_values = parts[2:]

                if len(data_values) == len(columns):
                    row = dict(zip(columns, data_values))

                    try:
                        output_line = [
                            current_city,
                            current_lat,
                            current_lon,
                            row.get('YEAR', ''),
                            row.get('DOY', ''),
                            row.get('T2M', ''),
                            row.get('T2M_MAX', ''),
                            row.get('T2M_MIN', ''),
                            row.get('ALLSKY_SFC_UV_INDEX', ''),
                            row.get('ALLSKY_SFC_UVA', ''),
                            row.get('ALLSKY_SFC_UVB', ''),
                            row.get('PRECTOTCORR', '')
                        ]
                        outfile.write(','.join(output_line) + '\n')
                    except Exception as e:
                        print(f"Erro ao escrever linha: {line}, erro: {e}")

            # Fim do bloco de header
            if line.startswith('-END HEADER-'):
                capture_data = False
                columns = []

def deduplicate_large_csv_fast(input_path, output_path):
    seen_hashes = set()
    with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8') as outfile:
        header = infile.readline()
        outfile.write(header)  # Mantém o cabeçalho

        for i, line in enumerate(infile):
            # Calcula hash da linha (bem mais leve que guardar a linha inteira)
            line_hash = hashlib.md5(line.encode('utf-8')).hexdigest()

            if line_hash not in seen_hashes:
                outfile.write(line)
                seen_hashes.add(line_hash)

            if i % 200000 == 0:
                print(f"{i} linhas processadas...")

    print(f"Arquivo final salvo em {output_path}")

def contar_tuplas_por_cidade(csv_path, output_csv='contagem_cidades.csv'):
    contagem = Counter()

    # Ler o CSV em chunks
    for chunk in pd.read_csv(csv_path, chunksize=100_000):
        # Criar uma tupla (cidade, latitude, longitude) para cada linha
        keys = zip(chunk['cidade'], chunk['latitude'], chunk['longitude'])
        contagem.update(keys)

    # Converter o Counter para DataFrame
    resultado = pd.DataFrame.from_records(
        [(cidade, lat, lon, qtd) for (cidade, lat, lon), qtd in contagem.items()],
        columns=['cidade', 'latitude', 'longitude', 'quantidade']
    )

    # Ordenar por quantidade decrescente
    resultado = resultado.sort_values(by='quantidade', ascending=False)

    # Salvar como CSV
    resultado.to_csv(output_csv, index=False)

    print(f'Tabela salva como: {output_csv}')
    print(resultado.head())


# Uso:
# clean_large_climate_csv('climate_raw.csv', '../data/climate.csv')
# deduplicate_large_csv_fast('climate_clean.csv', 'climate2.csv')
contar_tuplas_por_cidade('../data/climate.csv')