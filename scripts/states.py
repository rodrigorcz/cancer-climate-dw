import pandas as pd
from tqdm import tqdm

def round_4_decimals(f):
    try:
        return f"{round(float(f), 4):.4f}"
    except:
        return None

# Lê JSON
df_json = pd.read_json("cidades_uf_ndjson.json", lines=True)
df_json.rename(columns={"nome": "cidade"}, inplace=True)

# Arredonda JSON
df_json['latitude'] = df_json['latitude'].apply(round_4_decimals)
df_json['longitude'] = df_json['longitude'].apply(round_4_decimals)

print("\nExemplo JSON após arredondar:")
print(df_json.head(10))

chunksize = 100000
csv_input = "../data/climate.csv"
csv_output = "climate6.csv"

first_chunk = True
total_lines = sum(1 for _ in open(csv_input)) - 1

with pd.read_csv(csv_input, chunksize=chunksize) as reader:
    for chunk in tqdm(reader, total=total_lines // chunksize + 1, desc="Processando CSV"):
        # Arredonda também no CSV
        chunk['latitude'] = chunk['latitude'].apply(round_4_decimals)
        chunk['longitude'] = chunk['longitude'].apply(round_4_decimals)

        # Teste visual de Abaetetuba ou Abaiara
        test_city = chunk[(chunk['cidade'].isin(['Abaetetuba', 'Abaiara']))]
        if not test_city.empty:
            print("\nLinhas testadas:")
            print(test_city[['cidade', 'latitude', 'longitude']].head(5))

        merged = chunk.merge(df_json, on=['cidade', 'latitude', 'longitude'], how='left')

        merged.to_csv(csv_output, mode='w' if first_chunk else 'a', index=False, header=first_chunk)
        first_chunk = False