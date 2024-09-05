
import requests
import pandas as pd
import json

url = "https://homologacao.agroopenbank.com.br/api/v1/produtores?key=fpGCVXMudaBEmA46BgRG7LLrHqvQDeSb"
response = requests.get(url)

df = pd.DataFrame(json.loads(response.content))

id = df['id']
nome = df['nome']

print(id,nome)
