import os
import pandas as pd
from sqlalchemy import create_engine

# Configurações do banco
USER = 'postgres'
PASSWORD = 'root'
HOST = 'localhost'
PORT = '5432'
DB = 'gastos'

# Caminho dos CSVs
caminho_csvs = 'gastos/'

# Conexão
engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')

# Função para corrigir encoding
def corrigir_texto(texto):
    if isinstance(texto, str):
        return texto.encode('latin1').decode('utf-8')
    return texto

for ano in range(2008, 2026):  # de 2008 até 2025
    caminho = os.path.join(caminho_csvs, f'Ano-{ano}.csv')
    if not os.path.exists(caminho):
        print(f"Arquivo {caminho} não encontrado, pulando...")
        continue

    print(f"Importando ano {ano}...")

    # Lê o CSV
    df = pd.read_csv(
        caminho,
        sep=';',
        encoding='latin1',
        dtype=str   # Agora tudo lido como string
    )

    # Corrige caracteres
    df = df.map(corrigir_texto)

    # Renomeia a primeira coluna
    df = df.rename(columns={df.columns[0]: "txNomeParlamentar"})

    # Não converte mais 'vlrDocumento' para número aqui

    # Adiciona a coluna do ano
    df['ano'] = str(ano)

    # Substitui NaN por None
    df = df.where(pd.notnull(df), None)

    # Insere no banco
    df.to_sql('gastos_parlamentares', engine, if_exists='append', index=False, method='multi')

print("✅ Importação concluída!")
