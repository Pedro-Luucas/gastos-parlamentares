import pandas as pd


def corrigir_texto(texto):
    if isinstance(texto, str):
        return texto.encode('latin1').decode('utf-8')
    return texto

df = pd.read_csv(
    'gastos/Ano-2015.csv',
    sep=';',
    encoding='latin1',
    dtype=str
)

# Corrigindo os nomes, botando acento
df = df.map(corrigir_texto)
df = df.rename(columns={df.columns[0]:"txNomeParlamentar"})

# Transformando o vlrDocumento em float 
df['vlrDocumento'] = pd.to_numeric(df['vlrDocumento'], errors='coerce')