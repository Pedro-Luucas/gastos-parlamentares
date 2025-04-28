from flask import Flask, request, jsonify
import pandas as pd
import os, time

app = Flask(__name__)

# Função para corrigir textos
def corrigir_texto(texto):
    if isinstance(texto, str):
        return texto.encode('latin1').decode('utf-8')
    return texto

startTime = time.time()
# Função para carregar todos os CSVs em um DataFrame só
def carregar_csvs():
    frames = []
    for ano in range(2008, 2026):  # 2008 até 2025
        caminho = f'gastos/Ano-{ano}.csv'
        if os.path.exists(caminho):
            df = pd.read_csv(
                caminho,
                sep=';',
                encoding='latin1',
                dtype=str
            )
            df = df.map(corrigir_texto)
            df = df.rename(columns={df.columns[0]: "txNomeParlamentar"})
            df['vlrDocumento'] = pd.to_numeric(df['vlrDocumento'], errors='coerce')
            df['vlrDocumento'] = df['vlrDocumento'].abs()
            df['ano'] = str(ano)  # Adiciona o ano
            df['txNomeParlamentar'] = df['txNomeParlamentar'].str.lower()  # Deixa o nome minúsculo
            frames.append(df)
        else:
            print(f'Aviso: {caminho} não encontrado.')
    return pd.concat(frames, ignore_index=True)

# Carrega tudo na memória
df = carregar_csvs()

print(f"--- {(time.time() - startTime)} ---")





# Cria um dicionário
dados_por_cpf = {}
for cpf, grupo in df.groupby('cpf'):
    dados_por_cpf[cpf] = grupo


@app.route('/gastos/<cpf>')
def gastos(cpf):
    politico = dados_por_cpf.get(cpf.lower())

    if politico is None or politico.empty:
        return jsonify({'mensagem': 'Nenhum dado encontrado para esse politico'}), 404

    gastos = politico[politico['urlDocumento'].fillna('sem url')][['vlrDocumento', 'urlDocumento', 'ano']]
    return jsonify(gastos.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
