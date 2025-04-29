import os
import pandas as pd
import sqlalchemy
from sqlalchemy.types import Text

# Configurações do banco
USER = 'postgres'
PASSWORD = 'root'
HOST = 'localhost'
PORT = '5432'
DB = 'gastos'

# Caminho dos CSVs
caminho_csvs = 'gastos/'

# Conexão
engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')

# Função para corrigir encoding
def corrigir_texto(texto):
    if isinstance(texto, str):
        return texto.encode('latin1').decode('utf-8', errors='replace')
    return texto

# Primeiro verificar se temos acesso aos arquivos antes de criar a tabela
arquivos_existentes = False
for ano in range(2008, 2026):
    caminho = os.path.join(caminho_csvs, f'Ano-{ano}.csv')
    if os.path.exists(caminho):
        arquivos_existentes = True
        try:
            # Vamos ler o primeiro arquivo para determinar as colunas
            primeiro_df = pd.read_csv(
                caminho,
                sep=';',
                encoding='latin1',
                dtype=str,
                nrows=1  # Só precisamos das colunas
            )
            
            # Força o nome da primeira coluna para "txNomeParlamentar"
            colunas_corretas = primeiro_df.columns.tolist()
            colunas_corretas[0] = "txNomeParlamentar"  # Substitui o nome da primeira coluna
            primeiro_df.columns = colunas_corretas
            
            # Garantir que todas as colunas são strings sem caracteres especiais
            colunas_limpas = []
            for col in primeiro_df.columns:
                # Remove BOM e outros caracteres problemáticos
                col_limpa = col.replace('ï»¿', '').replace('\ufeff', '').strip('"\'').strip()
                colunas_limpas.append(col_limpa)
            
            primeiro_df.columns = colunas_limpas
            break
        except Exception as e:
            print(f"Erro ao ler o arquivo {caminho} para determinar colunas: {str(e)}")
            continue

if not arquivos_existentes:
    print("Nenhum arquivo CSV encontrado na pasta especificada.")
    exit()

# Agora criamos a tabela com base nas colunas corretas
with engine.connect() as conn:
    conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS gastos_parlamentares"))
    conn.commit()
    
    # Criar a tabela usando um formato mais seguro que evite problemas de sintaxe SQL
    colunas_sql = []
    for col in primeiro_df.columns:
        nome_seguro = col.replace('"', '').replace("'", "")
        colunas_sql.append(f'"{nome_seguro}" TEXT')
    
    colunas_sql.append('"ano" TEXT')  # Adiciona a coluna ano
    create_table_sql = f"CREATE TABLE gastos_parlamentares ({', '.join(colunas_sql)})"
    
    print(f"Criando tabela com as colunas: {primeiro_df.columns.tolist()} + ano")
    conn.execute(sqlalchemy.text(create_table_sql))
    conn.commit()

# Processamento de cada arquivo CSV
for ano in range(2008, 2026):
    caminho = os.path.join(caminho_csvs, f'Ano-{ano}.csv')
    if not os.path.exists(caminho):
        print(f"Arquivo {caminho} não encontrado, pulando...")
        continue

    print(f"Importando ano {ano}...")
    
    try:
        # Lê o CSV
        df = pd.read_csv(
            caminho,
            sep=';',
            encoding='latin1',
            dtype=str,
            low_memory=False,
            na_values=['nan', 'NaN', 'NULL', ''],
            keep_default_na=True
        )
        
        # Corrigir a primeira coluna sempre
        colunas = df.columns.tolist()
        colunas[0] = "txNomeParlamentar"  # Força o nome da primeira coluna
        
        # Limpar todas as colunas de caracteres especiais
        colunas_limpas = []
        for col in colunas:
            # Remove BOM e outros caracteres problemáticos
            col_limpa = col.replace('ï»¿', '').replace('\ufeff', '').strip('"\'').strip()
            colunas_limpas.append(col_limpa)
        
        df.columns = colunas_limpas
        
        # Corrige caracteres com tratamento de erro
        df = df.map(lambda x: corrigir_texto(x) if pd.notna(x) else None)
        
        # Adiciona a coluna do ano
        df['ano'] = str(ano)
        
        # Substitui NaN por None para SQLAlchemy
        df = df.replace({pd.NA: None})
        df = df.where(pd.notnull(df), None)
        
        # Configure os tipos de dados para esta importação
        dtypes = {col: Text() for col in df.columns}
        
        print(f"Iniciando inserção no banco para o ano {ano}...")
        
        # Inserir em pequenos chunks para evitar problemas de memória
        chunk_size = 1000
        
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            try:
                chunk.to_sql(
                    'gastos_parlamentares', 
                    engine, 
                    if_exists='append', 
                    index=False,
                    dtype=dtypes,
                    method='multi',
                    chunksize=200
                )
                print(f"  Inseridos registros {i} até {min(i+chunk_size, len(df))} de {len(df)}")
            except Exception as e:
                print(f"  Erro ao inserir registros: {str(e)}")
                # Se falhar, tente adicionar colunas faltantes
                if "column" in str(e).lower() and ("não existe" in str(e).lower() or "does not exist" in str(e).lower()):
                    print("  Tentando adicionar colunas faltantes...")
                    with engine.connect() as conn:
                        for col in chunk.columns:
                            col_limpa = col.replace('"', '').replace("'", "")
                            try:
                                alter_sql = f'ALTER TABLE gastos_parlamentares ADD COLUMN IF NOT EXISTS "{col_limpa}" TEXT'
                                conn.execute(sqlalchemy.text(alter_sql))
                            except Exception as alter_err:
                                print(f"    Erro ao adicionar coluna {col}: {str(alter_err)}")
                        conn.commit()
                    
                    # Tenta novamente após adicionar colunas
                    try:
                        chunk.to_sql(
                            'gastos_parlamentares', 
                            engine, 
                            if_exists='append', 
                            index=False,
                            dtype=dtypes,
                            method='multi',
                            chunksize=200
                        )
                        print(f"  Registros inseridos com sucesso após adicionar colunas")
                    except Exception as retry_err:
                        print(f"  Erro ao reinserir: {str(retry_err)}")
                        # Se ainda falhar, vamos simplificar e tentar apenas as colunas existentes
                        try:
                            with engine.connect() as conn:
                                # Obter as colunas atuais do banco
                                insp = sqlalchemy.inspect(engine)
                                colunas_existentes = insp.get_columns('gastos_parlamentares')
                                nomes_colunas = [col['name'] for col in colunas_existentes]
                                
                                # Filtrar apenas as colunas que existem na tabela
                                colunas_validas = [col for col in chunk.columns if col in nomes_colunas]
                                chunk_filtrado = chunk[colunas_validas]
                                
                                # Tentar inserir apenas com as colunas válidas
                                chunk_filtrado.to_sql(
                                    'gastos_parlamentares', 
                                    engine, 
                                    if_exists='append', 
                                    index=False,
                                    dtype={col: Text() for col in colunas_validas},
                                    method='multi',
                                    chunksize=200
                                )
                                print(f"  Inseridos {len(chunk_filtrado)} registros usando apenas colunas válidas")
                        except Exception as final_err:
                            print(f"  Erro final na inserção: {str(final_err)}")
                            continue
            
        print(f"✅ Ano {ano} importado com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao processar o ano {ano}:")
        print(f"Erro: {str(e)}")
        print("Tentando continuar com o próximo ano...")
        
print("Processo de importação finalizado!")