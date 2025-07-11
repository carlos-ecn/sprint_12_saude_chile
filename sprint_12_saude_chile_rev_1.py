import pandas as pd
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats as st
import math as mth
import seaborn as sns
from plotly import graph_objects as go
import plotly.express as px
import sys
import getopt
import re
import os
import sqlalchemy # Pode manter, embora não seja estritamente usado como 'sqlalchemy.something'
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
# from sqlalchemy.inspect import inspect # <<< Comentamos esta linha, não é mais necessária!


'''
Analisar argumentos da linha de comando para 
retornar o caminho do arquivo.

Retorna:
file_path (str): o caminho para o arquivo 
fornecido pelo usuário.
'''

def parse_arguments():
    unixOptions = 'f:' # argumento no formato unix e ira retornar '-f'
    gnuOptions = ['file='] # argumento no formato gnu e ira retornar '--file='

    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:] # excluir o nome do script

    file_path = ''
    try:
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
        for currentArgument, currentValue in arguments:
            if currentArgument in ('-f', '--file'):
                file_path = currentValue
    except getopt.error as err:
        print(f"Error parsing arguments: {err}")
        sys.exit(2)
                
    return file_path


'''
Extrair o ano do nome do caminho.

Retorna:
o ano do documento com os dados.
'''
# Dividir o caminho do arquivo em partes

def extract_year_from_path(file_path):
    # Use re to find a 4-digit number that likely represents the year in the filename
    match = re.search(r'(\d{4})\.csv$', file_path)
    if match:
        return int(match.group(1))

    # Fallback if the pattern doesn't match, or if year is in another part of the path
    try:
        # Tries to get the last 4 characters before .csv, if structure is like '...2018.csv'
        year_str = file_path.split('/')[-1].split('.')[0][-4:]
        return int(year_str)
    
    except (ValueError, IndexError):
        print(f"[ERROR]: Could not extract year from file path: {file_path}")
        return None


'''
Valida se os dados que queremos armazenar já existem no banco de dados. Para fazer isso:
1. Use o objeto "engine" para se conectar ao banco de dados.
2. Crie uma consulta SQL para verificar se há algum registro em "table_name" correspondente ao "year" (ano) fornecido.
3. Execute a consulta SQL.
        
Retorna:
True se os dados já estão armazenados no seu banco de dados. Caso contrário, retorna False. Analisar
outros resultados possíveis ao consultar a tabela.
'''

def data_already_exist(engine, table_name, year):
    if year is None: # Handle cases where year extraction failed
        print("[WARNING]: Year could not be extracted for data existence check. Assuming data might not exist.")
        return False
        
    try:
        with engine.connect() as connection:
            # Consultar a nova tabela
            # Usando LIMIT 1 para otimizar, já que só precisamos saber se existe *algum* registro
            query = text(f'SELECT 1 FROM {table_name} WHERE ANO_EGRESO = :year_val LIMIT 1')
            result = connection.execute(query, {'year_val': year})
            exists = result.fetchone() is not None
            return exists
    except OperationalError as e:
        # Se a tabela não existe ainda, isso causará um OperationalError.
        # Nesse caso, os dados não "existem" na tabela.
        # Imprimir uma mensagem mais específica para este caso.
        if "no such table" in str(e).lower():
            print(f"[INFO]: Tabela '{table_name}' não existe ainda. Dados não existem.")
        else:
            print(f"[ERRO]: Erro operacional ao verificar existência de dados: {e}")
        return False
    except Exception as e:
        print(f"[ERRO]: Ocorreu um erro inesperado ao verificar existência de dados: {e}")
        return False

# Carregar os dados em um DataFrame para operações futuras

def load_data(file_path):
    try:
        df = pd.read_csv(file_path, encoding='latin1', sep=';')
        return df
    except FileNotFoundError:
        print(f"[ERROR]: Arquivo não encontrado em {file_path}")
        return pd.DataFrame() # Retorna um DataFrame vazio em caso de erro
    except Exception as e:
        print(f"[ERROR]: Erro ao carregar dados de {file_path}: {e}")
        return pd.DataFrame()


'''
Args:
df (pd.DataFrame): o DataFrame de entrada.
threshold (float): a proporção de colunas que podem conter '*' antes que 
a linha seja removida.
        
Pré-processar o DataFrame removendo as linhas em que a maioria das colunas contêm o 
caractere '*'. Você também precisa padronizar os tipos de dados de colunas de acordo com a
estrutura de dados apropriada. Inspecionar arquivos CSV diferentes para analisar possíveis problemas. Para fazer isso:
            
1. Calcule o número de colunas.
2. Determine quantos caracteres '*' são permitidos por linha com base no limiar.
3. Filtre as linhas que ultrapassaram o número permitido de '*'.
4. Converta colunas específicas para o tipo inteiro.
5. Renomeie as colunas usando uma lista predefinida de nomes novos.
6. Retorne o DataFrame limpo e formatado.
                
Retorna:
pd.DataFrame: o DataFrame limpo.
'''

def preprocess_data(df, threshold=0.5):
    if df.empty:
        print("[AVISO]: DataFrame vazio passado para preprocess_data. Retornando DataFrame vazio.")
        return df

    # Calcular o número de colunas
    num_columns = len(df.columns)

    # Determinar o número de caracteres '*' permitidos com base no limiar
    allowed_stars = int(num_columns * threshold)

    # Filtrar as linhas em que o número de caracteres '*' excede o limiar permitido
    cleaned_df = df[df.apply(lambda x: (x == '*').sum() <= allowed_stars, axis=1)].copy() # Use .copy() para evitar SettingWithCopyWarning

    # Renomear as colunas - Mapeamento explícito (ajuste se seus headers forem diferentes)
    column_mapping = {
        'PERTE': 'PERTENENCIA_ESTABLECIMIENTO_SALUD',
        'SEXO_PERSONA': 'SEXO',
        'EDAD_GRUPO': 'GRUPO_EDAD',
        'GRUPOS_ETAREOS': 'ETNIA',
        'GLOSA_PAIS_ORIGEN': 'GLOSA_PAIS_ORIGEN',
        'COMUNA_RESIDENCIA': 'COMUNA_RESIDENCIA',
        'GLOSA_COMUNA_RESIDENCIA': 'GLOSA_COMUNA_RESIDENCIA',
        'REGION_RESIDENCIA': 'REGION_RESIDENCIA',
        'GLOSA_REGION_RESIDENCIA': 'GLOSA_REGION_RESIDENCIA',
        'PREVISION': 'PREVISION',
        'GLOSA_PREVISION': 'GLOSA_PREVISION',
        'ANO_EGRESO': 'ANO_EGRESO',
        'DIAG1': 'DIAG1',
        'DIAG2': 'DIAG2',
        'DIAS_ESTADIA': 'DIAS_ESTADA', # Corrigido de ESTADIA
        'CONDICION_EGRESO': 'CONDICION_EGRESO',
        'INTERV_Q': 'INTERV_Q',
        'PROCED': 'PROCED'
    }
    
    # Aplica o renomeio apenas para as colunas que realmente existem no DataFrame
    valid_column_mapping = {k: v for k, v in column_mapping.items() if k in cleaned_df.columns}
    cleaned_df.rename(columns=valid_column_mapping, inplace=True)

    # Converter colunas específicas para o tipo inteiro
    int_columns_to_convert = ['COMUNA_RESIDENCIA', 'REGION_RESIDENCIA', 'ANO_EGRESO', 'DIAS_ESTADA']
    for col in int_columns_to_convert:
        if col in cleaned_df.columns:
            # Converte valores não numéricos para NaN, preenche NaN com 0 e converte para int
            cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce').fillna(0).astype(int)
        else:
            print(f"[AVISO]: Coluna '{col}' não encontrada para conversão para inteiro. Pulando.")

    return cleaned_df


'''
Criar uma conexão ao banco de dados com "sqlite:///"
1. Crie uma string de conexão usando um nome de banco de dados conveniente.  Por exemplo,
"sqlite:///{nome do seu banco de dados}.db". Armazene-a em uma variável.
2. Inicie o mecanismo do SQLAlchemy para o banco de dados chamando create_engine(). Passe
a variável anterior como um parâmetro dessa função.
3. Imprima uma mensagem de confirmação de conexão.
4. Retorne o mecanismo do SQLAlchemy para mais interações com o banco de dados.
'''

def create_db_engine(db_path):
    """
    Cria um engine de conexão com o banco de dados usando SQLAlchemy.
    Garante que o diretório para o arquivo do banco de dados exista.

    Args:
        db_path (str): O caminho completo para o arquivo do banco de dados (ex: 'database/meu_banco.db').

    Returns:
        sqlalchemy.engine.base.Engine: O engine do SQLAlchemy para interações com o banco de dados.
    """
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
            print(f"[INFO]: Diretório do banco de dados criado: {db_dir}")
        except OSError as e:
            print(f"[ERRO]: Não foi possível criar o diretório {db_dir}: {e}")
            sys.exit(1) # Sai se o diretório não puder ser criado

    connection_string = f'sqlite:///{db_path}'
    try:
        engine = create_engine(connection_string)
        # Testa a conexão executando uma consulta simples
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print(f'[INFO]: Conexão com o banco de dados bem-sucedida: {connection_string}')
        return engine
    except OperationalError as e:
        print(f'[ERRO CRÍTICO]: Falha ao conectar ao banco de dados em {db_path}. Detalhes: {e}')
        print("Por favor, verifique se o caminho está correto e se você tem permissões de escrita.")
        sys.exit(1) # Sai do script se a conexão com o DB falhar


'''
Salvar o DataFrame (df) limpo no seu banco de dados.
1. O DataFrame é inserido em uma tabela SQL especificada por table_name.
2. Se a tabela já existir, novos dados serão anexados.
3. O índice do DataFrame é excluído das colunas da tabela.
4. A conexão ao banco de dados é gerenciada usando o mecanismo fornecido.
'''

def save_to_database(df, engine, table_name):
    """
    Salva o DataFrame limpo na tabela SQL especificada.

    Args:
        df (pd.DataFrame): O DataFrame a ser salvo.
        engine (sqlalchemy.engine.base.Engine): O engine do SQLAlchemy.
        table_name (str): O nome da tabela onde salvar.
    """
    if df.empty:
        print("[AVISO]: DataFrame vazio fornecido para salvar no banco de dados. Operação de salvamento ignorada.")
        return

    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"[INFO]: Dados carregados com sucesso na tabela '{table_name}'.")
    except Exception as e:
        print(f"[ERRO]: Falha ao salvar dados na tabela do banco de dados '{table_name}': {e}")


# --- CÓDIGO SIMPLIFICADO PARA TESTAR O PIPELINE (SEM INSPECT) ---
def validate_data(engine, table_name):
    """
    Valida os dados imprimindo a contagem de registros por ano do banco de dados.
    Esta versão simplificada tenta apenas consultar a tabela diretamente.

    Args:
        engine (sqlalchemy.engine.base.Engine): O engine do SQLAlchemy.
        table_name (str): O nome da tabela a ser validada.
    """
    print(f"\n--- Validação do Banco de Dados: Registros por ano em '{table_name}' ---")
    try:
        with engine.connect() as connection:
            # Tenta consultar a tabela diretamente. Se a tabela não existir,
            # uma OperationalError será levantada e capturada.
            query = text(f'SELECT ANO_EGRESO, count(*) FROM {table_name} GROUP BY ANO_EGRESO ORDER BY ANO_EGRESO')
            result = connection.execute(query)
            rows = result.fetchall()
            
            if rows:
                for row in rows:
                    print(f"Ano: {row[0]}, Registros: {row[1]}")
            else:
                print(f"Nenhum registro encontrado na tabela '{table_name}'.")
    except OperationalError as e:
        # Se a tabela não existe, uma OperationalError será levantada.
        if "no such table" in str(e).lower():
            print(f"[INFO]: A tabela '{table_name}' ainda não existe no banco de dados. Não há dados para validar.")
        else:
            print(f"[ERRO]: Erro operacional inesperado durante a validação do banco de dados: {e}")
    except Exception as e:
        print(f"[ERRO]: Ocorreu um erro inesperado durante a validação do banco de dados: {e}")


# --- Lógica Principal de Execução ---
if __name__ == "__main__":
    # Define o diretório onde seus arquivos de dados brutos estão localizados
    # Assume que há uma pasta 'data' ao lado do seu script, por exemplo:
    # my_project/
    # ├── seu_script.py
    # └── data/
    #     ├── EGRE_DATOS_ABIERTOS_2018.csv
    #     └── EGRE_DATOS_ABIERTOS_2019.csv
    #     └── EGRE_DATOS_ABIERTOS_2020.csv
    data_directory = os.path.join(os.path.dirname(__file__), 'data')
    
    # Ou, se os arquivos estiverem no mesmo diretório do script:
    # data_directory = os.path.dirname(__file__)

    table_name = 'egresos_pacientes'
    db_path = os.path.join(os.path.dirname(__file__), 'database', 'ministerio_de_salud_chile.db')

    # Cria o engine do DB (e garante que o diretório do banco de dados exista)
    engine = create_db_engine(db_path)

    # --- Descoberta Automática de Arquivos ---
    processed_any_file = False
    
    if not os.path.exists(data_directory):
        print(f"[ERRO]: Diretório de dados '{data_directory}' não encontrado. Por favor, crie-o e coloque seus arquivos CSV dentro.")
        sys.exit(1) # Sai se o diretório de dados não existir

    print(f"\nVerificando arquivos em: {data_directory}")
    for filename in os.listdir(data_directory):
        # Usa um padrão de regex para corresponder aos seus arquivos (ex: EGRE_DATOS_ABIERTOS_AAAA.csv)
        if re.match(r'EGRE_DATOS_ABIERTOS_\d{4}\.csv$', filename):
            file_path = os.path.join(data_directory, filename)
            
            print(f"\n--- Processando arquivo: {filename} ---")
            
            year = extract_year_from_path(file_path)
            if year is None:
                print(f"[PULAR]: Não foi possível extrair o ano de {filename}. Pulando este arquivo.")
                continue

            # Verifica se os dados para este ano já existem no banco de dados
            if data_already_exist(engine, table_name, year):
                print(f"Dados para o ano {year} de '{filename}' já existem no banco de dados. Pulando.")
            else:
                print(f"[INFO]: Dados para o ano {year} de '{filename}' não encontrados. Carregando e processando.")
                raw_data = load_data(file_path)
                
                if not raw_data.empty:
                    print(f'[INFO]: {len(raw_data)} linhas carregadas. Colunas originais: {raw_data.columns.tolist()}')
                    processed_data = preprocess_data(raw_data)
                    print(f'[INFO]: Dados pré-processados. Contém {len(processed_data)} linhas. Colunas após processamento: {processed_data.columns.tolist()}')
                    
                    if not processed_data.empty:
                        save_to_database(processed_data, engine, table_name)
                        processed_any_file = True
                    else:
                        print(f"[AVISO]: Dados processados para '{filename}' estão vazios. Não salvando no DB.")
                else:
                    print(f"[AVISO]: Falha ao carregar dados de '{filename}'. Pulando pré-processamento e salvamento.")
        else:
            print(f"[INFO]: Pulando arquivo não correspondente: {filename}")

    if not processed_any_file:
        print("\nNenhum arquivo novo foi processado ou salvo no banco de dados.")
    else:
        print("\nProcessamento de todos os arquivos novos/não processados concluído.")
    
    # Validação final dos dados no banco de dados
    validate_data(engine, table_name)