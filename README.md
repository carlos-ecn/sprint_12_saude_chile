📊 Healthcare Data Pipeline

Construção de Pipeline de Ingestão e Armazenamento de Dados de Saúde Pública



Autor: Carlos Eduardo Cruz Nakandakare



🎯 Objetivo do Projeto



Este projeto tem como objetivo desenvolver um pipeline automatizado de ingestão e processamento de dados de saúde pública do Chile, permitindo carregar dados estruturados em um banco de dados para análises futuras.



O pipeline foi projetado para:



automatizar a ingestão de múltiplos arquivos CSV



padronizar e limpar os dados



evitar duplicação de dados já carregados



armazenar os dados em um banco relacional



permitir reprocessamento incremental de novos arquivos



Esse tipo de solução é comum em ambientes de Data Engineering e Analytics Engineering, onde grandes volumes de dados precisam ser organizados antes de serem utilizados em análises ou dashboards.



📈 Resultado



O projeto resultou em um pipeline de dados funcional, capaz de:



detectar automaticamente novos arquivos de dados



extrair metadados do nome do arquivo (ano de referência)



processar e limpar os dados



armazenar as informações em um banco SQLite



evitar duplicação de registros já processados



validar os dados armazenados no banco



Ao final da execução, o sistema apresenta um resumo da quantidade de registros armazenados por ano, permitindo validar rapidamente a integridade da carga de dados.



🛠 Ferramentas Utilizadas



O projeto foi desenvolvido utilizando o ecossistema Python para engenharia e processamento de dados.



Linguagem e Ambiente



Python



Script Python executável via linha de comando



Bibliotecas Utilizadas



Pandas → manipulação e processamento de dados



NumPy → operações numéricas



SQLAlchemy → conexão e interação com banco de dados



SQLite → armazenamento persistente dos dados



Regex (re) → extração de metadados do nome dos arquivos



OS / Sys / Getopt → manipulação de arquivos e argumentos de linha de comando



Bibliotecas adicionais importadas para possíveis análises futuras:



Matplotlib



Seaborn



Plotly



SciPy



📚 O Que Eu Aprendi (Habilidades Desenvolvidas)

Técnicas



construção de pipelines de dados em Python



ingestão automatizada de múltiplos arquivos



processamento e padronização de datasets



integração de dados com banco relacional



manipulação de metadados a partir de nomes de arquivos



criação de scripts executáveis via linha de comando



Engenharia de Dados



controle de duplicidade de ingestão



persistência de dados em banco SQLite



automação de pipelines de ingestão



organização de estrutura de dados para análises futuras



Organização de Projetos



estruturação de diretórios de dados



separação entre dados brutos e banco de dados



criação de pipeline reprocessável



🔍 Descrição do Projeto



O projeto utiliza dados públicos de egressos hospitalares do Ministério da Saúde do Chile, distribuídos em arquivos CSV anuais.



O pipeline foi projetado para processar automaticamente arquivos no formato:



EGRE\_DATOS\_ABIERTOS\_YYYY.csv



Cada arquivo representa dados de um determinado ano.



O sistema:



identifica automaticamente os arquivos disponíveis



extrai o ano do nome do arquivo



verifica se os dados já existem no banco



processa os dados



salva no banco de dados



⚙️ Arquitetura do Pipeline



Fluxo do processamento:



Arquivos CSV (dados brutos)

&#x20;       │

&#x20;       ▼

Descoberta automática de arquivos

&#x20;       │

&#x20;       ▼

Extração do ano do arquivo

&#x20;       │

&#x20;       ▼

Verificação de duplicidade no banco

&#x20;       │

&#x20;       ▼

Carregamento dos dados (Pandas)

&#x20;       │

&#x20;       ▼

Pré-processamento e padronização

&#x20;       │

&#x20;       ▼

Persistência no banco SQLite

&#x20;       │

&#x20;       ▼

Validação dos registros carregados

🔧 Etapas do Pipeline

1️⃣ Descoberta Automática de Arquivos



O sistema percorre automaticamente o diretório data/ e identifica arquivos que seguem o padrão:



EGRE\_DATOS\_ABIERTOS\_YYYY.csv



Isso permite adicionar novos arquivos sem modificar o código.



2️⃣ Extração de Metadados



O ano do dataset é extraído automaticamente do nome do arquivo utilizando expressões regulares (regex).



Exemplo:



EGRE\_DATOS\_ABIERTOS\_2019.csv → 2019

3️⃣ Controle de Duplicidade



Antes de carregar os dados, o pipeline verifica se o ano já existe no banco de dados.



Caso os dados já tenham sido carregados anteriormente, o arquivo é ignorado.



Isso evita duplicação de dados.



4️⃣ Pré-processamento de Dados



Durante o processamento são realizadas etapas como:



remoção de linhas com excesso de valores inválidos (\*)



padronização de nomes de colunas



conversão de tipos de dados



tratamento de valores não numéricos



5️⃣ Persistência no Banco de Dados



Os dados processados são armazenados em um banco SQLite utilizando SQLAlchemy.



Tabela principal:



egresos\_pacientes



A carga é realizada utilizando o método:



DataFrame.to\_sql()



com modo append, permitindo cargas incrementais.



6️⃣ Validação da Carga



Após o processamento, o sistema executa uma consulta SQL que retorna:



ANO\_EGRESO | TOTAL\_REGISTROS



Isso permite validar rapidamente a integridade do banco de dados.



🚀 Como Executar o Projeto

Requisitos



Python 3.9 ou superior



Instalação das dependências

pip install pandas numpy sqlalchemy



(opcional)



pip install matplotlib seaborn plotly scipy

Estrutura esperada do projeto

project/

│

├── script\_pipeline.py

│

├── data/

│   ├── EGRE\_DATOS\_ABIERTOS\_2018.csv

│   ├── EGRE\_DATOS\_ABIERTOS\_2019.csv

│   └── EGRE\_DATOS\_ABIERTOS\_2020.csv

│

└── database/

&#x20;   └── ministerio\_de\_salud\_chile.db

Execução



Execute o script Python:



python script\_pipeline.py



O pipeline irá:



identificar os arquivos CSV



verificar duplicidade



processar dados novos



armazenar no banco



validar os registros



🔧 Possíveis Melhorias



Algumas evoluções possíveis para o pipeline:



utilização de PostgreSQL ou MySQL



implementação de logs estruturados



criação de testes automatizados



implementação de Airflow ou Prefect para orquestração



criação de camada analítica (data mart)

