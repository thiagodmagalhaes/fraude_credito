Fraude de Crédito com PySpark

Este projeto tem como objetivo realizar análise e tratamento de dados de
fraude de crédito utilizando PySpark, aplicando processos de leitura,
limpeza, indicadores de qualidade, cálculos estatísticos e exportação
dos resultados em CSV.

------------------------------------------------------------------------

🚀 Funcionalidades

-   Criar sessão Spark com configurações personalizadas.
-   Ler arquivos CSV em DataFrames Spark.
-   Limpar colunas removendo espaços extras, valores nulos, none e
    duplicados.
-   Calcular média de risco por região e salvar o resultado em CSV.
-   Gerar tabela com as Top 3 transações mais altas.
-   Criar indicadores de qualidade com base em inconsistências nos
    dados.
-   Exportar resultados em CSV.

------------------------------------------------------------------------

📂 Estrutura do Projeto

    fraude_credito/
    │
    ├── data/
    │   ├── input/        # Arquivos CSV de entrada
    │   └── output/       # Resultados processados (CSV)
    │
    ├── fraude_credito.py # Código principal do projeto
    └── README.md         # Documentação

------------------------------------------------------------------------

⚙️ Pré-requisitos

-   Python 3.8+
-   Apache Spark
-   Java 8 ou superior

Bibliotecas utilizadas

-   pyspark

------------------------------------------------------------------------

▶️ Como Executar

1.  Clone este repositório:

        git clone https://github.com/seu-usuario/fraude_credito.git
        cd fraude_credito

2.  Garanta que o Spark está configurado corretamente no seu ambiente.

3.  Execute o código:

        main.py

------------------------------------------------------------------------

📊 Exemplos de Saída

Indicadores de Qualidade

  total_registros   total_erros   percentual_conformidade
  ----------------- ------------- -------------------------
  10000             500           95.0 %

Média de Risco por Região

  location_region   media_risco
  ----------------- -------------
  Africa                0.85
  South America                0.72

Top 3 Transações

  receiving_address   amount   timestamp
  ------------------- -------- ---------------------
  X1A2B3C             1500     2025-09-15 10:00:00
  Y4D5E6F             1200     2025-09-15 11:30:00
  Z7G8H9I             1000     2025-09-15 12:45:00

------------------------------------------------------------------------

🛠 Funções Principais

-   criar_sessao_spark() → Cria sessão Spark configurada.
-   ler_arquivo_csv(spark, diretorio) → Lê arquivos CSV para DataFrame.
-   limpeza_dataframe(df) → Realiza limpeza e padronização dos dados.
-   media_risco(df, spark) → Calcula média de risco por região.
-   tabela_top3_transacoes(df, spark) → Gera ranking das 3 maiores
    transações.
-   indicadores_qualidade(df, spark) → Cria indicadores de qualidade do
    dataset.
-   salvar_arquivo_csv(spark, df, diretorio) → Salva DataFrames em CSV.

------------------------------------------------------------------------

📧 Contato

Caso tenha dúvidas ou sugestões, entre em contato: - Autor: Thiago Magalhães
Tecnologia - E-mail: thiago.dmagalhaes@hotmail.com - GitHub:
github.com/thiagodmagalhaes
