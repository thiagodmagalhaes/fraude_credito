from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, NumericType, DateType, TimestampType, BooleanType

def criar_sessao_spark():
    '''
    Define as configurações do pyspark
        Arg:
            Não recebe argumentos
        Return:
            spark (retorna sessão spark para ser utilizada)
    '''

    try:
        print('Iniando sessão Spark . . .')
        spark = SparkSession.builder \
            .appName("FraudeCredito") \
            .config("spark.driver.memory", "4G") \
            .getOrCreate()
        print('Sessão iniciada com sucesso !!!')
        return spark
    
    except Exception as e:
        print(f"Erro ao criar sessão: {e}")
        raise e
    
def ler_arquivo_csv(spark, diretorio):
    '''
    Realiza a leitura do arquivo csv e retorna em um dataframe
        Arg: 
            spark, diretorio  (recebe a sessão spark e o diretório do arquivo csv como argumento)
        Return: 
            df (retorna o .csv em um dataframe)
    '''
    try:
        print("Realizando leitura da base .CSV")
        df = spark.read.csv(diretorio, header=True, inferSchema=True)
        print("Leitura realizada com sucesso!!!")
        return df
    
    except Exception as e:
        print(f"Erro ao realizar letura do arquivo CSV: {e}")
        raise e

def limpeza_dataframe(df):
    '''
    Realiza a limpeza das colunas retirando espacos dos dados preenchidos (no comeco e fim), além de quebras de linhas
        Arg:
            df(dataframe)
        Return:
            df(tratado)
    '''
    try:
        print("Realizando a limpeza do dataframe, removendo espaços e quebra de linhas de cada coluna . . .")
        colunas = df.columns
        for coluna in colunas:
            df = df.withColumn(coluna, F.when(F.col(coluna).isNotNull(), F.trim(F.col(coluna))).otherwise(F.col(coluna)))

        df = df.withColumn("risk_score", 
                            F.when(F.col("risk_score")== "none", F.lit(None)).otherwise(F.col("risk_score").cast("double")))
        
        df = df.dropDuplicates()
        df = df.fillna({"location_region": "DESCONHECIDO", "risk_score":0, "amount":0})
        print("Limpeza concluida!!!")
        return df
    except Exception as e:
        print(f"Erro ao realizar limpeza dos dados: {e}")
        raise e

def media_risco(df):
    try:
        df_mediarisco = (df.groupBy("location_region")
                         .agg(F.avg("risk_score").alias("media_risco"))
                         .orderBy(F.col("media_risco").desc()))
        return df_mediarisco
    
    except Exception as e:
        print(f"Erro ao calcular média de risco: {e}")
        raise e
      
def tabela_top3_transacoes(df):
    """
    Realiza o ranking dos TOP 3 transações, retornando o resultado em um dataframe rankeado.
    Arg:
        df(dataframe completo)
    Return:
        df_top3(dataframe rankeado apenas com receiving_address, amount, timestamp)
    """
    try:
        print("Iniciando a filtragem dos TOP 3 de transações . . .")
        df = df.withColumn("amount",
            F.when((F.col("amount").isNull()) |(F.col("amount") == "") |
                (F.lower(F.col("amount")) == "none") |(F.lower(F.col("amount")) == "null"),None).otherwise(F.col("amount"))) 
        df = df.withColumn("amount", F.col("amount").cast("double"))
        df = df.filter(F.col("amount").isNotNull())
        df_filtrado = df.filter(F.col("transaction_type") == "sale")
        df_ultimas_transacoes = (df_filtrado.groupBy("receiving_address").agg(F.max("timestamp").alias("max_timestamp")))
        df_ultimas_transacoes = df_ultimas_transacoes.withColumnRenamed("receiving_address", "receiving_address_max")
        df_ultimas_transacoes = df_ultimas_transacoes.withColumnRenamed("max_timestamp", "max_timestamp_ult")
        df_mais_recente = df_filtrado.join(df_ultimas_transacoes,(df_filtrado.receiving_address == df_ultimas_transacoes.receiving_address_max) &
            (df_filtrado.timestamp == df_ultimas_transacoes.max_timestamp_ult),"inner")
        df_top3 = df_mais_recente.orderBy(F.col("amount").desc()).limit(3)
        print("TOP 3 Obtido com sucesso!!!")
        return df_top3.select("receiving_address", "amount", "timestamp")

    except Exception as e:
        print(f"Erro ao gerar top 3 transações: {e}")
        raise e

def indicadores_qualidade(df, spark):
    """Gera indicadores básicos de qualidade e retorna como DataFrame"""
    try:
        print("Iniciando cálculo dos indicadores de qualidade...")
        total_registros = df.count()
        print(f"Total de registros: {total_registros}")
        erros_por_coluna = {}
        for coluna in df.columns:
            tipo = df.schema[coluna].dataType  
            if isinstance(tipo, StringType):
                qtd_erros = df.filter(F.col(coluna).isNull() | (F.trim(F.col(coluna)) == "") | (F.col(coluna.lower()) == "none")).count()
            elif isinstance(tipo, NumericType):
                qtd_erros = df.filter(F.col(coluna).isNull()).count()
            elif isinstance(tipo, (DateType, TimestampType)):
                qtd_erros = df.filter(F.col(coluna).isNull()).count()
            elif isinstance(tipo, BooleanType):
                qtd_erros = df.filter(F.col(coluna).isNull()).count()
            else:
                qtd_erros = df.filter(F.col(coluna).isNull()).count()
            erros_por_coluna[coluna] = qtd_erros
        total_erros = sum(erros_por_coluna.values())
        percentual_conformidade = round(((total_registros - total_erros) / total_registros) * 100, 2)
        dados_resumo = [(total_registros, total_erros, float(percentual_conformidade))]
        colunas_resumo = ["total_registros", "total_erros", "percentual_conformidade"]
        df_resumo = spark.createDataFrame(dados_resumo, colunas_resumo)
        print("Indicadores gerados com sucesso.")
        return df_resumo

    except Exception as e:
        print(f"Erro ao gerar indicadores de qualidade: {e}")
        raise


def main():
    try:
        print("Iniciando execução do código. . .")
        spark = criar_sessao_spark()
        diretorio = rf"C:\Users\Abrasel Nacional\Desktop\TESTE\fraude_credito\data\input\df_fraud_credit.csv"
        df = ler_arquivo_csv(spark, diretorio)
        df_indicador_qld= indicadores_qualidade(df,spark)
        df_indicador_qld.show()
        df = limpeza_dataframe(df)
        df_mediarisco = media_risco(df)
        df_top3_transacoes = tabela_top3_transacoes(df)




    except Exception as e:
        print(f"EXECUÇÕO INTERROMPIDA!!!  Erro na execução do código: {e}")
        #enviar_email(e)
        spark.stop()

    finally:
        print("O código foi finalizado!!!!")
        spark.stop()


if __name__ == "__main__":
    main()

