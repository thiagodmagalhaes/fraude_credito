from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

def main():
    try:
        print("Iniciando execução do código. . .")
        spark = criar_sessao_spark()
        diretorio = rf"C:\Users\Abrasel Nacional\Desktop\TESTE\fraude_credito\data\input\df_fraud_credit.csv"
        df = ler_arquivo_csv(spark, diretorio)
        df = limpeza_dataframe(df)
        df_mediarisco = media_risco(df)
        df_mediarisco.show(10, truncate=False)

    except Exception as e:
        print(f"EXECUÇÕO INTERROMPIDA!!!  Erro na execução do código: {e}")
        #enviar_email(e)
        spark.stop()

    finally:
        print("O código foi finalizado!!!!")
        spark.stop()


if __name__ == "__main__":
    main()

