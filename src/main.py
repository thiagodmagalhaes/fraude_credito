from pyspark.sql import SparkSession

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



def main():
    try:
        print("Iniciando execução do código. . .")
        spark = criar_sessao_spark()
        diretorio = rf"C:\Users\Abrasel Nacional\Desktop\TESTE\fraude_credito\data\input\df_fraud_credit.csv"
        df = ler_arquivo_csv(spark, diretorio)
        df.show(10, truncate=False)

    except Exception as e:
        print(f"EXECUÇÕO INTERROMPIDA!!!  Erro na execução do código: {e}")
        #enviar_email(e)
        spark.stop()

    finally:
        print("O código foi finalizado!!!!")
        spark.stop()


if __name__ == "__main__":
    main()

