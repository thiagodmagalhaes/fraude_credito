from pyspark.sql import SparkSession

def criar_sessao_spark():
    '''
    Essa função cria a sessão do spark e retorna ela.
    Ela define os configurações do pyspark
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
    


def main():
    try:
        print("Iniciando execução do código. . .")
        spark = criar_sessao_spark()

    except Exception as e:
        print(f"EXECUÇÕO INTERROMPIDA!!!  Erro na execução do código: {e}")
        #enviar_email(e)
        spark.stop()

    finally:
        print("O código foi finalizado!!!!")
        spark.stop()


if __name__ == "__main__":
    main()

