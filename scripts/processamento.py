from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg, trim, round, sum

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Projeto Big Data - Spark") \
    .getOrCreate()

#=================================
# Ler arquivo csv
#=================================
caminho_dados = "dados\dados_loja_fooddelivery.csv"

df = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .option("inferSchema", True) \
    .csv(caminho_dados)

print("Dados carregados com sucesso!")

print("\nEstrutura de DataFrame")
df.printSchema()

print("\nPrimeiro Registro:")
df.show(5)

total_registros = df.count()
print(f"\nTotal de Resgitros: {total_registros}")





#===================================================
# 5. Análise simples (Big Data na prática)
#===================================================

#===================================================
# Total de vendas por restaurante
#===================================================

print("\nTotal de vendas por restaurante:")
df.groupBy("restaurante") \
  .agg(count("*").alias("QTD_Vendas")) \
  .show()

#===================================================
# Valor médio das vendas
#===================================================

print("\nValor médio das vendas:")
#df.select(avg("valor_pedido", 2).alias("media_vendas")).show()
df.select(round(avg("valor_pedido"), 2).alias("media_vendas")).show()

#===================================================
#  Total de pedidos por cidade
#===================================================
pedidos_cidade = df.groupBy("cidade").agg(
    count("*").alias("total_pedidos"),
    round(sum("valor_pedido"),2).alias("valor_total"),
    round(avg("tempo_entrega_min"),2).alias("tempo_medio_entrega")
)

#===================================================
# Desempenho por restaurante
#===================================================
restaurantes = df.groupBy("restaurante").agg(
    count("*").alias("qtd_pedidos"),
    round(avg("avaliacao_cliente"), 2).alias("avaliacao_media"),
    round(avg("tempo_entrega_min"), 2).alias("tempo_medio")
)

restaurantes.show()

pedidos_cidade.show()





spark.stop()