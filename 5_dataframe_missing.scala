"""#######################################################################################################################
Importa funções
#######################################################################################################################"""

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row

"""#######################################################################################################################
Criando um Dataframe na mão
#######################################################################################################################"""

// Carrega dados usando schema
//val schema = df.schema
val schema = new StructType(Array(
new StructField("age", IntegerType, true),
new StructField("job", StringType, true)))

// Cria as linhas do nosso futuro dataframe
val newRows = Seq(
Row(30, "Cientista de dados"),
Row(20, "Dev Java"),
Row(10, null)
)

// Cria um RDD de Rows
val parallelizedRows = spark.sparkContext.parallelize(newRows)

// Cria um dataframe a partir do RDD que criamos anteriormente
val dados_manual = spark.createDataFrame(parallelizedRows, schema)

// Mostra as informações do dataframe
dados_manual.show()

"""#######################################################################################################################
Carrega os dados
#######################################################################################################################"""

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")

"""#######################################################################################################################
Trabalhando com valores ausentes
#######################################################################################################################"""

// Apaga o registro se pelo menos uma coluna possui valor nulo
dados.select($"age", $"marital").na.drop("any")

// Apaga o registro se todas as colunas possuem valores nulos
dados.select($"age", $"marital").na.drop("all")

// Fill
dados_manual.na.fill("Desconhecido").show()

// Especificando valores para cada coluna
val valores_para_preencher = Map("age" -> 0, "job" -> "Desconhecido")
dados_manual.na.fill(valores_para_preencher).show()

"""#######################################################################################################################
Substituindo valores
#######################################################################################################################"""

dados_manual.na.replace("job", Map("Dev Java" -> "Desenvolvedor")).show()
