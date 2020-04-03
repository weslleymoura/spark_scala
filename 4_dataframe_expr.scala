"""#######################################################################################################################
Importa funções
#######################################################################################################################"""

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row

"""#######################################################################################################################
Carrega os dados
#######################################################################################################################"""

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")

"""#######################################################################################################################
Trabalhando com expressões
#######################################################################################################################"""

// Calculando expressões
dados.select($"age" + 10).show()
dados.select(expr("age + 10")).show(5)

// Usando selectExpr. Aqui você pode usar expressões SQL
dados.selectExpr(
"*",
"(age > 40) as idade_maior_40")
.show(5)

// Selecionando a maior idade com selectExpr
dados.selectExpr("max(age)").show()

// Utilizando palavras reservados
dados.selectExpr("age as `idade com espaço`").show(2)

"""#######################################################################################################################
Selecionando uma amostra dos dados e dividindo os dados em amostras diferentes
#######################################################################################################################"""

// Selecionando uma amostra
val seed = 2019
val withReplacement = false
val fraction = 0.1
dados.sample(withReplacement, fraction, seed).count()

// Dividindo os dados em amostras diderentes
val seed = 2019
val dataFrames = dados.randomSplit(Array(0.25, 0.75), seed)
dataFrames(0).count()
dataFrames(1).count()

"""#######################################################################################################################
Unindo linhas
#######################################################################################################################"""

val dados1 = dataFrames(0).union(dataFrames(1)).where($"marital" === "married")
dados1.show(5)

"""#######################################################################################################################
Limitando o resultado
#######################################################################################################################"""

dados1.limit(5)
