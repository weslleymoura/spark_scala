"""#######################################################################################################################
Importa funções
#######################################################################################################################"""

import org.apache.spark.sql.functions._
import spark.implicits._

"""#######################################################################################################################
Carrega os dados
#######################################################################################################################"""

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")

"""#######################################################################################################################
Função map e reduce
#######################################################################################################################"""

// Exemplo de uso da funcao map
dados.map(row => "Job: " + row.getAs[String]("job")).show(2)

// Outra forma de conseguir o mesmo resultado usando as abordagens que aprendemos até agora
//dados.select($"job").withColumn("teste", concat(lit("Job: "), $"job")).show(5)

// Usando a funcao map para aplicar a funcao upper case
dados.map (linha => linha.getAs[String]("poutcome").toUpperCase()).show(5)
// Outra forma de conseguir o mesmo resultado usando as abordagens que aprendemos até agora
//dados.select(upper($"poutcome")).show(5)

// Aplica a funcao size na coluna poutcome para cada observacao e depois aplicando reduce para somar a quantidade de caracteres
dados.map(row => row.getAs[String]("poutcome").size).reduce(_+_)
// Outra forma de conseguir o mesmo resultado usando as abordagens que aprendemos até agora
//dados.select(sum(length($"poutcome"))).show()
