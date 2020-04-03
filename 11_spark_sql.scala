"""#######################################################################################################################
Acessando prompt de comando do Spark SQL
#######################################################################################################################"""

/*
Você pode acessar o prompt de comando do Spark SQL por meio do comando
spark-sql

Uma vez dentro do spark-sql, você tem praticamente todas as principais operações de um banco de dados relacional,
como criação de banco de dados, tabelas, views, etc. Não veremos estes comandos neste curso, pois seria necessário um curso
específico este módulo do Spark.

Outra informação importante é que você pode conectar outras ferramentas (por exemplo Tableau) no seu banco de dados/tabelas
do spark-sql por meio de uma conexão Thrift JDBC
*/

"""#######################################################################################################################
Carregando os dados
#######################################################################################################################"""

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")

"""#######################################################################################################################
Registrando a tabela e executando consultas SQL
#######################################################################################################################"""

dados.createOrReplaceTempView("dados")
val teste = spark.sql("select * from dados limit 10")
