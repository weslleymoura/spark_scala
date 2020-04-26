/*
Informacoes gerais
Verifique os dados https://archive.ics.uci.edu/ml/datasets/Student+Performance
*/

// Carrega os dados
val dataset = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")

// Exibe a estrutura do dataframe
dataset.printSchema()

// Exibe os primeiros registros do dataframe
dataset.show(2)

// Retorna os primeiros registros
dataset.head(5)

// Retorna algumas estatisticas das colunas do dataframe
dataset.describe().show()

// Retorna as colunas do dataframe
dataset.columns

// Seleciona apenas os primeiros registros da coluna "age"
dataset.select($"age", $"job").filter($"age" > 55).orderBy($"age".desc).show(2)

// Exemplo de uso da funcao map
dataset.map(row => "Job: " + row.getAs[String]("job")).show(2)

// Verifica a distribuicao da variavel resposta
val dataset1 = dataset.groupBy($"y").agg(count($"y").alias("count_class"))
dataset1.select($"y", $"count_class").withColumn("percentage", col("count_class") / sum("count_class").over()).show()

// Define uma nova coluna com um valor constante
val dataset1 = dataset.withColumn("nova_coluna", lit(1))

// Usando map para selecionar uma coluna, transformar para Int e somar os valores via reduce
dataset1.map (row => row.getAs[Int]("nova_coluna")).reduce(_+_)

// Usando a funcao map para aplicar a funcao upper case
dataset1.map (row => row.getAs[String]("poutcome").toUpperCase())

// A mesma operacao pode ser feita com dataframes
dataset1.select(upper($"poutcome")).show(1)

// Aplica a funcao size na coluna poutcome para cada observacao
dataset1.map (row => row.getAs[String]("poutcome").size)
