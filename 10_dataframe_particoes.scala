"""#######################################################################################################################
Importa funções
#######################################################################################################################"""

import org.apache.spark.sql.functions._

"""#######################################################################################################################
Carregando os dados
#######################################################################################################################"""

val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")

"""#######################################################################################################################
Trabalhando com partições
#######################################################################################################################"""

// Consulta o número de partições
dados.rdd.getNumPartitions
dados.rdd.partitions.size
dados.rdd.glom().map(_.length).collect()

// Reduzindo o número de partições de um dataframe
val dados1 = dados.coalesce(1)
dados1.rdd.getNumPartitions
dados1.rdd.glom().map(_.length).collect()

// Aumentando o número de partições
val dados2 = dados.repartition(4)
dados2.rdd.getNumPartitions
dados2.rdd.glom().map(_.length).collect()

/*
Diferença entre coalesce e repartition:
Coalesce vai tentar organizar os dados das partições existentes apenas transferindo dados de algumas partições (que serão eliminadas)
para outras (que serão incrementadas). Não é necessário uma operação de full shuffle para reorganizar os dados e por isso é mais rápido do que o repartition.
Por este mesmo motivo, não é possível usar o coalesce para aumentar o número de partições.
Já o repartition faz um full suffle nas partições para reorganizá-las.
*/

// Criando partições com base no valor de uma coluna.
// Por padrão, quando fazemos particionamento por coluna, Spark vai criar 200 partições.
// Neste caso, temos uma partição para cada valor da coluna "marital" e o restante são partições vazias
val dados3 = dados.repartition($"marital")
dados3.rdd.getNumPartitions
dados3.rdd.glom().map(_.length).collect()

// Se você quiser, pode dar um coalesce para "countDistinct(marital) + 1" para diminuir o número de particoes e manter seu particionamento por "marital"
val dados4 = dados3.coalesce(5)

/*
Considerações sobre particionamento

Em uma situação normal de um ambiente de cluster, Spark não vai tentar usar todos os recursos disponíveis para
executar suas operações. Isso só vai acontecer se você configurar o nível de paralelismo de suas operações com um valor
alto suficiente para que o cluster seja utilizado por inteiro.

Por exemplo, Spark define automaticamente o número de tarefas map para rodar em um determinado arquivo de acordo com o
seu tamanho (embora seja possível configurar este comportamento por meio do parâmetro opcional SparkContext.textFile).
Já para operações de reduce Spark automaticamente usa a quantidade de partições do maior RDD envolvido na operação.

Você pode configurar o nível de paralelismo do Spark por meio da configuração spark.PairRDDFunctions ou por meio da
propriedade spark.default.parallelism

Na documentação do Spark é recomendado 1-3 tarefas por CPU core existente no cluster.
Você pode usar esta lógica para particionar seus dados e tomar melhor proveito do cluster.

ou seja,

n_partitions = n_cpu_cores * 3

Por fim, mais uma dica sobre particionamento:
Logo após filtrar um dataframe com grande volume de dados, você deve pensar se deve reparticionar o seu dataframe menor ou não
(em muitos casos vale a pena reorganizar as partições para obter ganhos de desempenho)

Parte da lógica desta aula teve como base o seguinte artigo:
https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
*/
