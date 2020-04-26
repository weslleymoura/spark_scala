/*
Informacoes gerais
Verifique os dados http://archive.ics.uci.edu/ml/datasets/Wholesale+customers
*/

/*******************************************************************************
Importa os pacotes
*******************************************************************************/

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder,PCA,StandardScaler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.log4j._
import spark.implicits._

/*******************************************************************************
Configuracoes
*******************************************************************************/

// Configura o nivel das notificacoes
Logger.getLogger("org").setLevel(Level.ERROR)

// Cria spark session
val spark = SparkSession.builder().getOrCreate()

/*******************************************************************************
Importa e prepara os dados para modelagem
*******************************************************************************/

// Carrega os dados
val dataset = spark.read.option("header","true").option("inferSchema","true").csv("data/Wholesale customers data.csv")

// Seleciona as colunas que serao utilizadas
val dataset_filtered = dataset.select($"Fresh", $"Milk", $"Grocery", $"Frozen", $"Detergents_Paper", $"Delicassen")

// Cria um VectorAssembler para separar as variaveis. Note, neste tipo de modelo não existe variável dependente (label/target)
val assembler = new VectorAssembler().setInputCols(Array("Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen")).setOutputCol("features")

// Executa a transformacao das variaveis para formar a base de treino no formato que pode ser utilizado pelo algoritmo
val training_data = assembler.transform(dataset_filtered).select("features")

/*******************************************************************************
Cria e treina o modelo
*******************************************************************************/

// Cria o modelo
val kmeans = new KMeans().setK(4).setSeed(1L)

// Treina o modelo
val model = kmeans.fit(training_data)

/*******************************************************************************
Avalia os resultados
*******************************************************************************/

// Avalia os resultados por meio da metrica Sum of Squared Errors (SSE).
val SSE = model.computeCost(training_data)
println(s"SSE = $SSE")

// Exibe os centroides de cada cluster
println("Cluster Centers: ")
model.clusterCenters.foreach(println)

/*******************************************************************************
Salva e carrega o modelo
*******************************************************************************/

//Salva o modelo
model.write.overwrite().save("/home/weslley/workspace/spark_udemy/clustering/dependencies/spark-clustering-model")

//Carrega o modelo
val loadedModel = KMeansModel.load("/home/weslley/workspace/spark_udemy/clustering/dependencies/spark-clustering-model")

//Simula a execucao do modelo em uma outra base de dados (neste caso estamos apenas reutilizando a base de dados de treino)
val results = loadedModel.transform(training_data)

//Verifica os clusters associados a cada observacao
results.select($"prediction").show(10)

/*******************************************************************************
Aplica Principal Component Analysis (PCA) para reduzir a dimensionalidade dos dados
que foram utilizados neste modelo. Podemos fazer este processo por dois motivos principais:
  1) para plotar os clusters em um scatter plot com duas dimensoes
  2) para validar o numero de clusters definidos
*******************************************************************************/

// Normaliza os dados para antes de aplicar PCA
val scaler = (new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(false))
val scalerModel = scaler.fit(training_data)

// Aplica a normalizacao nos dados de treino para que cada variavel tenha desvio padrao igual a zero
val scaledData = scalerModel.transform(training_data)

// Aplica PCA para encontrar os 4 componentes principais
val pca = (new PCA().setInputCol("scaledFeatures").setOutputCol("pcaFeatures").setK(4).fit(scaledData))

// Verifica a variancia que cada componente consegue explicar
val variancia_por_componente = pca.explainedVariance
val variancia_por_componente_array = variancia_por_componente.toArray
val total = variancia_por_componente_array.reduce(_ + _)
println(s"Total da variancia explicada eh: ${total}")

// Se os componentes principais conseguirem explicar pelo menos 90% dos dados, pode ser um indicativo de que
// o numero de clusters eh ideal. Adicionalmente pode-se aplicar o elbow method para explorar mais o numero ideal de cluster
// https://stats.stackexchange.com/questions/157621/how-would-pca-help-with-a-k-means-clustering-analysis

/*******************************************************************************
Monta um dataframe com os resultados do PCA
*******************************************************************************/

// Aplica PCA nos dados
val pcaDF = pca.transform(scaledData)

// Cria um dataframe apenas com a coluna que armazena os resultados do PCA
val resultPCA = pcaDF.select("pcaFeatures")

// Verifica os resultsdos
resultPCA.rdd.map(row => row.get(0)).take(1)
