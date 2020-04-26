/*
Informacoes gerais
Verifique os dados https://archive.ics.uci.edu/ml/datasets/Bank+Marketing#
*/

/*******************************************************************************
Importa os pacotes
*******************************************************************************/

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.{DecisionTreeClassifier, DecisionTreeClassificationModel}
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,IndexToString}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
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
Importa os dados e realiza algumas analises basicas
*******************************************************************************/

// Carrega os dados
val dataset = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")

// Exibe a estrutura do dataframe
dataset.printSchema()

// Exibe os primeiros registros do dataframe
dataset.show(2)

// Seleciona apenas os primeiros registros da coluna "age"
dataset.select($"age", $"job").filter($"age" > 55).orderBy($"age".desc).show(2)

// Exemplo de uso da funcao map
dataset.map(row => "Job: " + row.getAs[String]("job")).show(2)

// Verifica a distribuicao da variavel resposta
val dataset1 = dataset.groupBy($"y").agg(count($"y").alias("count_class"))
dataset1.select($"y", $"count_class").withColumn("percentage", col("count_class") / sum("count_class").over()).show()

/*******************************************************************************
Prepara os dados para modelagem
*******************************************************************************/

//Prepara a variavel target
val dataset_1 = dataset.withColumn("label", when($"y" === "no", 0.0).otherwise(1.0))

// Filtra os dados
val dataset_2 = (dataset_1.select($"label", $"age", $"duration", $"pdays", $"job"))

//Elimina registro se algum dos valores for nulo
val dataset_filtered = dataset_2.na.drop()

// Divide a base entre treino e teste
val Array(training, test) = dataset_filtered.randomSplit(Array(0.7, 0.3), seed = 12345)

// Cria um "lavel encoder" para a variavel categorica "job"
val jobIndexer = new StringIndexer().setInputCol("job").setOutputCol("indexedJob").fit(dataset_filtered)

// Caso queira reverter o label encoder, podemos fazer da seguinte forma
val jobConverter = new IndexToString().setInputCol("indexedJob").setOutputCol("job").setLabels(jobIndexer.labels)
//depois temos que passar um dataframe que possua uma coluna chamada "indexedJob": jobConverter.transform (seu_dataframe)

// Cria um VectorAssembler para separar as variaveis independentes
val assembler = (new VectorAssembler().setInputCols(Array("age", "duration", "pdays", "indexedJob")).setOutputCol("features"))

/*******************************************************************************
Configura o pipeline e o metodo de avalidacao dos resultados
*******************************************************************************/

// Define o algoritmo
val dt = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")

// Define o pipeline
val pipeline = new Pipeline().setStages(Array(jobIndexer, assembler, dt))

// Configura o objeto evaluator, responsavel por calcular a funcao de custo
// Note, mesmo sendo um problema de classificacao binaria, estou utilizando a classe MulticlassClassificationEvaluator porque
// quero utilizar a metrica "accuracy", que ate a presente data (Dez 2018) nao faz parte da classe BinaryClassificationEvaluator
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")

/*******************************************************************************
Treina o modelo sem realizar validacao cruzada ou grid search
Gravamos os resultados na variavel "model_sem_cv"
*******************************************************************************/

val model_sem_cv = pipeline.fit(training)

/*******************************************************************************
Treina o modelo com cross validation
Gravamos os resultados na variavel "model_com_cv"
*******************************************************************************/

// Configura o grid search vazio apenas para conseguir executar o cross validator
val paramGridVazio = new ParamGridBuilder().build()

// Configura a validacao cruzada
val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGridVazio).setNumFolds(2)

// Treina o modelo com validacao cruzada
val model_com_cv = cv.fit(training)

// Retorna o resultado do cross validation, ou seja, a media da metrica que foi definida apos k execucoes
model_com_cv.avgMetrics

// Retorna o modelo. Note que neste caso nao definimos opcoes no paramGrid. Portanto, o nosso bestModel na verdade eh o unico modelo que foi treinado
// Note, varios modelos foram treinados para extrair a metrica de avaliacao; porem, apos o cross validation apenas um modelo foi treinado para
// montrar a arvore de decisao final (ja que nao existem mais opcoes configuradas na gridSearch)
val pipelineModel = model_com_cv.bestModel.asInstanceOf[PipelineModel]

// Note, tanto o objeto model_com_cv quanto o objeto pipelineModel podem ser usados para fazer previsoes

/*******************************************************************************
Treina o modelo com grid search e cross validation para encontrar o melhor modelo
Gravamos os resultados na variavel "model_com_cv_grid"
*******************************************************************************/

// Configura o grid search
val paramGrid = new ParamGridBuilder().addGrid(dt.maxDepth,Array(5,10)).build()

// Configura a validacao cruzada
val cv_grid = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(2)

// Treina o modelo com validacao cruzada e grid search
val model_com_cv_grid = cv_grid.fit(training)

// Retorna o resultado do cross validation, ou seja, a media da metrica que foi definida apos k execucoes
// Note, se voce estiver trabalhando com gridSearch no cross validator, voce tera um resultado para cada combinacao do gridSearch
model_com_cv_grid.avgMetrics

// Para retornar o resultado do melhor modelo, neste caso bastaria selecionar o melhor resultado encontrado
//Note, neste caso, sabemos que quanto maior a metrica, mehor. Atencao neste ponto, pois depende de cada metrica (as vezes precisamos o min)
model_com_cv_grid.avgMetrics.max

// Retorna as configuracoes do grid search. Podemos usar o metodo "get" para qualquer parametro definido com o metodo "set"
model_com_cv_grid.getEstimatorParamMaps

//Retorna o melhor modelo encontrado
val bestPipelineModel = model_com_cv_grid.bestModel.asInstanceOf[PipelineModel]

// Acessa o pipeline e retorna nossa arvore de decisao
val stages = bestPipelineModel.stages
val dtStage = stages(2).asInstanceOf[DecisionTreeClassificationModel]

// Exibe o parametro escolhido dentro do mehor modelo
println("maxDepth = " + dtStage.getMaxDepth)

// Note, tanto o objeto model_com_cv_grid quanto o objeto bestPipelineModel podem ser usados para fazer previsoes

/*******************************************************************************
Avalia os resultados
*******************************************************************************/

// Escolhe um dos modelos para usarmos no teste
val model = bestPipelineModel

// Executa o modelo nos dados de teste
val results = model.transform(test)
results.show(10)

// Seleciona as colunas prediction e target para calcular a matriz de confusao. Note que temos que retornar como rdd
val predictionAndLabels = results.select($"prediction",$"label").as[(Double, Double)].rdd

// Instancia um objeto da classe MulticlassMetrics
val metrics = new MulticlassMetrics(predictionAndLabels)

// Exibe a matriz de confusao
println("Confusion matrix:")
println(metrics.confusionMatrix)

// Calcula a precisao
val accuracy = evaluator.evaluate(results)
println(s"Accuracy = ${(accuracy)}")

/*******************************************************************************
Salvando o modelo e carregando para uso futuro
*******************************************************************************/

//Salva o modelo
model.write.overwrite().save("/home/weslley/workspace/spark_udemy/classification/dependencies/spark-binary-classification-model")

//Carrega o modelo
val loadedModel = PipelineModel.load("/home/weslley/workspace/spark_udemy/classification/dependencies/spark-binary-classification-model")
