/*
Informacoes gerais
Verifique os dados https://archive.ics.uci.edu/ml/datasets/Bank+Marketing#
*/

/*******************************************************************************
Importa os pacotes
*******************************************************************************/

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
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
val dataset = spark.read.option("header","true").option("inferSchema","true").csv("data/movie_ratings.csv")

// Exibe a estrutura do dataframe
dataset.printSchema()

/*******************************************************************************
Divide a base entre treino e teste
*******************************************************************************/

val Array(training, test) = dataset.randomSplit(Array(0.8, 0.2))

/*******************************************************************************
Cria e avalia o modelo
*******************************************************************************/

// Define o modelo
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

// Treina o modelo
val model = als.fit(training)

// Garante que nao teremos NaN ao avaliar o modelo
model.setColdStartStrategy("drop")

// Executa o modelo nos dados de teste
val predictions = model.transform(test)

// Calcula o erro
val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")

// Gera 10 recomendacoes de filmes para cada usuario
val userRecs = model.recommendForAllUsers(10)

// Gera 10 recomendacoes de usuarios para cada filme
val movieRecs = model.recommendForAllItems(10)

// Gera 10 recomendacoes de filmes para um grupo especifico de usuarios
val users = dataset.select(als.getUserCol).distinct().limit(3)
val userSubsetRecs = model.recommendForUserSubset(users, 10)

// Gera 10 recomendacoes de usuarios para um grupo especifico de filmes
val movies = dataset.select(als.getItemCol).distinct().limit(3)
val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
