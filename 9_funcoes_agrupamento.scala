"""#######################################################################################################################
Importando funcoes e classes
#######################################################################################################################"""

import org.apache.spark.sql.functions.{max, min, count, sum, avg, countDistinct, approx_count_distinct, first, last, sumDistinct}
import org.apache.spark.sql.functions.{desc, expr, col, to_date, grouping_id}
import org.apache.spark.sql.functions.{var_pop, stddev_pop, var_samp, stddev_samp, skewness, kurtosis, corr, covar_pop, covar_samp}
import org.apache.spark.sql.functions.{collect_set, collect_list}
import org.apache.spark.sql.functions.{dense_rank, rank, row_number, ntile}
import org.apache.spark.sql.expressions.{Window, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

"""#######################################################################################################################
Lendo dados
#######################################################################################################################"""

// Ler dados
val dataset = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("data/bank-additional-full.csv")

// Print schema
dataset.printSchema

"""#######################################################################################################################
Usando funcoes de agrupamento
#######################################################################################################################"""

// Calcula a média de idade por job
// Podemos usar tanto o método .agg quanto a própria funcao de grupo
dataset.groupBy($"job").agg(avg("age")).show()
dataset.groupBy("job").avg("age").show()

// Acrescenta nome para a coluna agrupada (alias ou withColumnRenamed)
dataset.groupBy("job").agg(avg("age").alias("idade")).show()
dataset.groupBy("job").agg(avg("age")).withColumnRenamed("avg(age)", "idade").show()

// Limita as 5 maiores idades
dataset.groupBy("job").agg(avg("age").alias("idade")).orderBy(desc("idade")).limit(5).show()

// Calcula mais métricas por job
dataset.groupBy("job").agg(min("age").alias("min_age"), max("age").alias("max_age")).show()

// Também é possível agrupar os dados por meio de maps
dataset.groupBy("job").agg("age"-> "min", "age" -> "max").show()

// Calcula countDistinct aproximado
dataset.groupBy("job").agg(approx_count_distinct("age", 0.1)).show()

// Seleciona o primeiro o ultimo registro do dataframe
dataset.select(first("age"), last("age")).show()

// Aplicando funcao de grupo com expr
dataset.select(expr("max(age)")).show()

// Agrupamento de dados complexos. Podemos concatenar lista de valores
dataset.groupBy("job").agg(collect_set("education"), collect_list("education")).show()

// Verifica a distribuicao da variavel resposta
val dataset1 = dataset.groupBy($"y").agg(count($"y").alias("count_class"))
dataset1.select($"y", $"count_class").withColumn("percentage", col("count_class") / sum("count_class").over()).show()

"""#######################################################################################################################
Trabalhando com Window functions (ranqueamento)
#######################################################################################################################"""

// Usando funcoes de ranqueamento (Window Functions)
val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("../guide/book/data/retail-data/all/*.csv").coalesce(5)
val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
val windowSpec = Window.partitionBy("CustomerId", "date").orderBy(col("Quantity").desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)
val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


""" in SQL
SELECT
    CustomerId,
    date,
    Quantity,
    rank(Quantity) OVER (PARTITION BY CustomerId, date ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rank,
    dense_rank(Quantity) OVER (PARTITION BY CustomerId, date ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as dRank,
    max(Quantity) OVER (PARTITION BY CustomerId, date ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as maxPurchase
FROM
    dfWithDate
WHERE
    CustomerId IS NOT NULL
ORDER BY
    CustomerId
"""

"""#######################################################################################################################
Grouping sets
#######################################################################################################################"""

// Grouping sets
val dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

spark.sql("SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
ORDER BY CustomerId DESC, stockCode DESC")

spark.sql("SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode),())
ORDER BY CustomerId DESC, stockCode DESC")

// Rollup
val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity")).selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity").orderBy("Date")
rolledUpDF.show()

rolledUpDF.where("Country IS NULL").show()
rolledUpDF.where("Date IS NULL").show()

// Cube
dfNoNull.cube("Date", "Country").agg(sum(col("Quantity"))).select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

// Grouping Metadata
dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity")).orderBy(expr("grouping_id()").desc).show()

// Pivot
val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()

"""#######################################################################################################################
User-Defined Aggregation Functions (UDAFs)
#######################################################################################################################"""

// Criando a funcao
class BoolAnd extends UserDefinedAggregateFunction {
    def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(StructField("value", BooleanType) :: Nil)

    def bufferSchema: StructType = StructType(
        StructField("result", BooleanType) :: Nil
    )

    def dataType: DataType = BooleanType
    def deterministic: Boolean = true
    def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = true
    }
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
    }
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
    }
    def evaluate(buffer: Row): Any = {
        buffer(0)
    }
}

// Registrando a funcao
val ba = new BoolAnd
spark.udf.register("booland", ba)

// Aplicando a funcao
val df1 = spark.range(1).selectExpr("explode(array(TRUE, TRUE, TRUE)) as t").selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
df1.show()
df1.select(ba(col("t")), expr("booland(f)")).show()
