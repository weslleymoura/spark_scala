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
new StructField("id", IntegerType, true),
new StructField("job", StringType, true)))

val newRows = Seq(
Row(30, "Cientista de dados"),
Row(20, "Dev Java"),
Row(10, null)
)

val parallelizedRows = spark.sparkContext.parallelize(newRows)
val dados_manual = spark.createDataFrame(parallelizedRows, schema)
dados_manual.show()

"""#######################################################################################################################
Trabalhando dados complexos
#######################################################################################################################"""

// Colunas do tipo struct -> Colocando id e job numa unica coluna do tipo struct
val dados_complexos = dados_manual.select(struct("id", "job").alias("complexo"))
dados_complexos.select("complexo.job").show(5)

// Função Split -> Separa as informações da coluna job quando encontrar espaço. O resultado será um array
val dados_split = dados_manual.select(split(col("job"), " ").alias("split_column"))
dados_split.show()

// Podemos referenciar nosso array pela sua posição
dados_split.selectExpr("split_column[0]").show()

// Tammém podemos contar quantas posições temos em cada array
dados_split.select(size($"split_column")).show()

// A função array_contains nos permite verificar se um determinado elemento existe no array
dados_split.select(array_contains($"split_column", "Dev")).show()

// Por fim, a função explore pode ser aplicada em colunas Array para extrair cada elemento em uma nova linha
dados_split.select(explode($"split_column")).show()

// Se você quiser trabalhar com conjunto de valores do tipo "key-value", pode trabalhar com o tipo complexo MAP
val dados_map = dados_manual.select(map(col("id"), col("job")).alias("mapa"))
dados_map.show()

// Depois podemos usar a coluna map da seguinte forma
dados_map.selectExpr("mapa['30']").show()

// Também é possível aplicar a função explode em uma columa map para retornar as colunas key -> value
dados_map.selectExpr("explode(mapa)").show()

"""#######################################################################################################################
User Defined Functions (UDF)
#######################################################################################################################"""

// Criando uma função
def incrementaId (number:Integer):Integer = number + 1
adicionaIdade(10)

// Registra a função para ser aplicada em Dataframes
val incrementaIdUDF = udf(incrementaId(_:Integer):Integer)

// Usando a função
dados_manual.select($"id", incrementaIdUDF(col("id"))).show(5)

// Para funcionar no contexto SQL, precisamos registrár a função no Spark SQL
spark.udf.register("incrementaId", incrementaId(_:Integer):Integer)

// Agora também podemos usá-la no SQL Context
dados_manual.selectExpr("incrementaId(id)").show(5)
