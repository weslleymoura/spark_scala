"""#######################################################################################################################
Importando funcoes e classes
#######################################################################################################################"""

import org.apache.spark.sql.functions.expr

"""#######################################################################################################################
Cria dataframes de exemplo
#######################################################################################################################"""

val pessoa = Seq((1, "Mateus", 0, Seq(300)),(2, "Patricia", 2, Seq(500, 400)),(3, "Rafaela", 1, Seq(500)),(4, "Henrique", -1, Seq(-1))).toDF("id", "nome", "curso", "id_funcoes")
val curso = Seq((0, "Bacharelado"),(1, "Mestrado"),(2, "Doutorado"),(3, "Tecnólogo")).toDF("id", "curso")
val funcao = Seq((500, "Gerente"),(400, "Dono"),(300, "Funcionário")).toDF("id", "funcao")

"""#######################################################################################################################
INNER JOIN
#######################################################################################################################"""

val joinExpression = pessoa.col("curso") === curso.col("id")
var joinType = "inner"
pessoa.join(curso, joinExpression, joinType).show()

"""#######################################################################################################################
OUTER JOIN
#######################################################################################################################"""

val joinExpression = pessoa.col("curso") === curso.col("id")
var joinType = "outer"
pessoa.join(curso, joinExpression, joinType).show()

"""#######################################################################################################################
LEFT OUTER JOIN
#######################################################################################################################"""

val joinExpression = pessoa.col("curso") === curso.col("id")
var joinType = "left_outer"
pessoa.join(curso, joinExpression, joinType).show()

"""#######################################################################################################################
RIGHT OUTER JOIN
#######################################################################################################################"""

val joinExpression = pessoa.col("curso") === curso.col("id")
var joinType = "right_outer"
pessoa.join(curso, joinExpression, joinType).show()

"""#######################################################################################################################
CROSS JOIN
#######################################################################################################################"""

pessoa.crossJoin(curso).show()

"""#######################################################################################################################
LEFT SEMI JOIN
#######################################################################################################################"""

// Lista todos os registros do dataframe da esquerda que possuem match no dataframe da direita.
// Podemos olhar para este JOIN como uma espécie de filtro (não é efetivamente um JOIN como estamos acostumados)
val joinExpression = pessoa.col("curso") === curso.col("id")
var joinType = "left_semi"
pessoa.join(curso, joinExpression, joinType).show()

"""#######################################################################################################################
LEFT ANTI JOIN
#######################################################################################################################"""

// É o oposto do SEMI JOIN, no sentido de que apenas os registros que NÃO possuem match no dataframe da direta serão retornados
val joinExpression = pessoa.col("curso") === curso.col("id")
var joinType = "left_anti"
pessoa.join(curso, joinExpression, joinType).show()

"""#######################################################################################################################
JOIN em tipos de dados complexos
#######################################################################################################################"""

pessoa.withColumnRenamed("id", "pessoaId").join(funcao, expr("array_contains(id_funcoes, id)")).show()

"""#######################################################################################################################
Tratado possível problema de nome de coluna duplicado
#######################################################################################################################"""

val joinExpression = pessoa.col("curso") === curso.col("id")
var joinType = "inner"
val resultado = pessoa.join(curso, joinExpression, joinType)

// Note que o resultado é um dataframe com duas colunas com o mesmo nome (curso)
resultado.show()

// Erro: Neste cenário, o comando abaixo não vai funcionar porque spark não saberá quais das duas colunas devem ser retornadas
//resultado.select("curso").show()

// Podemos excluir uma das colunas após a junçao
val resultado = pessoa.join(curso, joinExpression, joinType).drop(pessoa.col("curso"))
resultado.select("curso").show()
