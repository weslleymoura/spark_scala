/*
Informacoes gerais
Verifique os dados https://archive.ics.uci.edu/ml/datasets/Student+Performance
*/

/*
Import
*/
org.apache.spark.rdd.RDD

/*
Carrega os dados
*/
val dataset = sc.textFile("data/student-mat.csv")

/*
Retorna as primeiras linhas do rdd. Note que quando carregamos os dados, criamos um RDD[String], ou seja, cada linha
do nosso arquivo está em uma posicao do RDD
*/
dataset.take(10)

/*
Se quiser pegar acessar a primeira linha como String, podemos fazer da seguinte forma
*/
dataset.take(10)(0)

/*
Agora vamos retirar o cabeçalho do nosso RDD
*/
val header = dataset.take(1)(0)
val resultados = dataset.filter(x => x!=header)
resultados.take(1)

/*
Agora iremos transformar nosso RDD [String] em um RDD [Array[String]]
*/
val resultados = dataset.map(x => x.split(";"))

/*
Uma vez que temos um RDD [Array[String]] podemos selecionar uma coluna especifica. Lembrando das posicoes das colunas:
school;sex;age;address;famsize;Pstatus;Medu;Fedu;Mjob;Fjob;reason;guardian;traveltime;studytime;failures;schoolsup;famsu
p;paid;activities;nursery;higher;internet;romantic;famrel;freetime;goout;Dalc;Walc;health;absences;G1;G2;G3
*/

// Seleciona a coluna school
resultados.map(x => x(0)).take(10)

// Seleciona a coluna sex
resultados.map(x => x(1)).take(10)

/*
Agora iremos colocar todos os comandos juntos para selecionar a coluna idade
Já convertendo a idade para Int
*/

val header = dataset.take(1)(0)
dataset.filter(x => x != header)
.map(x => x.split(";"))
.map(x => x(2).toInt)
.take(10)

/*
Verifica a quantidade de registros no dataset
*/

dataset.count

/*
Calcula a média da coluna idade manualmente
*/
val header = dataset.take(1)(0)
val contagem = dataset.count
val soma = dataset.filter(x => x != header).map(x => x.split(";")).map(x => x(2).toInt).reduce(_+_)
val media = soma / contagem

/*
Filtra apenas sex = "M"
*/
val header = dataset.take(1)(0)
dataset.filter(x => x != header).map(x => x.split(";")).filter(x => x(1).contains("M")).count
dataset.filter(x => x != header).map(x => x.split(";")).filter(_(1).contains("M")).count

/*
Seleciona as tres primeiras colunas
*/
val resultados = dataset.filter(x => x != header).map(x => x.split(";")).map(x => Array(x(0), x(1), x(2)))

/*
Utiliza uma classe para tratar as colunas do rdd
*/
case class Aluno(school: String, sex: String, age: Int)
val rdd: RDD[Aluno] = resultados.map(x => Aluno(x(0), x(1), x(2).toInt))
rdd.filter(aluno => aluno.sex.contains("M"))

/*
Aplicando sua propria funcao na transformacao map
*/

def convertToLower (x:String): String = {
    x.toLowerCase
}

case class Aluno(school: String, sex: String, age: Int)
val rdd: RDD[Aluno] = resultados.map(x => Aluno(x(0), x(1), x(2).toInt))
rdd.map(aluno => convertToLower(aluno.school)).take(10)

/*
Usando match para selecionar todos os alunos que possuem 15 anos de idade
*/
def checkGender (x:String): Boolean = {
    x match {
        case "15" => true
        case _ => false
    }
}

dataset.filter(x => x != header).map(x => x.split(";")).filter(x => checkGender(x(2))).count

/*
Convert para rdd de tuplas (sexo, failures)
*/
val resultados = dataset.filter(x => x != header).map(x => x.split(";")).map(x => (x(1), x(14).toInt))

/*
Acessando dados da tupla. Retorna os valores (segunda coluna da tupla)
*/
resultados.map(x => x._2).take(10)

/*
Aplicando reduceByKey para somar a quantidade de failures por sexo
*/
resultados.reduceByKey( (x,y) =>  x+y)

/*
Retorna o maior quantidade de failures por sexo. groupByKey vai retornar uma chave e um
valor Iterable com todos os valores encontrados para aquela chave. Iremos aplicar a funcao max
no item Iterable para retornar o maior valor observado para cada sexo
*/
resultados.groupByKey().map(x=>(x._1,x._2.max))

/*
Também podemos ordenar os valores pela chace quando estamos usando um Key-pair RDD.
Neste exemplo vamos selecionar as colunas school e age e em seguida aplicar ordenação em ordem descedente
*/
val resultados = dataset.filter(x => x != header).map(x => x.split(";")).map(x => (x(0), x(2).toInt)).sortByKey(false)
resultados.take(10)

/*
Para ordenar por idade, podemos trocar os atributos chave/valor, fazer a ordenacao e depois reverter a troca
*/
val resultados = dataset.filter(x => x != header).map(x => x.split(";")).map(x => (x(2).toInt, x(0))).sortByKey(false).map(x => (x._2, x._1))
resultados.take(10)

/*
Podemos unir dois RDDs por meio do comando JOIN
*/
//Neste primeiro RDD temos a quantidade de alunos por sexo
val rdd1 = dataset.filter(x => x != header).map(x => x.split(";")).map(x => (x(1), x(1))).groupByKey().map(x => (x._1, x._2.size))

//Neste outro RDD temos o soma de failures por sexo
val rdd2 = dataset.filter(x => x != header).map(x => x.split(";")).map(x => (x(1), x(14).toDouble)).reduceByKey(_+_)

//Agora podemos juntar os dois RDDs pela chave
val rdd3 = rdd1.join(rdd2)

//E podemos acessar os resultados da seguinte forma
rdd3.map(x => (x._1, x._2._1, x._2._2)).take(2)

//Por fim podemos persistir nossos RDDs para otimizar o processamento. Assim as etapas do DAG nao serao
//mais executadas porque o RDD esta persistido. Consulte a documentacao do spark para verificar os tipos
//existentes de persistencia (memory, disk...). O padrao eh memory
rdd3.persist()

//A operação reversa tambem existe
rdd3.unpersist()

// fim ;)
