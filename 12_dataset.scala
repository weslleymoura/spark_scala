/*
Dizemos que Dataframes são Datasets do tipo Row, porque cada linha do meu Dataframe é composta por um objeto da classe Row
Quando utilizamos Datasets, podemos alterar este objeto do tipo Row para um objeto definido por nós mesmos. Tecnicamente, este objeto será
definido por uma Case Class (se for escrito em Scala) ou um Java Bean (se for escrito em Java)
Esta flexibilidade vem com um custo na questão do desempenho

Portanto, você deve preferir usar Datasets se:
- Não for possível realizar sua operação com Dataframes ou
- Você precisa de tipos de dados bem definidos (type-safety). Por exemplo, uma subtração entre duas Strings vai falhar durante a compilação
do código (e não em tempo de execução).

Sempre leve esses pontos em consideracao, já que o uso de datasets prejudicará o desempenho.
*/


case class Bank (
    AGE: Int,
    JOB: String,
    MARITAL: String,
    EDUCATION: String,
    DEFAULT: String,
    HOUSING: String,
    LOAN: String,
    CONTACT: String,
    MONTH: String,
    DAY_OF_WEEK: String,
    DURATION: String,
    CAMPAIGN: Int,
    PDAYS: Int,
    PREVIOUS: Int,
    POUTCOME: String,
    `EMP.VAR.RATE`: Double,
    `CONS.PRICE.IDX`: Double,
    `CONS.CONF.IDX`: Double,
    EURIBOR3M: Double,
    `NR.EMPLOYED`: Double,
    Y: String
)


// Carrega arquivo CSV
val dados = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("data/bank-additional-full.csv")

// Converte para Dataset
val dataset = dados.as[Bank]

// Verificando os tipos de objetos
dados
dataset

// Exibindo alguns registros do dataset
dataset.show(5)

/*
Basicamente, tudo que podemos fazer com Dataframes também é aceito em Datasets.
Porém, datasets possuem mais possibilidaes para se trabalhar com ações e transformações
No exemplo abaixo aplicaremos um filtro por meio de uma função
*/

// Exemplo que filtro por meio de uma função
def ageGreaterThan40 (row: Bank): Boolean = {
    return row.AGE > 40
}

// Aplica filtro usando uma função
val dataset2 = dataset.filter(row => ageGreaterThan40(row))

// Confere se o dataset2 realmente não possui os dados que foram filtrados
dataset.filter(row => row.AGE < 40).show(5)

/*
Também podemos usar o comando .map para aplicar transformações no dataset
Usando a função map para extrair informações do dataset
*/
dataset.map(row => row.MARITAL).show(5)
