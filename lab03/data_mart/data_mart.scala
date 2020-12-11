import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._

object data_mart {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("yarn")
      .appName("lab03")
      .getOrCreate

    import spark.implicits._

    // ------------------------------Конфиги и пути

    spark.conf.set("spark.cassandra.connection.host", "10.0.0.5")
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")
    val cassandraTableOpts = Map("table" -> "clients","keyspace" -> "labdata")

    val webLogsPath = "/labs/laba03/weblogs.json"

    val jdbcPgUrlIn = "jdbc:postgresql://10.0.0.5:5432/labdata"
    val jdbcPgUrlOut = "jdbc:postgresql://10.0.0.5:5432/lev_osipov"
    val jdbcPgUser = "lev_osipov"
    val jdbcPgPassword = "q8Fff3sE"

    val esOptions =
      Map(
        "es.nodes" -> "10.0.0.5:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true"
      )


    // ------------------------------ Пользователи


    val getAgeCat = udf {
      age: Int =>
        age match {
          case age if age >= 55 => ">=55"
          case age if age >= 45 => "45-54"
          case age if age >= 35 => "35-44"
          case age if age >= 25 => "25-34"
          case age if age >= 18 => "18-24"
          case _ => "<18"
        }
    }

    val users = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(cassandraTableOpts)
      .load
      .withColumn("age_cat", getAgeCat($"age"))
      .drop($"age")


    // ------------------------------ Логи веб-сайтов


    // получаем домен для visits.url: сначала избавляемся от дублирующего http://, где он есть,
    // потом получаем хост функцией parse_url, потом удаляем www.
    val exprGetDomain = "regexp_replace(parse_url(regexp_replace(visits.url, '^http://http://', 'http://'), 'HOST'), '^www.', '')"

    val webLogsAgg = spark.read
      .json(webLogsPath)
      .select($"uid", explode($"visits").alias("visits"))
      .withColumn("timestamp", $"visits.timestamp")
      .withColumn("domain", expr(exprGetDomain))
      .drop($"visits")
      .dropDuplicates // есть дубли
      .drop($"timestamp")
      .groupBy($"uid", $"domain")
      .agg(count("*").alias("cnt_visits"))


    // ------------------------------ Категории веб-сайтов


    val webCategories = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcPgUrlIn)
      .option("user", jdbcPgUser)
      .option("password", jdbcPgPassword)
      .option("dbtable", "domain_cats")
      .load


    //------------------------------- Посещение веб-сайты по категориям

    // преобразуем наименование колонки category для транспонирования
    val exprGetCatName = "regexp_replace(lower(category), '-| ', '_')"

    val webLogsCategories = webLogsAgg
      .join(broadcast(webCategories), Seq("domain"), "inner")
      .groupBy($"uid")
      .pivot(expr("concat('web_', " + exprGetCatName + ")"))
      .agg(sum($"cnt_visits"))


    //------------------------------- Посещение интернет-магазина по категориям

    val visitsCategories = spark
      .read
      .format("es")
      .options(esOptions)
      .load("visits")
      .filter($"uid".isNotNull)
      .groupBy($"uid")
      .pivot(expr("concat('shop_', " + exprGetCatName + ")"))
      .agg(count("*"))


    //--------------------------------- Результат

    val result = users
      .join(webLogsCategories, Seq("uid"), "left")
      .join(visitsCategories, Seq("uid"), "left")

    //result.show(100, false)

    result.write
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcPgUrlOut)
      .option("user", jdbcPgUser)
      .option("password", jdbcPgPassword)
      .option("dbtable", "clients")
      .save

    //--------------------------------- Освобождаем ресурсы
    spark.stop

  }
}