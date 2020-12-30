import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.SparseVector

object features {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("lab06")
      .getOrCreate

    import spark.implicits._

    val webLogsPath = "/labs/laba03/weblogs.json"
    val usersItemsPath = "/user/lev.osipov/users-items/20200429"
    val outputPath = "/user/lev.osipov/features"

    // ------------------------------ Чтение и парсинг логов

    val exprGetDomain = "regexp_replace(parse_url(visits.url, 'HOST'), '^www.', '')"

    val logs = spark
      .read
      .json(webLogsPath)
      .select(col("uid"), explode(col("visits")).alias("visits"))
      .withColumn("timestamp", col("visits.timestamp"))
      .withColumn("domain", expr(exprGetDomain))
      .drop(col("visits"))

    // ------------------------------ Подготовка агрегатов по временным отрезкам

    val dayOfWeeks = "Monday" :: "Tuesday" :: "Wednesday" :: "Thursday" :: "Friday" :: "Saturday" :: "Sunday" :: Nil
    val dayOfWeeksAggSum = dayOfWeeks.map(x =>
      expr(s"SUM(CASE WHEN dayOfWeek = '${x}' THEN 1 ELSE 0 END) AS web_day_${x.substring(0, 3).toLowerCase}"))

    val hours = 0 to 23
    val hoursAggSum = hours.map(x =>
      expr(s"SUM(CASE WHEN hour = '${x}' THEN 1 ELSE 0 END) AS web_hour_${x}"))

    val workHoursStr = (9 to 17).mkString(", ")
    val fractionWorkAgg =
      expr(s"SUM(CASE WHEN hour IN (${workHoursStr}) THEN 1 ELSE 0 END) / COUNT(*) AS web_fraction_work_hours") :: Nil
    val eveningHoursStr = (18 to 23).mkString(", ")
    val fractionEveningAgg =
      expr(s"SUM(CASE WHEN hour IN (${eveningHoursStr}) THEN 1 ELSE 0 END) / COUNT(*) AS web_fraction_evening_hours") :: Nil

    // ------------------------------ Агрегирование по временным отрезкам

    val udfToDateUTC = udf((epochMilliUTC: Long) => {
      val dateFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(java.time.ZoneId.of("UTC"))
      dateFormatter.format(java.time.Instant.ofEpochMilli(epochMilliUTC))
    })

    val logAgg = logs
      .withColumn("utcTime", udfToDateUTC(col("timestamp")))
      .withColumn("dayOfWeek", date_format(col("utcTime"), "EEEE"))
      .withColumn("hour", hour(col("utcTime")))
      .groupBy(col("uid"))
      // Колонка foo_mock нужна только чтобы попасть в сигнатуру метода agg, просто :_* сделать не позволяет
      .agg(lit(0).alias("foo_mock"),
        (dayOfWeeksAggSum ++ hoursAggSum ++ fractionWorkAgg ++ fractionEveningAgg): _*)
      .drop(col("foo_mock"))

    // ------------------------------ Расчет фичей по доменам с помощью CountVectorizer

    val topDomains = logs
      .filter(col("domain").isNotNull)
      .groupBy(col("domain"))
      .agg(count("*").alias("cnt"))
      .sort(col("cnt").desc)
      .limit(1000)

    val topDomainsLogAgg = logs
      .join(broadcast(topDomains), Seq("domain"), "inner")
      .groupBy(col("uid"))
      .agg(collect_list("domain").alias("domains"))

    val topDomainsArr = topDomains.select(col("domain")).as[String].collect.sorted
    val cvModel: CountVectorizerModel = new CountVectorizerModel(topDomainsArr)
      .setInputCol("domains")
      .setOutputCol("features")

    val getResVector = udf((v: SparseVector) => {
      val res = Array.fill(1000)(0)
      v.indices.indices.foreach { i => res(v.indices(i)) = v.values(i).toInt }
      res
    })

    val domainFeatures = cvModel
      .transform(topDomainsLogAgg)
      .withColumn("domain_features", getResVector(col("features")))
      .drop("domains", "features")

    // ------------------------------ Чтение матрицы users-items

    val usersItems = spark.read.parquet(usersItemsPath)

    // ------------------------------ Соединение всех атрибутов

    val result = logAgg
      .join(domainFeatures, Seq("uid"), "left")
      .join(usersItems, Seq("uid"), "left")

    // ------------------------------ Запись в файл

    result
      .repartition(200)
      .write
      .mode("append")
      .parquet(outputPath)

    // ------------------------------ Освобождение ресурсов

     spark.stop
  }
}