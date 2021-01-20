import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel

object dashboard {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("lab08")
      .getOrCreate

    val modelPath = spark.conf.get("spark.mlproject.model_path") // "/user/lev.osipov/laba07/model/"
    val inputPath = spark.conf.get("spark.mlproject.input_path") // "/labs/laba08"
    val esOptions =
      Map(
        "es.nodes" -> "10.0.0.5:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true",
        "es.net.http.auth.user" -> "lev.osipov",
        "es.net.http.auth.pass" -> "q8Fff3sE"
      )

    // ------------------------------ Чтение модели

    val model = PipelineModel.load(modelPath)

    // ------------------------------ Чтение данных

    val exprGetDomain = "regexp_replace(parse_url(visits.url, 'HOST'), '^www.', '')"

    val logs = spark
      .read
      .json(inputPath)
      .select(col("date"), col("uid"), explode(col("visits")).alias("visits"))
      .withColumn("domain", expr(exprGetDomain))
      .groupBy(col("date"), col("uid"))
      .agg(collect_list("domain").alias("domains"))

    // ------------------------------ Применение модели

    val result = model.transform(logs)

    // ------------------------------ Запись

    result
      .select(col("date"), col("uid"), col("label_string").alias("gender_age"))
      .write
      .format("es")
      .options(esOptions)
      .save("lev_osipov_lab08/_doc")

    // ------------------------------ Освобождение ресурсов
    spark.stop
  }
}