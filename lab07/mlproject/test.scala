import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.ml.{Pipeline, PipelineModel}

object test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("lab07_test")
      .getOrCreate

    val modelPath = spark.conf.get("spark.mlproject.model_path") // "/user/lev.osipov/laba07/model/"
    val kafkaInputTopic = spark.conf.get("spark.mlproject.kafka_input_topic") // "lev_osipov"
    val kafkaOutputTopic = spark.conf.get("spark.mlproject.kafka_output_topic")  // "lev_osipov_lab04b_out"
    val kafkaServer = "spark-master-1:6667"

    // ------------------------------ Чтение модели

    val model = PipelineModel.load(modelPath)

    // ------------------------------ Чтение данных

    val schema = StructType(Seq(
      StructField("uid", StringType, nullable = true),
      StructField("visits", ArrayType(StructType(Seq(
        StructField("url", StringType, nullable = true),
        StructField("timestamp", StringType, nullable = true))))
        , nullable = true)
    ))

    val exprGetDomain = "regexp_replace(parse_url(visits.url, 'HOST'), '^www.', '')"

    val logs = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", kafkaInputTopic)
      .load

    // ------------------------------ Преобразование данных, применение модели и запись - для каждого батча

    val kafkaSink = logs
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // Подготовка данных
        val preparedLogs = batchDF
          .select(col("timestamp"), from_json(col("value").cast("string"), schema).alias("valueParsed"))
          .select(col("valueParsed.*"))
          .select(col("uid"), explode(col("visits")).alias("visits"))
          .withColumn("domain", expr(exprGetDomain))
          .groupBy(col("uid"))
          .agg(collect_list("domain").alias("domains"))
        // Применение модели
        val result = model.transform(preparedLogs)
        // Запись
        result
          .select(lit(null).cast(StringType), to_json(struct(
            col("uid"),
            col("label_string").alias("gender_age")
          )).alias("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaServer)
          .option("topic", kafkaOutputTopic)
          .save
      }

    kafkaSink.start
  }
}