import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.Instant
import java.time.ZoneId

object filter {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("lab04a")
      .getOrCreate

    val kafkaTopic = spark.conf.get("spark.filter.topic_name")
    val offset = spark.conf.get("spark.filter.offset")
    val outputDir = spark.conf.get("spark.filter.output_dir_prefix")

    // ------------------------------ Валидация параметров

    if (kafkaTopic == null || offset == null || outputDir == null) {
      throw new IllegalArgumentException("Incorrect params!")
    }
    if (kafkaTopic.isEmpty) {
      throw new IllegalArgumentException("Incorrect topic name!")
    }
    if (offset.isEmpty || (Try(offset.toInt).toOption.isEmpty && offset != "earliest")) {
      throw new IllegalArgumentException("Incorrect offset!")
    }
    if (outputDir.isEmpty) {
      throw new IllegalArgumentException("Incorrect output dir!")
    }

    // ------------------------------ Подключение и чтение

    val kafkaServer = "spark-master-1:6667"
    val kafkaOffset = if (offset == "earliest") offset else " { \"%s\": { \"0\": %s } } ".format(kafkaTopic, offset)
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> kafkaServer,
      "subscribe" -> kafkaTopic,
      "startingOffsets" -> kafkaOffset
    )

    val schema = StructType(Seq(
      StructField("event_type", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("item_id", StringType, nullable = true),
      StructField("item_price", StringType, nullable = true),
      StructField("uid", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true)
    ))

    val getDate = udf {
      timestamp: String =>
        val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        Try(timestamp.toLong).toOption match {
          case Some(d) => formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(d), ZoneId.of("UTC")))
          case None => ""
        }

    }

    val logs = spark.read
      .format("kafka")
      .options(kafkaParams)
      .load
      .withColumn("valueParsed", from_json(col("value").cast("string"), schema))
      .select(col("valueParsed.*"), getDate(col("valueParsed.timestamp")).alias("date"))
      .select(col("*"), col("date").alias("p_date"))

    // ------------------------------ Запись

    logs
      .filter(col("event_type") === lit("view"))
      .write
      .partitionBy("p_date")
      .json(outputDir + "/view")

    logs
      .filter(col("event_type") === lit("buy"))
      .write
      .partitionBy("p_date")
      .json(outputDir + "/buy")

    // ------------------------------ Освобождаем ресурсы
    spark.stop
  }
}
