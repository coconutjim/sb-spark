import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

import scala.util.Try

import java.net.URLDecoder.decode
import java.net.URL

import java.io.File
import java.io.PrintWriter

object Lab02 {

  case class Log (uid: String, timestamp: String, url: String)

  def main(args: Array[String]): Unit = {

    // Параметры путей
    val usersPath = "/labs/laba02/autousers.json"
    val logsPath = "/labs/laba02/logs/"
    val outputPath = "/data/home/lev.osipov/laba02_domains.txt"

    // Инициализация Spark
    val spark = SparkSession
      .builder
      .master("yarn")
      .appName("lab02")
      .getOrCreate
    import spark.implicits._

    // Читаем пользователей
    val autoUsers = spark.read
      .json(usersPath)
      .select(explode('autousers).alias("uid"), lit(1).alias("auto_flg"))

    // Читаем логи
    val logSchema = ScalaReflection.schemaFor[Log].dataType.asInstanceOf[StructType]
    val csvOptions = Map("header" -> "true", "inferSchema" -> "false", "delimiter" -> "\t")
    val logsRaw = spark.read
      .options(csvOptions)
      .schema(logSchema)
      .csv(logsPath)

    // Чистим логи
    val getDomain = udf {
      (url: String) =>
        Try(new URL(decode(url)).getHost).toOption match {
          case Some(s) => s.replaceAll("^www.", "")
          case None => null
        }
    }
    val logs = logsRaw
      .na.drop("any")
      .dropDuplicates
      .select('uid, getDomain('url).alias("domain"))
      .filter('domain.isNotNull)

    // Считаем релевантность
    val result = logs
      .join(broadcast(autoUsers), Seq("uid"), "left")
      .select('domain, coalesce('auto_flg, lit(0)).alias("auto_flg"))
      .withColumn("cnt_flags", sum('auto_flg).over(Window.partitionBy()))
      .groupBy('domain, 'cnt_flags)
      .agg(round(sum('auto_flg) * sum('auto_flg) * lit(1.0) / count("*") / 'cnt_flags, 20)
        .alias("relevance"))
      .orderBy('relevance.desc, 'domain.asc)
      .select('domain, format_number('relevance, 20).alias("relevance"))

    // Пишем в файл
    val pw = new PrintWriter(new File(outputPath))
    try {
      result.take(200).foreach { x => pw.append(x.mkString("\t") + "\n") }
    }
    finally {
      pw.close()
    }

    // Освобождаем ресурсы
    spark.stop
  }
}
