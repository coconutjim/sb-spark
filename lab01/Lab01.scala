import java.io._

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object Lab01 {
  def main(args: Array[String]): Unit = {

    val filmIndex = 1
    val markIndex = 2
    val fileNameIn = "u.data"
    val fileNameOut = "lab01.json"
    val targetFilmId = "181"

    try {
      // Получаем оценки из файла
      case class Mark(filmId: String, mark: String)
      val source = scala.io.Source.fromFile(fileNameIn)
      val data = source.getLines.map(x => {
        val arr = x.split("\t")
        Mark(arr(filmIndex), arr(markIndex))
      }).toList

      // Делаем группировку "Оценка, Кол-во" и возвращаем список кол-в по возрастанию оценок
      def aggAndSort(marks: Seq[Mark]) = marks.groupBy(_.mark).mapValues(_.size).toSeq.sortBy(_._1).map(_._2)
      val hist_all = aggAndSort(data)
      val hist_film = aggAndSort(data.filter(_.filmId == targetFilmId)) // можно использовать == для строк

      // Преобразуем в JSON и пишем в файл
      val json = ("hist_film" -> hist_film) ~ ("hist_all" -> hist_all)
      val jsonString = pretty(render(json))
      val pw = new PrintWriter(new File(fileNameOut))
      pw.write(jsonString)
      pw.close() // судя по всему, в finally прятать не обязательно..

    }
    catch {
      case _: FileNotFoundException => println("File not found!")
      case _: IOException => println("Error while working with file!")
      case _: ArrayIndexOutOfBoundsException => println("Incorrect format of data!")

    }
  }
}