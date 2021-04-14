import scala.collection.immutable.List
import scala.util.control.Breaks.{break, breakable}

object TopTen {
  val dimension = 10
  def add(
      buffer: List[(String, Double)],
      country: String,
      percIncrease: Double
  ): List[(String, Double)] = {
    var temp: List[(String, Double)] = buffer
    breakable {
      if (buffer.length < dimension) {
        temp = temp :+ (country, percIncrease)
        temp = temp.sortWith(_._2 < _._2)
      } else {
        var index = -1
        buffer.foreach(elem => {
          if (elem._2 < percIncrease) {
            index = buffer.indexOf(elem)
            break
          }
        })
        if (index != -1) {
          temp = temp.updated(index, (country, percIncrease))
        }
      }
    }
    temp
  }

  def getCountries(buffer: List[(String, Double)]): List[String] = {
    var countries = List.empty[String]
    buffer.foreach(score => {
      countries = countries :+ score._1
    })
    countries
  }

  def clear(buffer: List[(String, Double)]): List[(String, Double)] = {
    List.empty
  }

}
