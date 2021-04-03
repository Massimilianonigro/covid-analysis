import scala.::
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.{break, breakable}

object TopTen {
  val dimension = 10
  var buffer: ArrayBuffer[(String, Double)] =
    ArrayBuffer.empty[(String, Double)]

  def add(country: String, percIncrease: Double): Unit = {
    breakable {
      if (buffer.length < dimension) {
        buffer = buffer :+ (country, percIncrease)
        buffer = buffer.sortWith(_._2 < _._2)
      } else {
        var index = -1
        buffer.foreach(elem => {
          if (elem._2 < percIncrease) {
            index = buffer.indexOf(elem)
            break
          }
        })
        if (index != -1) {
          buffer(index) = (country, percIncrease)
        }
      }
    }
  }

  def getCountries: ArrayBuffer[String] = {
    var countries = ArrayBuffer.empty[String]
    buffer.foreach(score => {
      countries = countries :+ score._1
    })
    countries
  }

  def clear(): Unit = {
    buffer.clear()
  }

}
