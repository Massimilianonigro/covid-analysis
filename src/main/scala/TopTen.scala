import scala.::
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break

object TopTen {
  val dimension = 10
  var buffer: ArrayBuffer[(String, Double)] = ArrayBuffer(dimension)

  def add(country: String, percIncrease: Double): Unit = {
    if (buffer.length < dimension) {
      buffer += (country, percIncrease)
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
