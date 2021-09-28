import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object Main {

  def main(args: Array[String]): Unit = {
    time {
      // initialize Spark session
      val spark = initSession()
      spark.sparkContext.setLogLevel("ERROR")
      val countries: String = args(0)
      val session = spark.sqlContext.sparkSession

      // read dataset from input
      var df = spark.read.csv(
        "/home/mpiuser/cloud/data.csv"
      )

      // remove unused columns, rename used ones
      df = PreprocessingHandler.dfPreprocessing(df, countries)
      df.createOrReplaceTempView("df")
      val dbManipulator = new DbManipulator(df, session)

      // create view to manipulate and query
      val (country_views, data_date_range) =
        dbManipulator.computeMovingAverageAndPercentageIncrease()
      val topTen = dbManipulator.computeTopTen(country_views, data_date_range)
      topTen.show(100)
      println(df.count())
      write_output("dataset_size", df.count().toString)
    }
  }

  def initSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Covid-Analysis")
      .config("spark.jars", "/home/mpiuser/cloud/covid-analysis.jar")
      .getOrCreate()

  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime() / scala.math.pow(10, 9)
    val result = block // call-by-name
    val t1 = System.nanoTime() / scala.math.pow(10, 9)
    write_output("execution_time", (t1 - t0).toString)
    println("°Total execution time (" + (t1 - t0) + ")")
    result
  }

  def write_output(field: String, value: String) = {
    val output_json = os.read(os.pwd / "profiling" / "output.json")
    val output = ujson.read(output_json)
    output("field") = value
    os.write(os.pwd / "profiling" / "output.json", output)
  }
}
