import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    time {
      // initialize Spark session
      val spark = initSession()
      spark.sparkContext.setLogLevel("ERROR")
      val session = spark.sqlContext.sparkSession

      // read dataset from input
      var df = spark.read.csv(
        "/home/mpiuser/cloud/data.csv"
      )

      // remove unused columns, rename used ones
      df = PreprocessingHandler.dfPreprocessing(df)
      df.createOrReplaceTempView("df")
      val dbManipulator = new DbManipulator(df, session)

      // create view to manipulate and query
      val (country_views, data_date_range) =
        dbManipulator.computeMovingAverageAndPercentageIncrease()
      val topTen = dbManipulator.computeTopTen(country_views, data_date_range)
      topTen.show(100)
    }
  }

  def initSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Covid-Analysis")
      .config("spark.master", "local")
      .config("spark.jars", "/home/mpiuser/cloud/covid-analysis.jar")
      .config("spark.executor.memory", "4g")
      .config("spark.executor.core", "4")
      .getOrCreate()

  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("°Total execution time (" + (t1 - t0) + ")")
    result
  }

}
