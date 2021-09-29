import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime() / scala.math.pow(10, 9)
    // initialize Spark session
    val spark = initSession(args(1))
    spark.sparkContext.setLogLevel("ERROR")
    val countries: String = args(0)
    val session = spark.sqlContext.sparkSession

    // read dataset from input
    var df = spark.read.csv(
      "/home/mpiuser/cloud/data.csv"
    )
    df.cache // cache to avoid FileNotFoundException
    df.show(2, false)
    // remove unused columns, rename used ones
    df = PreprocessingHandler.dfPreprocessing(df, countries)
    df.createOrReplaceTempView("df")
    val dbManipulator = new DbManipulator(df, session)

    // create view to manipulate and query
    val (country_views, data_date_range) =
      dbManipulator.computeMovingAverageAndPercentageIncrease()
    val topTen = dbManipulator.computeTopTen(country_views, data_date_range)
    topTen.show(100)

    val t1 = System.nanoTime() / scala.math.pow(10, 9)
    println("°Total execution time (" + (t1 - t0) + ")")
    val output_json = os.read(os.pwd / "output" / "output.json")
    val output = ujson.read(output_json)
    output("execution_time") = (t1 - t0).toString
    output("dataset_size") = df.count().toString
    os.write(os.pwd / "output" / "output.json", output)
    println("Exec time: " + (t1 - t0).toString)
    println("Dataset size: " + df.count().toString)
  }

  def initSession(cores: String): SparkSession = {
    SparkSession
      .builder()
      .appName("Covid-Analysis")
      .config("spark.master", "local")
      .config("spark.jars", "/home/mpiuser/cloud/covid-analysis.jar")
      .getOrCreate()

  }

}
