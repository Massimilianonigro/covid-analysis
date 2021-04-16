import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val spark = initSession()
    spark.sparkContext.setLogLevel("ERROR")
    val session = spark.sqlContext.sparkSession

    var df = spark.read.csv(
      "/home/mpiuser/cloud/data.csv"
    )

    df = PreprocessingHandler.dfPreprocessing(df)
    df.createOrReplaceTempView("df")
    val dbManipulator = new DbManipulator(df, session)

    val (country_views, data_date_range) =
      dbManipulator.computeMovingAverageAndPercentageIncrease()
    val topTen = dbManipulator.computeTopTen(country_views, data_date_range)
    topTen.show(100)
  }

  def initSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Covid-Analysis")
      .config("spark.master", "local")
      .config("spark.jars", "/home/mpiuser/cloud/covid-analysis.jar")
      .getOrCreate()

  }
}
