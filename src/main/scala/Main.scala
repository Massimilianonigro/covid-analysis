import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val spark = initSession()
    spark.sparkContext.setLogLevel("ERROR")
    val session = spark.sqlContext.sparkSession

    var df = spark.read.csv(
      "/home/mpiuser/resources/data.csv"
    )

    df = PreprocessingHandler.dfPreprocessing(df)
    df.createOrReplaceTempView("df")
    val dbManipulator = new DbManipulator(df, session)

    val (country_views, data_date_range) =
      dbManipulator.computeMovingAverageAndPercentageIncrease()
    val topTen = dbManipulator.computeTopTen(country_views, data_date_range)
    dbManipulator.shutdown()
    topTen.show(100)
  }

  def initSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Covid-Analysis")
      .config("spark.master", "spark://192.168.43.129:7077")
      .config("spark.jars", "out/artifacts/covid-analysis.jar")
      .getOrCreate()

  }
}
