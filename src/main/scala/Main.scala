import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit.DAYS
import java.util.Date
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}

object Main {

  def main(args: Array[String]): Unit = {

    val spark = initSession()
    spark.sparkContext.setLogLevel("ERROR")
    val session = spark.sqlContext.sparkSession

    var df = spark.read.csv(
      "./resources/data.csv"
    )

    df = PreprocessingHandler.dfPreprocessing(df)
    df.createOrReplaceTempView("df")
    val dbManipulator = new DbManipulator(df, session)

    dbManipulator.setMovingAverageAndPercentageIncrease()
    dbManipulator.computeTopTen()
    dbManipulator.shutdown()
    dbManipulator.minMax.show(100)
  }

  def initSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Covid-Analysis")
      .config("spark.master", "local")
      .getOrCreate()

  }
}
