import org.apache.spark.sql.{Dataset, Row}

// Performs an initial cleanup of the dataset
object PreprocessingHandler {
  def dfPreprocessing(df: Dataset[Row]): Dataset[Row] = {

    // removes unused columns and renames used columns
    val out = df
      .drop("_c1") //day
      .drop("_c2") //month
      .drop("_c3") //year
      .drop("_c5") //deaths
      .drop("_c7") //geoId
      .drop("_c8") //countryTerritoryCode
      .drop("_c9") //popData2019
      .drop("_c10") //continentExp
      .drop("_c11") //Cumulative_number
      .withColumnRenamed("_c0", "dateRep")
      .withColumnRenamed("_c6", "country")
      .withColumnRenamed("_c4", "cases")

    // deleting the first row, which contains the name of the columns
    val firstRow = out.first()
    out
      .filter(row => row != firstRow)
  }
}
