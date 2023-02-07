
import IngestionWithDataFrame.joinDf
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class DataIngestionTest extends AnyFunSuite with SparkSessionTest with DataFrameTestUtils {

  import spark.implicits._


  test("Join Test Functionality") {

    val sourceDf1 = Seq(
      ("Adore", "Shaddock", "19981217-73959"),
      ("Lorenza", "Kiersten", "19621220-14807"),
      ("Mureil", "Willie", "19781211-72222")
    ).toDF("name", "surname", "identification_number")
    val sourceDf2 = Seq(
      ("Adore", 23, "19981217-73959","Paris"),
      ("Lorenza", 24, "19621220-14807","Paris"),
      ("Mureil", 21, "19781211-72222","Paris")
    ).toDF("name", "age", "identification_number","ville")


    val resDf = joinDf(sourceDf1,sourceDf2,sourceDf1("identification_number"),sourceDf2("identification_number"),sourceDf1("name"),sourceDf2("name") )

    val expectedDf = Seq(
      ("Adore", "Shaddock", "19981217-73959", "Adore", 23, "19981217-73959","Paris"),
      ("Lorenza", "Kiersten", "19621220-14807", "Lorenza", 24, "19621220-14807","Paris"),
      ("Mureil", "Willie", "19781211-72222",  21, "19781211-72222","Paris")
    ).toDF("name", "surname", "identification_number", "age", "identification_number", "ville")

    assert(assertSchema(resDf.schema, expectedDf.schema))


  }
  test("Join Data Test Functionality") {

    val sourceDf1 = Seq(
      ("Adore", "Shaddock", "19981217-73959"),
      ("Lorenza", "Kiersten", "19621220-14807"),
      ("Mureil", "Willie", "19781211-72222")
    ).toDF("name", "surname", "identification_number")
    val sourceDf2 = Seq(
      ("Adore", 23, "19981217-73959", "Paris"),
      ("Lorenza", 24, "19621220-14807", "Paris"),
      ("Mureil", 21, "19781211-72222", "Paris")
    ).toDF("name", "age", "identification_number", "ville")


    val resDf = joinDf(sourceDf1, sourceDf2, sourceDf1("identification_number"), sourceDf2("identification_number"), sourceDf1("name"), sourceDf2("name"))

    val expectedDf = Seq(
      ("Adore", "Shaddock", "19981217-73959", "Adore", 23, "19981217-73959", "Paris"),
      ("Lorenza", "Kiersten", "19621220-14807", "Lorenza", 24, "19621220-14807", "Paris"),
      ("Mureil", "Willie", "19781211-72222", 21, "19781211-72222", "Paris")
    ).toDF("name", "surname", "identification_number", "age", "identification_number", "ville")

    assert(assertData(resDf, expectedDf))


  }


  // Check if output have a good schema
  test("Good column Test Functionality") {
    val testDf = spark.read.format("parquet").load("data/output/part-00000-48cc479c-965c-499a-b477-c74200d52f48-c000.snappy.parquet")
    val listes_df = testDf.columns.toList
    val listes = List("upc","isrc","label_name","album_name","song_id","song_name","artist_name","content_type","total_net_revenue","sales_country")
    assert(listes == listes_df)

  }
  // Check if output have a good schema
  test("Good schema Test Functionality") {
    val testDf = spark.read.format("parquet").load("data/output/part-00000-c0d30792-8ef0-4952-9c0d-1f475496a712-c000.snappy.parquet")
    val schemaDf = testDf.schema

    val expectedSchema = new StructType()
      .add("upc", StringType, nullable = true)
      .add("isrc", StringType, nullable = true)
      .add("label_name", StringType, nullable = true)
      .add("album_name", StringType, nullable = true)
      .add("song_id", LongType, nullable = true)
      .add("song_name", StringType, nullable = true)
      .add("artist_name", StringType, nullable = true)
      .add("content_type", StringType, nullable = true)
      .add("total_net_revenue", DoubleType, nullable = true)
      .add("sales_country", StringType, nullable = true)

    assert (schemaDf == expectedSchema)

  }
  test("Check if all columns have not null value Test Functionality") {
    val testDf = spark.read.format("parquet").load("data/output/part-00000-c0d30792-8ef0-4952-9c0d-1f475496a712-c000.snappy.parquet")

    def countCols(columns: Array[String]): Array[Column] = {
      columns.map(c => {
        count(when(col(c).isNull ||
          col(c) === "" ||
          col(c).contains("NULL") ||
          col(c).contains("null"), c)
        ).alias(c)
      })
    }

    val resultDf= testDf.select(countCols(testDf.columns): _*)
    //resultDf.show()
    val expectedDf = Seq((0, 0, 0, 0, 0, 0, 0, 0, 0, 0)).toDF("upc","isrc","label_name","album_name","song_id","song_name","artist_name","content_type","total_net_revenue","sales_country")
    //expectedDf.show()
    assert(resultDf == expectedDf)
  }
  }