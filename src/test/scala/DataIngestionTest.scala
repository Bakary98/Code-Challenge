
import IngestionWithDataFrame.joinDf
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
    assert(assertData(resDf, expectedDf))


  }

  /*test("Check non functionality ") {
    val sourceDf1 = Seq(
      ("Adore", "Shaddock", "19981217-73959"),
      ("Lorenza", "Kiersten", "19621220-14807"),
      ("Mureil", "Willie", "19781211-72222")
    ).toDF("name", "surname", "identification_number")
    val sourceDf2 = Seq(
      ("Adore", 23, "19981217-73959"),
      ("Lorenza", 24, "19621220-14807"),
      ("Mureil", 21, "19781211-72222")
    ).toDF("name", "age", "identification_number")

    val resDf = joinDf(sourceDf1,sourceDf2,sourceDf1("identification_number"),sourceDf2("identification_number"),sourceDf1("name"),sourceDf2("name") )

    val expectedDf = Seq(
      ("Adore", "Shaddock", "19981217-73959", "Adore", 23, "19981217-73959", "Paris"),
      ("Lorenza", "Kiersten", "19621220-14807", "Lorenza", 24, "19621220-14807", "Paris"),
      ("Mureil", "Willie", "19781211-72222", 21, "19781211-72222", "Paris")
    ).toDF("name", "surname", "identification_number", "age", "identification_number", "ville")

    assert(assertSchema(resDf.schema, expectedDf.schema))
  }*/


}