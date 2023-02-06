
import IngestionWithDataFrame.joinDf
import org.scalatest.funsuite.AnyFunSuite

class DataIngestionTest extends AnyFunSuite with SparkSessionTest with DataFrameTestUtils {

  import spark.implicits._

  test("DataFrame Schema Test") {

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

 /* test("DataFrame Data Test") {
    val sourceDf = Seq(
      ("Jackie", "Ax", "19861126-29967"),
      ("Vanessa", "Campball", "19881021-86591"),
      ("Willetta", "Reneta", "19991125-38555")
    ).toDF("name", "surname", "identification_number")

    val resDf = extractBirthDate(sourceDf)

    val expectedDf = Seq(
      ("Jackie", "Ax", "19861126-29967", "19861126", "1986", "11", "26"),
      ("Vanessa", "Campball", "19881021-86591", "19881021", "1988", "10", "21"),
      ("Willetta", "Reneta", "19991125-38555", "19991125", "1999", "11", "25")
    ).toDF("name", "surname", "identification_number", "birth_date", "year", "month", "day")

    assert(assertData(resDf, expectedDf))
  }*/
}