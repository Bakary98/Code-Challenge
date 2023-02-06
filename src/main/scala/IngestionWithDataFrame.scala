
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.catalyst.csv.CSVInferSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object IngestionWithDataFrame  {


    /** Our main function where the action happens */
    def main(args: Array[String]) {
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)

      // Use new SparkSession interface in Spark 2.0
      val spark = SparkSession
        .builder
        .appName("Ingestion")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()

      // Function uses to read the data
      def dataFrameRead (path: String, delimiter: String,header:String,inferSchema: String): DataFrame = {

        spark.read.options(Map("header"->header, "delimiter"->delimiter, "inferSchema"->inferSchema)).csv(path)
      }
      // Read albums, sales and songs csv files
      val albums = dataFrameRead("data/albums.csv", ";","true","true")
      val sales  = dataFrameRead("data/sales.csv", ";","true","true")
      val songs  = dataFrameRead("data/songs.csv", ";","true","true")

      // Joins all dataframes of albums, songs and sales to obtain data

      def joinDf(df1:DataFrame,df2: DataFrame,col1: Column, col2: Column,col3: Column, col4:Column): DataFrame= {
        df1.join(df2,col1 === col2 && col3 === col4)
      }
      val data1= joinDf(sales,songs,sales("TRACK_ISRC_CODE"),songs("isrc"),sales("TRACK_ID"),songs("song_id") )
      val dataFinal = joinDf(data1,albums,sales("PRODUCT_UPC"),albums("upc"),sales("TERRITORY"),albums("country") )

      // Joins all dataframes of albums, songs and sales to obtain data

     /* val data = sales.join(songs, sales("TRACK_ISRC_CODE") === songs("isrc") &&
                                sales("TRACK_ID") === songs("song_id"), "inner")
                      .join(albums,sales("PRODUCT_UPC")=== albums("upc") &&
                          sales("TERRITORY") === albums("country"))

      */

      // Cast net_total , song_id and upc columns and Renamed net_total and country columns

      val finalDf= dataFinal.select("upc","isrc","label_name","album_name","song_id","song_name","artist_name","content_type","net_total","country")

                             .withColumn("net_total",col("net_total").cast(DoubleType))
                             .withColumn("song_id",col("song_id").cast(LongType))
                             .withColumn("upc",col("upc").cast(StringType))
                             .withColumnRenamed("net_total", "total_net_revenue")
                             .withColumnRenamed("country","sales_country")

      // Check if we obtain the line we have in pdf challenge
      finalDf.filter(finalDf("upc") === "5016958061173").show(false)

      //Send data in format delta in path data/output

      finalDf.write.format("delta")
        .option("mode", "OVERWRITE")
        .option("path", "data/output")
        .save()



    }


}
