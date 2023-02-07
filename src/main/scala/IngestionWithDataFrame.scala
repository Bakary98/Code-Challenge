
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._

object IngestionWithDataFrame extends App  {


    /** Our main function where the action happens */

      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)

      // Use new SparkSession interface in Spark 2.0
      val spark = SparkSession
        .builder
        .appName("Ingestion")
        .master("local[*]")
        .getOrCreate()

      // Function uses to read the data
      val albumsSchema = new StructType()
        .add("upc", StringType, nullable = true)
        .add("album_name", StringType, nullable = true)
        .add("label_name", StringType, nullable = true)
        .add("country", StringType, nullable = true)

      val salesSchema = new StructType()
        .add("PRODUCT_UPC", StringType, nullable = true)
        .add("TRACK_ISRC_CODE", StringType, nullable = true)
        .add("TRACK_ID", StringType, nullable = true)
        .add("DELIVERY", StringType, nullable = true)
        .add("NET_TOTAL", DoubleType, nullable = true)
        .add("TERRITORY", StringType, nullable = true)

      val songsSchema = new StructType()
        .add("isrc", StringType, nullable = true)
        .add("song_id", LongType, nullable = true)
        .add("song_name", StringType, nullable = true)
        .add("artist_name", StringType, nullable = true)
        .add("content_type", StringType, nullable = true)
      def dataFrameReadCsv (path: String, delimiter: String,header:String, schema: StructType): DataFrame = {

        spark.read.options(Map("header"->header, "delimiter"->delimiter)).schema(schema).csv(path)
      }
      // Read albums, sales and songs csv files
      val albums = dataFrameReadCsv("data/input/albums.csv", ";","true",albumsSchema)
      val sales  = dataFrameReadCsv("data/input/sales.csv", ";","true",salesSchema)
      val songs  = dataFrameReadCsv("data/input/songs.csv", ";","true",songsSchema)

      // Joins all dataframes of albums, songs and sales to obtain data

      def joinDf(df1:DataFrame,df2: DataFrame,col1: Column, col2: Column,col3: Column, col4:Column): DataFrame= {
        df1.join(df2,col1 === col2 && col3 === col4)
      }

      val data1= joinDf(sales,songs,sales("TRACK_ISRC_CODE"),songs("isrc"),sales("TRACK_ID"),songs("song_id") )
      val dataFinal = joinDf(data1,albums,sales("PRODUCT_UPC"),albums("upc"),sales("TERRITORY"),albums("country") )



      // Cast net_total , song_id and upc columns and Renamed net_total and country columns

      val finalDf= dataFinal.select("upc","isrc","label_name","album_name","song_id","song_name","artist_name","content_type","net_total","country")

                             .withColumnRenamed("net_total", "total_net_revenue")
                             .withColumnRenamed("country","sales_country")
      finalDf.printSchema()
      // Check if we obtain the line we have in pdf challenge
      finalDf.filter(finalDf("upc") === "5016958061173").show(false)

      //Send data in format delta in path data/output

      finalDf.write.format("delta")
        .option("mode", "OVERWRITE")
        .option("path", "data/output")
        .save()



}
