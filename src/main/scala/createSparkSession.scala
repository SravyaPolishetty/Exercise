import org.apache.spark.sql.SparkSession

object createSparkSession {
 //Creates and returns spark session
  def getSparkSession(appName:String): SparkSession = {
    val session = SparkSession.builder()
                              .appName(appName)
                              .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                              .config("spark.kryoserializer.buffer.max","1g")                  
                              .master("local").getOrCreate()
    session
  }
}
