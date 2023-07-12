/*****************************
Description : Your task is to create a new matching engine for FX orders. The engine will take a CSV file of orders for a given
currency pair and match orders together.

Used - Spark Framework along with Scala

******************************/

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path,FileUtil}
import org.apache.spark.sql.SaveMode

Object ProcessTradeData {
   val spark=GetSparkSession.getSparkSession("ProcessTradeData")
   
   import spark.implicits._
   
   fs.setVerifyChecksum(false)
   
   val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

//Below method reads CSV file with schema and load into a dataframe
def readCSVFile(filePath:String,Schema:StructType):DataFrame={
   val hadoopfs=new Path("$filePath")
   if(hadoopfs.exists(hadoopfs))
   {
     spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header","false").schema(Schema).load(filePath)
	}
    else{
	   spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schema)
	}

}

/*matchOrdersBetweenOrderBookAndOrderFile - defines to capture matchorders  from orderbook and unmatch orders from orderBook,orderFile */

def matchOrdersBetweenOrderBookAndOrderFile(orderBookDF:DataFrame,orderFileDF:DataFrame):(DataFrame,DataFrame,DataFrame)={

    val orderBookBuyOrdersDF=orderBookDF.where("""order_type="BUY" """).withColumn("row_num",row_number() over (Window.partitionBy("quantity").orderBy("order_time")))
	
	val orderBookSellOrdersDF=orderBookDF.where("""order_type="SELL" """).withColumn("row_num",row_number() over (Window.partitionBy("quantity").orderBy("order_time")))
	
	val orderFileBuyOrderDF=orderFileDF.where("""order_type="BUY" """).withColumn("row_num",row_number() over (Window.partitionBy("quantity").orderBy("order_time")))
	
	val orderFileSellOrderDF=orderFileDF.where("""order_type="SELL" """).withColumn("row_num",row_number() over (Window.partitionBy("quantity").orderBy("order_time")))

    val orderBookBuyMatchDF=orderBookBuyOrdersDF.as("bookbuy").join(orderFileSellOrderDF.as("filesell"),bookbuy("quantity")===filesell("quantity") AND bookbuy("row_num")===filesell("row_num"),"inner")
	
	.withColumn("match_order_id",when bookbuy("order_time") > filesell("order_time"),bookbuy("order_id").otherwise(filesell("order_id")))
	
	.withColumn("first_order_id",when bookbuy("order_time") > filesell("order_time"),filesell("order_id").otherwise(bookbuy("order_id")))
	
	.withColumn("order_time_matched",greatest(bookbuy("order_time"),filesell("order_time")))
	
	.withColumn("matchedQuantity",bookbuy("quantity"))
	
	.withColumn("orderPrice",bookbuy("price"))
	
	.select("match_order_id","first_order_id","order_time_matched","matchedQuantity","orderPrice")

    val orderBookSellmatchDF=orderBookSellOrdersDF.as("booksell").join(orderfilebuyOrderDF.as("filebuy"),booksell("quantity")===filebuy("quantity") AND booksell("row_num")===filebuy("row_num"),"inner")
	
	.withColumn("match_order_id",when booksell("order_time") > filebuy("order_time"),booksell("order_id").otherwise(filebuy("order_id")))
	
	.withColumn("first_order_id",when booksell("order_time") > filebuy("order_time"),filebuy("order_id").otherwise(booksell("order_id")))
	
	.withColumn("order_time_matched",greatest(booksell("order_time"),filebuy("order_time")))
	
	.withColumn("matchedQuantity",booksell("quantity"))
	
	.withColumn("orderPrice",booksell("price"))
	
	.select("match_order_id","first_order_id","order_time_matched","matchedQuantity","orderPrice")



  val ordersMatchedDF=orderBookBuyMatchDF.union(orderBookSellmatchDF)
  
  val closedOrdersDF=ordersMatchedDF.withColumn("closedOrderId",explode(array("match_order_id","first_order_id"))).select("closedOrderId")
  
  val unclosedOrdersBookDF=orderBookDF.join(closedOrdersDF,orderBookDF("order_id")===closedOrdersDF("order_id"),"left_anti")
  
  val unclosedOrdersFileDF=orderFileDF.join(closedOrdersDF,orderFileDF("order_id")===closedOrdersDF("order_id"),"left_anti")
  
  (ordersMatchedDF,unclosedOrdersBookDF,unclosedOrdersFileDF)
  
  }

/*matchOrderWithInTheOrderFile : defines match orders from orderFile and returns the matched,unmatched orders from orderFile */
  def matchOrderWithInTheOrderFile(orderFile:DataFrame):(DataFrame,DataFrame)={
  
  val buyOrdersDF=orderFileDF.where("order_type='BUY'").withColumn("row_num",row_number() over(Window.partitionBy("quantity"),.orderBy("order_time")))
  
  val sellOrdersDF=orderFileDF.where("order_type='SELL'").withColumn("row_num",rwo_number() over(Window.partitionBy("quantity").orderBy("order_time")))

  val closedOdersDF=buyOrdersDF.jon("sellOrdersDF",buyOrdersDF("quantity")===sellOrdersDF("quantity") AND buyOrdersDF("row_number") === sellOrdersDF("row_number"))
                       .withColumn("matchOrderId",when(buyOrdersDF("order_time") > sellOrdersDF("order_time"),buyOrdersDF("order_id").otherwise(sellOrdersDF("order_id"))))
					   
                       .withColumn("firstOrderId",when(buyOrdersDF("order_time") > sellOrdersDF("order_time"),sellOrdersDF("order_id").otherwise(buyOrdersDF("order_id"))))
					   
					   .withColumn("matchedOrderTime",greatest(buyOrdersDF("order_time"),sellOrdersDF("order_time"))
					   
					   .withColumn("quantityMatched",sellOrdersDF("quantity"))
					   
					   .withColumn("order_price",when(buyOrdersDF("order_time")>sellOrdersDF("order_time"),sellOrdersDF("price")).otherwise(buyOrdersDF("price")))
					   
					   .select("matchOrderId","firstOrderId","matchedOrderTime","quantityMatched","order_price")
	

    val closedOrdersDF=closedOdersDF.withColumn("closedOrderId",explode(array("match_order_id","first_order_id"))).select("closedOrderId")	
	
    val unclosedOrdersBookDF=orderFileDF.join(closedOrdersDF,orderBookDF("order_id")===closedOrdersDF("order_id"),"left_anti")
  
   (closedOrdersDF,unclosedOrdersBookDF)
  }

def main(args:Array[String])={
    try{
	val ordersFilePath=args(0)
	val ordersFileName=args(1)
	val ordersBookPath=args(2)
	val orderBookFileName=args(3)
	
	val schema=StructType(Array(
	     StructField("order_id","StringType"),
		 StructField("user_name","StringType"),
		 StructField("order_time","LongType"),
		 StructField("order_type"StringType"),
		 StructField("quantity","IntergerType"),
		 StructField("price","LongType")))
		 
	val ordersDF=readCsvFile("$ordersFilePath/$ordersFileName",ordersSchema).persist()
	
	val orderBookDF=readCsvFile("$orderBookFilePath/$orderBookFileName",ordersSchema).persist()
	
	val (closedOrders,unclosedOrders)=matchOrders(orderBookDF,ordersDF)
	
	closedOrders.coalesce(1).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Append).save(s"$closedOrdersFilePath/output_${closeOrdersFileName}")
	
	unclosedOrders.coalesce(1).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Append).save(s"$orderBookFilePath/output_${orderBookFileName}")
	
	println("end of the process")
		 
	}

   catch {
      case e:Throwable=>e.printStackTrace()
    }
    finally{
      spark.stop()
    }
  }
}

