import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._

case class Trade(price: Double, qty: Double, time: Long, bucket: Long)


/**
  create a org.apache.spark.sql.DataFrame
  from a file that contains a bucket index for each observation

**/
def createDf(dataDir: String, pair : String, keepFields : Array[String]) = {

    val r = sc.textFile(dataDir + pair + ".csv")
    .filter(_.split(",")(0) != "price")
    .map(_.split(","))
    .map(s => (s(0).toDouble, s(1).toDouble, (s(2).toDouble*1000.0f).toLong, s(3).toString, s(4).toString, s(5).toLong))
    .keyBy(_._6)
    .groupByKey()

    // condense row based on weighed price
    def condense (s : (Long, Iterable[(Double, Double, Long, String, String, Long)])) = {
      val bucket = s._1
      val data = s._2.toArray
      var wsum = 0.0
      var qsum = 0.0
      for(i <- 0 until data.size) {
            var px = data(i)._1
	    var qx = data(i)._2
	    if (qx == 0.0) {
	       qx = 0.001
	    }
	    wsum += px*qx
	    qsum += qx
      }
      val weighted_px = wsum / qsum
      val last = data.size - 1
      (bucket, (weighted_px, qsum, data(last)._3, bucket))
     }
     var df = r.map(s => condense(s)).sortByKey().map(s => Trade(s._2._1, s._2._2, s._2._3, s._2._4)).toDF()

     // create backward looking moving average price     
     val lagBack = org.apache.spark.sql.expressions.Window.orderBy("bucket").rowsBetween(-10,0)
     df = df.withColumn("ma_back", avg(df("price")).over(lagBack))

     // create forward looking moving average price
     val lagFwd = org.apache.spark.sql.expressions.Window.orderBy("bucket").rowsBetween(0,10)
     df = df.withColumn("ma_fwd", avg(df("price")).over(lagFwd))

     // create column which is difference between current price and backward moving average
     df = df.withColumn("dpx", df("price") - df("ma_back"))

     // create column wihic is differente between fwd moving average and current price
     df = df.withColumn("fwdDelta", df("ma_fwd") - df("price"))

     val fields = df.schema.fields.toList
     fields.foreach((field) => {
        if(!keepFields.contains(field.name)) {
	  df = df.drop(df.col(field.name))
        }
        else if(!field.name.equals("bucket")) {
	   df = df.withColumnRenamed(field.name, pair + "_" + field.name)
        }
     })
     df	
}

def merge(dataDir: String, pairs : Array[String], keepFields : Array[String]) : org.apache.spark.sql.DataFrame = {
    val frames = pairs.map(p => (p,createDf(dataDir, p, keepFields)))
    val head = frames.head._1
    var merged = frames.head._2
    frames.foreach(pair => {
       val key = pair._1
       val data = pair._2
       if (!key.equals(head)) {
          merged = merged.join(data, "bucket")
       }
    })
    merged   
}


/**
   global variables for the run	
   using the small bucketed set generated via python 
**/ 


val dataDir : String = "hdfs:///user/jr4716/bucket/"
val pairs4 : Array[String] = Array("XRP", "ETH", "XBT", "LTC")
val pairs2 : Array[String] = Array("ETH", "XBT")

/**
  we dont' need all of the fields from the input so just keep the below 
  fields to conserve space
**/
 
val keepFields : Array[String] = Array("bucket", "price", "dpx", "fwdDelta")

/**
   output from 
   (1) bucketing - bucket is from input and all observations placed in
       mapped to 1 minute bucket 
   (2) condensing buckets (by taking weighted price)
   (3) computing fwd and back moving averages
   (4) merging the pairs using bucket	


   the colms from  merged2
   -------
   columns
   -------
   bucket, ETH_price, ETH_dpx, ETH_fwdDelta, XBT_price, XBT_dpx, XBT_fwdDelta

**/


val merged2 = merge(dataDir, pairs2, keepFields)

/**
	do a simple regression just on two features to start 
	features are the change in price of the lagged moving avergae

	--------
	features
	---------
	ETH_dpx, XBT_dpx

	---------
	responses
	---------
	ETH_fwdDelta, XBT_fwdDelta


	---------------------
	regression equations
	we are going to estimate the coefficients
	(e1, e2) using features (ETH_dpx, XBT_dpx) and response ETH_fwdDelta
	(x1, x2) using features (ETH_dpx, XBT_dpx) and response XBT_fwdDelta
	---------------------

	ETH_fwdDelta = e1 * ETH_dpx  + e2 * XBT_dpx
	XBT_fwdDelta = x1 * ETH_dpx  + x2 * XBT_dpx


   this is what sample data looks like for a linear regression
   val training = spark.createDataFrame(Seq(
     (1.0, Vectors.dense(0.0, 1.1, 0.1)),
     (0.0, Vectors.dense(2.0, 1.0, -1.0)),
     (0.0, Vectors.dense(2.0, 1.3, 1.0)),
     (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")

**/


/**
  set up data to run in linear regression
  each row of data to the regression is a tuple of (double(response), Vector(features))
**/



def rdata(row : org.apache.spark.sql.Row, ri: Int, fi:Array[Int]) = {
    (row(ri).asInstanceOf[Double], Vectors.dense(fi.map(i => row(i).asInstanceOf[Double])))
}

val reg1 = merged2.map(s => rdata(s, 3, Array(2, 5)))
val df = reg1.toDF("label", "features")


/**
  run a linear regression
**/



val lr = new LinearRegression()
lr.setMaxIter(100)
val model = lr.fit(df)



