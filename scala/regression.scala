import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
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
   output from 
   (1) bucketing - bucket is from input and all observations placed in
       mapped to 1 minute bucket 
   (2) condensing buckets (by taking weighted price)
   (3) computing fwd and back moving averages
   (4) merging the pairs using bucket	


   -------
   columns
   -------
   bucket , {pair_i_price, pair_i_dpx, pair_i_fwdDelta}


	do a simple regression just on two features to start 
	features are the change in price of the lagged moving avergae

	--------
	features
	---------
	[{pair_i_dpx}] 

	---------
	responses
	---------
	pair_i_fwdDelta


	---------------------
	regression equations
	---------------------

	pair_i_fwdDelta = B*[pair_i_dpx]
	
	----------------------
	running in spark
	----------------------


	A. create data set in from of (response,Vectors.dense(feautres))
	----------------------------------------------------------------
	create DataFrame
	Seq(
	  pair_i_fwdDelta(0), Vectors.dense({pair_i_dpx}(0)),
	  ...
 	  pair_i_fwdDelta(n), Vectors.dense({pair_i_dpx}(n))
	)
	

        val dataSet = dataFrame.toDF("label", "features")

        B. nornalize data by x_norm = (xi - mean(x))/std(d)
       
        C. run regression on x_norm

	D. unscale the coefficients


	E. calculated the predicted change in price
	predicted_change_price = B_esimated * X

	precicted_price = current_price + predicted changed in price	


**/ 

val dataDir : String = "hdfs:///user/jr4716/bucket/"
val pairs : Array[String] = Array("XBT", "ETH", "XRP", "LTC")
val keepFields : Array[String] = Array("bucket", "price", "dpx", "fwdDelta")

import scala.collection.mutable.ArrayBuffer

def featIndex(n : Int) : Array[Int] = {
  var b = ArrayBuffer.fill[Int](n)(0)
  for(i <- 0 until n) {
    b(i) = 2 + 3*(i)
  }
  b.toArray
}

val featureIndex = featIndex(pairs.size)
val respIndex = featureIndex.map(x => x + 1)

//---------------------------------------------------------------------------------
// STEP 1, get the merged dataset and create DataFrame in ml format for regression
//---------------------------------------------------------------------------------
def rdata(row : org.apache.spark.sql.Row, ri: Int, fi:Array[Int]) = {
    (row(ri).asInstanceOf[Double], Vectors.dense(fi.map(i => row(i).asInstanceOf[Double])))
}

val dataSet = merge(dataDir, pairs, keepFields)
val regressionSet = dataSet.map(s => rdata(s, respIndex(0), featureIndex)).toDF("label", "features")

//---------------------------------------------------------------------------------
// STEP 2, scale the data for ML Algos (unscaled data will give unpredecitable results)
//---------------------------------------------------------------------------------
val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(false)
val scalerModel = scaler.fit(regressionSet)
val scalerData = scalerModel.transform(regressionSet)

//---------------------------------------------------------------------------------
// STEP 3, Apply ML Algo to Scaled Data
//---------------------------------------------------------------------------------
val linearReg = new LinearRegression().setMaxIter(200).setFeaturesCol("scaledFeatures")
val fitModel = linearReg.fit(scalerData)
val prediction = fitModel.transform(scalerData).select("features", "scaledFeatures", "prediction")









