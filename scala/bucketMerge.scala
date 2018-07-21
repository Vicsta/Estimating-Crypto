import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import sqlContext.implicits._
import org.apache.spark.sql._

case class Trade(price: Float, qty: Float, time: Long, bucket: Long)

def createDf(dataDir: String, pair : String) = {

    val r = sc.textFile(dataDir + pair + ".csv")
    .filter(_.split(",")(0) != "price")
    .map(_.split(","))
    .map(s => (s(0).toFloat, s(1).toFloat, (s(2).toFloat*1000.0f).toLong, s(3).toString, s(4).toString, s(5).toLong))
    .keyBy(_._6)
    .groupByKey()
    def condense (s : (Long, Iterable[(Float, Float, Long, String, String, Long)])) = {
      val bucket = s._1
      val data = s._2.toArray
      var wsum = 0.0f
      var qsum = 0.0f
      for(i <- 0 until data.size) {
            var px = data(i)._1
	    var qx = data(i)._2
	    if (qx == 0.0f) {
	       qx = 0.001f
	    }
	    wsum += px*qx
	    qsum += qx
      }
      val weighted_px = wsum / qsum
      val last = data.size - 1
      (bucket, (weighted_px, qsum, data(last)._3, bucket))
     }
     var df = r.map(s => condense(s)).sortByKey().map(s => Trade(s._2._1, s._2._2, s._2._3, s._2._4)).toDF()
     val fields = df.schema.fields.toList
     fields.foreach((field) => {
        if(!field.name.equals("bucket")) {
	  df = df.withColumnRenamed(field.name, pair + "_" + field.name)
	}
     })
     df	
}

def merge(dataDir: String, pairs : Array[String]) : org.apache.spark.sql.DataFrame = {
    val frames = pairs.map(p => (p,createDf(dataDir, p)))
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

val pairs : Array[String] = Array("XRP", "ETH", "XBT", "LTC")
val dataDir : String = "hdfs:///user/jr4716/bucket/"

val merged = merge(dataDir, pairs)





