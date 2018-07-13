package controllers

import javax.inject._

import java.io.File
import java.io.PipedOutputStream
import java.io.PipedInputStream
import play.api.mvc._
import play.api.libs.iteratee.Enumerator
import org.apache.spark.sql.SparkSession
import play.api.http.HttpEntity
import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream.scaladsl.{ FileIO, Source, StreamConverters }
import play.api.libs.iteratee.streams.IterateeStreams
import play.api.libs.ws._
import play.api.libs.iteratee._
import akka.util.ByteString
import java.io.{InputStream, ByteArrayInputStream, ByteArrayOutputStream, PrintWriter, OutputStreamWriter}

@Singleton
class ReportController @Inject()(cc: ControllerComponents, sparkSession: SparkSession) (implicit assetsFinder: AssetsFinder)
  extends AbstractController(cc) {

  def index = Action {
    val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("data/eth.csv")
    Ok(views.html.index(df))
  }

  def data(pair: String) = Action {
    val source: Source[ByteString, _] = StreamConverters.fromInputStream(()=> getData(pair))
    Result(
        header = ResponseHeader(200, Map.empty),
        body =  HttpEntity.Streamed(source, None, Some("text/csv"))
    )
  }

  def getData(pair: String) : InputStream = {
    val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("data/" + pair + ".csv")
    val out = new PipedOutputStream()
    val in = new PipedInputStream()
    val writer = new PrintWriter(new OutputStreamWriter(out))
    val header = df.columns.mkString(",")
    writer.println(header)
		in.connect(out);
		val pipeWriter = new Thread(new Runnable() {
      def run(): Unit = {
        df.collect.foreach((row)=> {
            val line = row.mkString(",")
            writer.println(line)
          }
        )
        writer.close()
      }
    })
    pipeWriter.start()
    return in
  }
}
