package controllers

import javax.inject._

import java.io.File
import play.api.mvc._
import play.api.libs.iteratee.Enumerator
import org.apache.spark.sql.SparkSession
import play.api.http.HttpEntity
import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream.scaladsl.{ FileIO, Source }
import play.api.libs.iteratee.streams.IterateeStreams
import play.api.libs.ws._
import play.api.libs.iteratee._
import akka.util.ByteString

@Singleton
class HomeController @Inject()(cc: ControllerComponents, sparkSession: SparkSession) (implicit assetsFinder: AssetsFinder)
  extends AbstractController(cc) {

  def index = Action {
    val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("data/eth.csv")
    Ok(views.html.index(df))
  }

  def data = Action {
    val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("data/eth.csv")
    val file = new java.io.File("data/eth.csv")
    val path = file.toPath();
    val source: Source[ByteString, _] = FileIO.fromPath(path)
    Result(
        header = ResponseHeader(200, Map.empty),
        body =  HttpEntity.Streamed(source, None, Some("text/csv"))
    )
  }

}
