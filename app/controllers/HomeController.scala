package controllers

import javax.inject._

import java.io.File
import play.api.mvc._
import play.api.libs.iteratee.Enumerator
import org.apache.spark.sql.SparkSession
import play.http.HttpEntity
import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream.scaladsl.Source
import play.api.libs.streams.Streams

@Singleton
class HomeController @Inject()(cc: ControllerComponents, sparkSession: SparkSession) (implicit assetsFinder: AssetsFinder)
  extends AbstractController(cc) {

  def index = Action {
    val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("data/eth.csv")
    Ok(views.html.index(df))
  }

  def data = Action {
    val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("data/eth.csv")
    val enumerator = Enumerator.fromFile(new File("data/eth.csv"))
    val source = Source.fromPublisher(Streams.enumeratorToPublisher(enumerator))
    Ok.sendEntity(HttpEntity.Streamed(source, None, Some("text/csv")))
  }

}
