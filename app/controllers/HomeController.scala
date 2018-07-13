package controllers

import javax.inject._

import play.api.mvc._
import play.api.libs.iteratee.Enumerator
import org.apache.spark.sql.SparkSession
import play.http.HttpEntity

@Singleton
class HomeController @Inject()(cc: ControllerComponents, sparkSession: SparkSession) (implicit assetsFinder: AssetsFinder)
  extends AbstractController(cc) {

  def index = Action {
    val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("data/eth.csv")
    Ok(views.html.index(df))
  }

  def data = Action {
    val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("data/eth.csv")
    val enum   = Enumerator.fromFile("data/eth.csv")
    val source = akka.stream.scaladsl.Source.fromPublisher(play.api.libs.streams.Streams.enumeratorToPublisher(enum))
    Result(
      header = ResponseHeader(OK, Map(CONTENT_DISPOSITION -> "attachment; filename=data.csv")),
      body = HttpEntity.Streamed(source, None, None)
    )
  }

}
