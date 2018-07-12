package controllers

import javax.inject._

import play.api.mvc._
import org.apache.spark.sql.SparkSession

@Singleton
class HomeController @Inject()(cc: ControllerComponents, spark: SparkSession) (implicit assetsFinder: AssetsFinder)
  extends AbstractController(cc) {

  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

}
