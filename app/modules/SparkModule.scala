package modules
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import org.apache.spark.sql.SparkSession
import play.api._

class SparkModule extends AbstractModule {

  def configure() = {
    //TODO connect to dumbo spark
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Valhalla")
      .getOrCreate()
    bind(classOf[SparkSession])
      .toInstance(sparkSession);
  }
}
