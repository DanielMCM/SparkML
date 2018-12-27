package upm.bd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

object SQL {
  case class Person(name: String, age: Long)
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("My second Spark application").set("spark.local.dir", "/tmp/spark-temp")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .config("some option", "value")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Encoders are created for case classes
    val personSeq = Seq(Person("Matt",49),
      Person("Ray", 38),
      Person("Gill", 62),
      Person("George", 28))
    var personDF = sc.parallelize(personSeq).toDF()

    personDF.filter(col("age") < 40).select(personDF("name")).show
    personDF.createOrReplaceTempView("person")
    spark.sql("select name from person where age < 40").show
  }
}