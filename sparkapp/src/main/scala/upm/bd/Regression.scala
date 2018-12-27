package upm.bd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.regression.LinearRegression

object Regression {
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

    val df = Seq((-3.0965012, 5.2371198,-0.7370271),
      (-0.2100299,-0.7810844,-1.3284768),
      (8.3525083, 5.3337562, 21.8897181),
      (-3.0380369, 6.5357180, 0.3469820),
      (5.9354651, 6.0223208, 17.9566144),
      (-6.8357707, 5.6629804,-8.1598308),
      (8.8919844, -2.5149762, 15.3622538),
      (6.3404984, 4.1778706, 16.7931822))
      .toDF("x1","x2","y")
    val assembler = new VectorAssembler()
      .setInputCols(Array("x1", "x2"))
      .setOutputCol("features")
    val output = assembler.transform(df)

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)
    val l1NormData = normalizer.transform(output)

    val lr = new LinearRegression()
      .setFeaturesCol("normFeatures")
      .setLabelCol("y")
      .setMaxIter(10)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(l1NormData)
    println(s"Coefficients: ${lrModel.coefficients}")
    println(s"Intercept: ${lrModel.intercept}")
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")


  }
}
