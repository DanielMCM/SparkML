package es.upm


import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegression,LinearRegressionModel}
//{LinearRegression, LinearRegressionModel, RandomForestRegressionModel, RandomForestRegressor,GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StringIndexer

import scala.math.{abs, log}

object Importance{

  def main(args: Array[String]) : Unit = {
    // 1.- Arguments + Logs set up + Spark session

    val file_path = args(0)
    val model_path = args(1)
    val mode = args(2)

    Logger.getRootLogger().setLevel(Level.FATAL)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[*]")
      .appName("Spark CSV Reader")
      .getOrCreate;

    // 2.- Load and parse the data file, converting it to a DataFrame.

    var data = spark.read
      .format("csv")
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .load(file_path)
      .filter("Cancelled == 0")
      .filter("Diverted == 0")

    // 3.- We only select some of the fields

    data = data
      .select(
        data("Month").cast("integer"),
        // data("DayofMonth").cast("integer"),
        // data("DayOfWeek").cast("integer"),
        data("DepTime").cast("integer"),
        data("CRSArrTime").cast("integer"),
        // data("UniqueCarrier"),
        // data("CRSElapsedTime").cast("integer"),
        data("ArrDelay").cast("integer"),
        data("DepDelay").cast("integer"),
        data("Origin"),
        data("Dest"),
        // data("Distance").cast("integer"),
        data("TaxiOut").cast("integer")
      )

    // 4.- Helping functions
    val log_trans = (value:Int) => {
        log(value + 550 + 1)
    }
    val log_trans_udf = udf(log_trans)

    // 5.- Transformation
    data = data.withColumn("DepDelay_Tmp", log_trans_udf(data("DepDelay"))).drop("DepDelay").withColumnRenamed("DepDelay_Tmp", "DepDelay")

    // 6.- Automatically identify categorical features, and index them.

    val indexer = new StringIndexer()
      .setInputCol("Origin")
      .setOutputCol("Origin2").fit(data)

    val indexed = indexer.transform(data)

    val indexer2 = new StringIndexer()
      .setInputCol("Dest")
      .setOutputCol("Dest2").fit(indexed)

    val indexed2 = indexer2.transform(indexed)

    // 7.- Prepare model

    val assembler = new VectorAssembler()
      .setInputCols(Array(
      // "Month",
      // "DayofMonth",
      // "DayOfWeek",
      "DepTime",
      "CRSArrTime",
      // "UniqueCarrier",
      // "CRSElapsedTime",
      // "ArrDelay",
      "DepDelay",
      // "Origin","Dest",
      // "Distance",
      "TaxiOut", 
      "Origin2", 
      "Dest2"))
      .setOutputCol("features")

    // 7.1.- Split the data into training and test sets (10 months - 2 months).

    val trainingData =indexed2.filter("Month <= 10")
      // .filter("DepDelay > -59.7")
      // .filter("DepDelay < 79.1")
    val testData = indexed2.filter("Month > 10")

    // 7.2.- Train a Linear Regression model.
    val rf = if (mode == "Train"){
      new LinearRegression()
        .setLabelCol("ArrDelay")
        .setMaxIter(25)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)
    }else {
      null
    }

    val pipeline = if (mode == "Train"){
      new Pipeline().setStages(Array(assembler, rf))
    } else{
      null
    }

    // 8.- Train model. This also runs the indexer.
    val model = if (mode == "Train"){
      pipeline.fit(trainingData)
    }else{
      PipelineModel.load(model_path)
    }

    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "ArrDelay", "features").show(5)

    // 9.- Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    if (mode != "True"){
      model.save(model_path)
    }

  }
}