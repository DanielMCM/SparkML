package upm.bd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
object App {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("My first Spark application").set("spark.local.dir", "/tmp/spark-temp")
    val sc = new SparkContext(conf)
    val data = sc.textFile("C:\\Users\\34629\\Desktop\\09 DS Madrid\\01 UPM\\1º Semestre\\01 Big Data\\02 Hw\\Hw2\\test.txt")
    val numAs = data.filter(line => line.contains("a")).count()
    val numBs = data.filter(line => line.contains("b")).count()
    println(s"Lines with a: ${numAs}, Lines with b: ${numBs}")
  }
}


