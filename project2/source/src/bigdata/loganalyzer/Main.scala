package bigdata.loganalyzer

import java.io._
import java.nio.file.Files
import java.nio.file.Paths
import scala.io.Source
import scala.math.random
import org.apache.spark._
import org.apache.spark.rdd.RDD

object Main extends App {
  override def main(args: Array[String]) {
    // Parse command line parameters
    //    val inputFilePath = args(0).toString
    //    val outputFile = args(1).toString

    val inputFilePath = "project2/input/access_log"
    //    if (!Files.exists(Paths.get(inputFilePath))) {
    //      throw new IllegalArgumentException("Input file doesn't exist!!!")
    //    }

    val conf = new SparkConf().setAppName("Spark Sort").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val logParser = new LogParser()

    val logEntries = sc.textFile(inputFilePath)
      .map(logParser.parse)
      .filter(_ != null)
      .cache()

    // Get the average response size per user IP
    val averagePerIP = getAverageResponseSize(logEntries)

    // Write the result to file
    val writer = new PrintWriter(new File("output/average.out.txt"))
    averagePerIP.foreach(writer.println)
    writer.close()
  }

  def getAverageResponseSize(logEntries: RDD[LogEntry]): Array[(String, Double)] = {
    logEntries
      .map(e => (e.ipAddress, e.responseSize))
      .groupBy(_._1)
      .sortByKey()
      .map(e => (e._1, e._2.map(_._2)))
      .map(e => (e._1, average(e._2)))
      .collect()
  }

  def average(list: Iterable[Long]): Double = {
    list.sum / list.size
  }
}