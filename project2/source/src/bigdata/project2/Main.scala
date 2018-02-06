package bigdata.project2

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import java.text.SimpleDateFormat
import org.apache.commons.io.FilenameUtils

object Main extends App {
  override def main(args: Array[String]) {
    val inputFilePath = "project2/input/access_log"
    val conf = new SparkConf().setAppName("Spark Sort").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val logParser = new LogParser()

    val logEntries = sc.textFile(inputFilePath)
      .map(logParser.parse)
      .filter(_ != null)
      .cache()
      
    // Get the average response size per user IP and write to file
    val averagePerIP = getAverageResponseSize(logEntries)
    var writer = new PrintWriter(new File("output/average.out.txt"))
    averagePerIP.foreach(writer.println)
    writer.close()
    
    // Get top 10 visited dates and write output to file
    val top10VisitedDates = getTop10VisitedDates(logEntries)
    writer = new PrintWriter(new File("output/Top10VisitedDates.out.txt"))
    top10VisitedDates.foreach(writer.println)
    writer.close()
    
    // Get top 10 requested resource and output result to file
    val top10Resources = getTop10RequestedResourcesPerDay(logEntries)
    writer = new PrintWriter(new File("output/Top10Resources.out.txt"))
    top10Resources.foreach(writer.println)
    writer.close()
  }

  def getAverageResponseSize(logEntries: RDD[LogEntry]): Array[(String, Double)] = {
    logEntries
      .map(e => (e.ipAddress, e.responseSize))      // (ipAddress, response size)
      .groupBy(_._1)
      .sortByKey()
      .map(e => (e._1, e._2.map(_._2)))             // (ipAddress, [size1, size2, ...])
      .map(e => (e._1, average(e._2)))              // (ipAddress, avg)
      .collect()
  }

  def average(list: Iterable[Long]): Double = {
    list.sum / list.size
  }

  def getTop10VisitedDates(logEntries: RDD[LogEntry]): Array[(String, Long)] = {
    val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val logByDates = logEntries
      .map(entry => (dateFormat.format(entry.datetime), entry))

    logByDates
      .map(p => (p._1, p._2.ipAddress))  // (date, ipAddress)
      .distinct
      .map(p => (p._1, 1L))              // (date, count)
      .reduceByKey(_ + _)
      .sortBy(p => p._2, false)                // sort by counts
      .take(10)
  }
  
  def getTop10RequestedResourcesPerDay(logEntries: RDD[LogEntry]) : Array[(String, Long)] = {
    logEntries
    .map(e => (e.resourcePath, 1L))
    .reduceByKey(_ + _)
    .sortBy(p => p._2, false)
    .take(10)
  }
}