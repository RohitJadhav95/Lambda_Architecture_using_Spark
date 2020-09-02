package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Duration}

object SparkUtils {
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName: String) = {

    var checkPointDirectory = ""

    //get spark configuration
    val conf = new SparkConf().setAppName(appName)

    //Check if running from IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir","C:\\Hadoop\\bin\\winutils")
      conf.setMaster("local[*]")
      checkPointDirectory = "file:///C:/tmp"
    } else {
      checkPointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    //setting up spark context
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkPointDirectory)
    sc
  }

  def getSqlContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext ,sc:  SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true )
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }

}
