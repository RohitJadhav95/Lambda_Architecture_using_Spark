package streaming

import config.Settings
import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}

import scala.Predef.Set
//import org.apache.spark.sql.hive.test.TestHive.executionHive.state
import utils.SparkUtils._
import org.apache.spark.streaming._
import functions._
import com.twitter.algebird.HyperLogLogMonoid

object StreamingJob {

  def main(args: Array[String]) : Unit =  {
    // get Spark Context
    val sc = getSparkContext ("Lambda with Spark")
    val sqlContext = getSqlContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds (4)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {

      val ssc = new StreamingContext(sc, batchDuration)
      val wlc = Settings.WebLogGen
      val topic = wlc.kafkaTopic

//   Using Kafka Receiver Model Approach
//      val kafkaParams = Map(
//        "zookeeper.connect" -> "localhost:2181",
//        "group.id" -> "lambda",
//        "auto.offset.reset" -> "largest"
//      )

      // Using Kafka Direct Model Approach
      val kafkaDirectParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "smallest"
      )

      val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaDirectParams, Set(topic)
      )

//     Using Kafka Receiver Model Approach
//     val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
//        ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_AND_DISK)
//        .map(_._2)

//      val inputPath = isIDE match {
//        case true => "file:///C:/Users/Rohit Jadhav/Lambda_Project/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
//        case false => "file:///vagrant/input"
//      }

//      val textDStream = ssc.textFileStream(inputPath)

//      val activityStream = textDStream.transform( input =>
//      val activityStream = kafkaStream.transform( input =>
      val activityStream = kafkaDirectStream.transform( input =>
        functions.rddToRDDActivity(input)
      ).cache()

      activityStream.foreachRDD( rdd => {
        val activityDF = rdd
          .toDF()
          .selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page", "visitor", "product",
          "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition", "inputProps.fromOffset as fromOffset",
          "inputProps.untilOffset as untilOffset")

        activityDF
          .write
          .partitionBy("topic", "kafkaPartition", "timestamp_hour")
          .parquet("hdfs://lambda-pluralsight:9000/lambda/weblogs-app1/")
      })

      val activityStateSpec =
        StateSpec
          .function(mapActivityStateFunc)
          .timeout(Minutes(120))

        val statefulActivityByProduct =  activityStream.transform( rdd => {
          val df = rdd.toDF()
          df.registerTempTable("activity")

          val activityByProduct = sqlContext.sql(
          """SELECT product,
            |       timestamp_hour,
            |       SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
            |       SUM(CASE WHEN action = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
            |       SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
            | FROM activity
            | GROUP BY product, timestamp_hour
            |""")

          activityByProduct
            .map{ r => ((r.getString(0), r.getLong(1)),
            ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
            )}
      }).mapWithState(activityStateSpec)

      val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()

      activityStateSnapshot
        .reduceByKeyAndWindow(
          (a,b) => b,
          (x,y) => x,
          Seconds(30 / 4 * 4)
        ) //only save or expose the snapshot every x seconds
        .foreachRDD(rdd => rdd.map( sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
          .toDF().registerTempTable("ActivityByProduct"))

      // unique visitors by product
      val visitorStateSpec =
        StateSpec
          .function(mapVisitorsStateFunc)
          .timeout(Minutes(120))

      val statefulVisitorsByProduct = activityStream.map( a => {
        val hll = new HyperLogLogMonoid(12)
        ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
      }).mapWithState(visitorStateSpec)

      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()

      visitorStateSnapshot
        .reduceByKeyAndWindow(
          (a,b) => b,
          (x,y) => x,
          Seconds(30/4 * 4)
        ) //only save or expose the snapshot every x seconds
        .foreachRDD(rdd => rdd.map( sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
          .toDF().registerTempTable("VisitorsByProduct"))

      ssc
    }

    val ssc =  getStreamingContext(streamingApp, sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()

  }
}


// UpdateStateByKey Example
//          .updateStateByKey((newItemsPerKey : Seq[ActivityByProduct], currentState : Option[(Long, Long, Long, Long)]) => {
//
//          var (prevTimeStamp, purchase_count, add_to_cart_count, page_view_count) = currentState.getOrElse(0L, 0L, 0L, 0L)
//          var result : Option[(Long, Long, Long, Long)] = null
//
//          if(newItemsPerKey.isEmpty) {
//            if(System.currentTimeMillis() - prevTimeStamp > 30000 + 4000) {
//              result = None
//            } else {
//              result = Some(prevTimeStamp, purchase_count, add_to_cart_count, page_view_count)
//            }
//          } else {
//            newItemsPerKey.foreach( a => {
//              purchase_count += a.purchase_count
//              add_to_cart_count += a.add_to_cart_count
//              page_view_count += a.page_view_count
//            })
//
//            result = Some((System.currentTimeMillis(), purchase_count,add_to_cart_count, page_view_count))
//          }
//
//          result
//        })
