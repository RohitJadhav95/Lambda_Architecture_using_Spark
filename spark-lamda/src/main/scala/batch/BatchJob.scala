package batch

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import domain._
import org.apache.spark.sql.{SQLContext, SaveMode}
import utils.SparkUtils._

object BatchJob {
  def main(args: Array[String]): Unit = {

    // get spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSqlContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    //Initialize input RDD
    val sourceFile = "file:///vagrant/data.tsv"
    val input = sc.textFile(sourceFile)

    val inputDF = input.flatMap { line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong/MS_IN_HOUR * MS_IN_HOUR, record(1), record(2),
          record(3), record(4), record(5), record(6)))
      else
        None
    }.toDF()

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour") / 1000),1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    ).cache()

    df.registerTempTable("activity")

    val visitorsByProduct = sqlContext.sql(
      """ SELECT product, timestamp_hour, COUNT(DISTINCT visitor) AS Unique_Visitors
        |  FROM activity
        |  GROUP BY product, timestamp_hour
        |""".stripMargin
    )

    visitorsByProduct.printSchema()

    val activityByProduct = sqlContext.sql(
      """SELECT product,
        |       timestamp_hour,
        |       SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        |       SUM(CASE WHEN action = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
        |       SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
        | FROM activity
        | GROUP BY product, timestamp_hour
        |""".stripMargin
    ).cache()

    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)

  }

}
