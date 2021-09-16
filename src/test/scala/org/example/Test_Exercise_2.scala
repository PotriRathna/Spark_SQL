package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class Test_Exercise_2 extends AnyFunSuite with BeforeAndAfterEach{
  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val data = Seq(("Banana",1000,"Canada"), ("Orange",1500,"Canada"), ("Banana",1600,"USA"),
    ("Orange",2000,"USA"))
  val DF: DataFrame = Service_2.creae_df(data)

  import spark.implicits._
  val pivotDF= Service_2.create_pivotdf(DF)
  pivotDF.collect().toList should contain allElementsOf List(("Orange",1500,2000),
     ("Banana",1000,1600)).toDF.collect().toList

  val expression:String = "stack(2, 'Canada', Canada,'USA', USA) as (Country,Total)"
  Service_2.unpivotDf(pivotDF,expression).collect().toList should contain allElementsOf List(("Orange","Canada",1500),
    ("Orange","USA",2000),("Banana","Canada",1000),("Banana","USA",1600)).toDF.collect().toList
}
