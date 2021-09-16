package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Exercise_2 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  //1.	Create a non-nested dataframe with product, amount and country fields.
  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"), ("Banana",2000,"Sweden"))
  val df= Service_2.creae_df(data)
  df.show()

  //2.	Find total amount exported to each country of each product.
  //Hint:- use pivot function
  val pivotDF = Service_2.create_pivotdf(df)
  pivotDF.show()
  //3.	Perform unpivot function on output of question 2.
  val expression = "stack(3, 'Sweden', Sweden, 'China', China, 'USA', USA) as (Country,Total)"
  Service_2.unpivotDf(pivotDF,expression).show()

}
