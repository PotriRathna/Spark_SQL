package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

object Exercise_1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val input = Seq(Row(Row("Potri Rathna", "", "Prabhakaran"), 19940404, "M", 3000),
    Row(Row("Nimisha ", "", "Arun"), 20190206, "M", 4000),
    Row(Row("Arun ", "", "Murugesan"), 24101990, "M", 5000),
    Row(Row("Naguna ", "Siva", "Shankar"), 15082018, "F", 4000),
    Row(Row("Janani", "", "Kumar"), 10102008, "F", 1000))

  val schema = service1.new_schema()
  val dataframe = spark.createDataFrame(spark.sparkContext.parallelize(input), schema)
  dataframe.printSchema()

  service1.select(dataframe).show()
  val newDF = service1.add_column(dataframe)
  newDF.show()
  service1.change_column(dataframe).show()
  service1.change_datatype(dataframe).printSchema()
  service1.derive_newcolum(dataframe).show()
  service1.rename_nested_column(dataframe).show()
  service1.Max_salary(dataframe).show()
  service1.drop_coulmn(newDF).show()
  println(s"Distinct count of DOB And Salary : ${service1.distinct_value(dataframe).count()}")
}
