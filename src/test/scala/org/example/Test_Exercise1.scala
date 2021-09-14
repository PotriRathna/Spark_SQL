package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class Test_Exercise1 extends AnyFunSuite with BeforeAndAfterEach{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val input = Seq(Row(Row("ABC", "","XYZ"), 19940404, "M", 3000),
    Row(Row("EFG", "LMN","HIJ"), 20190206, "M", 5000),
    Row(Row("MIN", "SPT","BCD"), 15082018, "F", 4000))
  val schema: StructType = service1.new_schema()
  val dataframe: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(input), schema)
  val newDF: DataFrame = service1.add_column(dataframe)
  println(service1.change_datatype(dataframe).printSchema())
  import spark.implicits._
  service1.select(dataframe).collect().toList should contain allElementsOf List(("ABC","XYZ",3000),("EFG","HIJ",5000),("MIN","BCD",4000))
            .toDF.collect().toList
  service1.add_column(dataframe).collect().toList should contain allElementsOf List(("ABC","XYZ",3000,"INDIA",26,"IT"),("EFG","HIJ",5000,"INDIA",26,"IT"),("MIN","BCD",4000,"INDIA",26,"IT"))
    .toDF.collect().toList
  service1.change_column(dataframe).collect().toList should contain allElementsOf List((("ABC","","XYZ"),19940404,"M",75000)
    ,(("EFG","LMN","HIJ"),20190206,"M",125000),(("MIN","SPT","BCD"),15082018,"F",100000)).toDF.collect().toList
  service1.derive_newcolum(dataframe).collect().toList should contain allElementsOf List((("ABC","","XYZ"),19940404,"M",3000,6000)
    ,(("EFG","LMN","HIJ"),20190206,"M",5000,10000),(("MIN","SPT","BCD"),15082018,"F",4000,8000)).toDF.collect().toList
  service1.rename_nested_column(dataframe).collect().toList should contain allElementsOf List(("ABC","","XYZ"),("EFG","LMN","HIJ"),("MIN","SPT","BCD"))
    .toDF.collect().toList
  service1.Max_salary(dataframe).collect().toList should contain allElementsOf List((("EFG","LMN","HIJ"),20190206,"M",5000)).toDF.collect().toList
  service1.drop_coulmn(newDF).collect().toList should contain allElementsOf List(("ABC","XYZ",3000,"INDIA"),("EFG","HIJ",5000,"INDIA"),("MIN","BCD",4000,"INDIA"))
    .toDF.collect().toList
 assert(service1.distinct_value(dataframe).count()===3)
}
