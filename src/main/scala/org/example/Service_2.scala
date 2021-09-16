package org.example

import org.apache.spark.sql
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}

object Service_2 {
  def creae_df(input: Seq[(String,Int,String)])(implicit spark:SparkSession):DataFrame={
    import spark.sqlContext.implicits._
    input.toDF("Product","Amount","Country")
  }
  def create_pivotdf(Df:sql.DataFrame):sql.DataFrame={
    Df.groupBy("Product").pivot("Country").sum("Amount")
  }
  def unpivotDf(pivotDF:sql.DataFrame,expression:String)(implicit spark:SparkSession):sql.DataFrame={
    import spark.sqlContext.implicits._
    pivotDF.select($"Product",
      expr(expression))
      .where("Total is not null")
  }

}
