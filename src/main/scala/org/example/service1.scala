package org.example
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object service1 {

  def new_schema(): StructType = {
    new StructType()
      .add("Name", new StructType()
        .add("Firstname", StringType)
        .add("Middlename", StringType)
        .add("Lastname", StringType))
      .add("DOB", IntegerType)
      .add("Gender", StringType)
      .add("Salary", IntegerType)
  }

  //2.	Select firstname, lastname and salary from Dataframe.
  def select(df: DataFrame): DataFrame = {
    df.select(col("Name.Firstname").as("fname"),
      col("Name.Lastname").as("lname"),
      col("salary"))
  }

  //3.	Add Country, department, and age column in the dataframe.
  def add_column(df: DataFrame): DataFrame = {
    val column = select(df)
    column.withColumn("Country", lit("INDIA"))
      .withColumn("Age", lit(26)).withColumn("Department", lit("IT"))
  }

  //4.	Change the value of salary column.
  def change_column(df: DataFrame): DataFrame = {
    df.withColumn("salary", col("salary") * 25)
  }

  //5.	Change the data types of DOB and salary to String
  def change_datatype(df: DataFrame): DataFrame = {
    df.withColumn("salary", col("salary").cast("String"))
      .withColumn("DOB", col("DOB").cast("String"))
  }

  // 6.	Derive new column from salary column.
  def derive_newcolum(df: DataFrame): DataFrame = {
    df.withColumn("New_Salary", col("salary") * 2)
  }

  //7.	Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)
  def rename_nested_column(df: DataFrame): DataFrame = {
    df.select(col("Name.Firstname").as("firstposition"),
      col("Name.Middlename").as("secondposition"),
      col("Name.Lastname").as("lastposition"))
  }

  // 8.	Filter the name column whose salary in maximum.
  def Max_salary(df: DataFrame): DataFrame = {
    val Maxsalary = df.select(col("salary")).collect()
      .toList.flatMap(_.toSeq.asInstanceOf[Seq[Int]]).max
    df.filter(df("Salary") === Maxsalary)
  }

  //9.	Drop the department and age column.
  def drop_coulmn(df: DataFrame): DataFrame = {
    df.drop("Department", "Age")
  }

  //10.	List out distinct value of dob and salary
  def distinct_value(df: DataFrame): DataFrame = {
    df.dropDuplicates("DOB", "Salary")
  }
}
