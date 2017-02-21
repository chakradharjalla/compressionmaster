package hcsc

import org.apache.spark.sql.types._

/**
  * Created by PJalla on 7/27/2016.
  */
class helperfunctions {


  def convertDate(df: org.apache.spark.sql.DataFrame, name: String) = {
    val df_1 = df.withColumnRenamed(name, "swap")
    df_1.withColumn(name, df_1.col("swap").cast(DateType)).drop("swap")
  }

  def convertTimestampType(df: org.apache.spark.sql.DataFrame, name: String) = {
    val df_1 = df.withColumnRenamed(name, "swap")
    df_1.withColumn(name, df_1.col("swap").cast(TimestampType)).drop("swap")
  }

  def convertint(df: org.apache.spark.sql.DataFrame, name: String) = {
    val df_1 = df.withColumnRenamed(name, "swap")
    df_1.withColumn(name, df_1.col("swap").cast(IntegerType)).drop("swap")
  }

  def convertfloat(df: org.apache.spark.sql.DataFrame, name: String) = {
    val df_1 = df.withColumnRenamed(name, "swap")
    df_1.withColumn(name, df_1.col("swap").cast(FloatType)).drop("swap")
  }


}
