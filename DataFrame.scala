package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .appName("Spark TP")
                  .config("spark.some.config.option", "some-value")
                  .getOrCreate()
    
    val df_explo = spark.read
      .option("header", true)
      .option("delimiter",";")
      .csv("./src/main/resources/laposte_hexasmal.csv")

    df_explo.printSchema();
    print("Count : \n");
    print(df_explo.select("nom_de_la_commune").distinct().count());
    

  }

}
