package hcsc

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by PJalla on 8/10/2016.
  */



object EncounterCompressionMaster{


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("EncounterCompressionMaster").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val help = new helperfunctions
    val warePath = "/apps/hive/warehouse"
    val dbName = readLine("enter the database name > ")
    sparkSession.sql("CREATE DATABASE IF NOT EXISTS "+dbName)
    sparkSession.sql("USE "+dbName)

    val load=new CompressionMaster(sparkSession: SparkSession)




    val cvtencounterilhmo = sparkSession.read.option("header", true).option("delimiter", "\t").csv("D:\\workspace\\EncounterCompressionMaster\\EncounterCompressionMaster\\cvtencounterilhmo.csv")
    cvtencounterilhmo.createOrReplaceTempView("cvtencounterilhmo")
    //cvtencounterilhmo.printSchema()

    val groupmaster = sparkSession.read.parquet("D:\\workspace\\EncounterCompressionMaster\\EncounterCompressionMaster\\paraquet_inputfiles\\groupmaster")
    groupmaster.createOrReplaceTempView("groupmaster")


    val compressionmaster=sparkSession.read.parquet("D:\\workspace\\EncounterCompressionMaster\\EncounterCompressionMaster\\paraquet_inputfiles\\compressionmaster")
    compressionmaster.createOrReplaceTempView("compressionmaster")

    val cvticdmap = sparkSession.read.parquet("D:\\workspace\\EncounterCompressionMaster\\EncounterCompressionMaster\\paraquet_inputfiles\\cvticdmap")
    cvticdmap.createOrReplaceTempView("_cvticdmap")



//    // for cluster
//
//
//    val cvtencounterilhmo = sparkSession.read.parquet("/lake/hcsc_er/ETLRUNID=201606/Encounter IL HMO/*")
//    cvtencounterilhmo.createOrReplaceTempView("cvtencounterilhmo")
//
//    val groupmaster = sparkSession.read.parquet("/apps/hive/warehouse/hcsc_er_21.db/groupmaster")
//    groupmaster.createOrReplaceTempView("groupmaster")
//
//
//    val compressionmaster=sparkSession.read.parquet("/apps/hive/warehouse/hcsc_er_21.db/compressionmaster")
//    compressionmaster.createOrReplaceTempView("compressionmaster")
//
//
//    val cvticdmap = sparkSession.read.parquet("/lake/hcsc_er/static/ICD Map/icd_map.txt")
//    cvticdmap.createOrReplaceTempView("_cvticdmap")
//
//


    load.load_CompressionMaster



  }
}