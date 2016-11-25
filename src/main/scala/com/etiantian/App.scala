package com.etiantian

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
 * Hello world!
 *
 */
case class Row(resourceId:String, userId:String, cTime:String, actionTime:String, point:Int)
case class CRow(resourceId:String, jid:String, actionTime:String, point:Int)
case class Line(resourceId:String, userId:String, actionTime:String, endTime:String, cost:Int)

object App {
  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 1) {
      Console.err.print("Missing a parameter: Properties' path")
    }
    val file = new File(args(0).substring(0, args(0).lastIndexOf("/")) + "/log4j.properties")
    if (file.exists()) {
      PropertyConfigurator.configure(args(0).substring(0, args(0).lastIndexOf("/")) + "/log4j.properties")
    }
    val logger = Logger.getLogger("MessageHandler")


    val configFile = new File(args(0))
    if (!configFile.exists()) {
      logger.error("Missing config.properties file!")
    }

    val properties = new Properties()
    properties.load(new FileInputStream(configFile))

    val sparkConf = new SparkConf().setAppName("app")
    val sc = new SparkContext(sparkConf)
    val spark = new HiveContext(sc)


    if(args.length<2 || args(1) == null || args(1).trim.length<0){

    }
    val tableList = sc.textFile(args(1))
    var hbaseRdd:RDD[(String, Row)] = null

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val userInfo = spark.sql("select user_id userId, ett_user_id jid from user_info_mysql")
    tableList.map(_.split(",")).map(array => {
      val conf = new Configuration()
      conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.quorum"))
      conf.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zkPort"))

      conf.set(TableInputFormat.INPUT_TABLE, array(0))
      //    conf.set(TableInputFormat.INPUT_TABLE,"action_logs")
      val scan = new Scan()
      scan.addFamily(array(1).getBytes)
      val proto = ProtobufUtil.toScan(scan)
      val scanToString = Base64.encodeBytes(proto.toByteArray)
      conf.set(TableInputFormat.SCAN, scanToString)

      val rdd = sc.newAPIHadoopRDD(conf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      ).map(_._2).map(result => {
        val cellList = result.rawCells()
        var resourceId = ""
        var userId = ""
        var actionTime = ""
        var cTime = ""
        var point = 0
        for (cell: Cell <- cellList) {
          val column = Bytes.toString(cell.getQualifier)
          val value = Bytes.toString(cell.getValue)


          for(i <- 2 to array.length) {
            if (column != null && column.equals(array(i))) {
              i match {
                case 2 => {
                  userId = value.trim
                }
                case 3 => {
                  resourceId = value.trim
                }
                case 4 => {
                  cTime = format.parse(value.trim).getTime.toString
                }
                case 5 => {
                  actionTime = format.parse(value.trim).getTime.toString
                }
                case 6 => {
                  if (value.indexOf(".") != -1) {
                    point = value.substring(0, value.indexOf(".")).toInt
                  }
                  else {
                    point = value.toInt
                  }
                }
              }
            }
          }
        }
        (actionTime, Row(resourceId, userId, cTime, actionTime, point))
      }).filter(tuple => !tuple._1.equals(""))

      if(hbaseRdd == null) {
        hbaseRdd = rdd
      }
      else {
        hbaseRdd.union(rdd)
      }
    })


      logger.warn("Totoal processes count = " + hbaseRdd.count())
      val lineList = hbaseRdd.map(_._2).map(result => {
        val cellList = result.rawCells()
        var resourceId = ""
        var userId = ""
        var actionTime = ""
        var point = 0
        for (cell: Cell <- cellList) {
          val column = Bytes.toString(cell.getQualifier)
          val value = Bytes.toString(cell.getValue)
          if (column != null && column.equals("resourceId")) {
            resourceId = value.trim
          }
          else if (column != null && column.equals("userId")) {
            userId = value.trim
          }
          else if (column != null && column.equals("actionTime")) {
            actionTime = format.parse(value.trim).getTime.toString
          }
          else if (column != null && column.equals("point")) {
            if (value.indexOf(".") != -1) {
              point = value.substring(0, value.indexOf(".")).toInt
            }
            else {
              point = value.toInt
            }
          }
        }
        (actionTime, Row(resourceId, userId, actionTime, point))
      }).filter(tuple => !tuple._1.equals(""))
        .sortByKey().map(tuple =>
        ((tuple._2.userId, tuple._2.resourceId), tuple._2))
        .groupByKey().map(tuple => {
        val userId = tuple._1._1
        val resourceId = tuple._1._2
        val maxLength = Math.ceil(properties.getProperty("cycle").toInt * 1.05)
        val minLength = Math.floor(properties.getProperty("cycle").toInt * 0.95)

        var list = List[Line]()

        var cTime1 = 0l
        var cTime2 = new Date().getTime

        var point1 = 0
        var point2 = 0

        var startTime: Date = null
        var endTime: Date = null
        var length = 0


        var index = 0

        for (row <- tuple._2) {
          cTime2 = row.actionTime.toLong
          point2 = row.point
          if (cTime1 != 0l) {
            val cost = (cTime2 - cTime1) / 1000
            val pointLength = point2 - point1
            if (cost > maxLength
              || pointLength > maxLength
              || pointLength < 0
              || cost < minLength) {
              if (length > 0) {
                list = list :+ Line(resourceId, userId, format.format(startTime), format.format(cTime1), length)
              }
              startTime = new Date(cTime2)
              length = 0
            }
            else {
              length = length + pointLength
              endTime = new Date(cTime2)
            }
          }
          else {
            startTime = new Date(cTime2)
          }
          cTime1 = cTime2
          point1 = point2
          index = index + 1
          if (index == tuple._2.size) {
            if (length > 0) {
              list = list :+ Line(resourceId, userId, format.format(startTime), format.format(cTime2), length)
            }
          }
        }

        list
      }).reduce((a, b) => {
        val c = a ::: b
        c
      }).map(line => {
        val obj = new JSONObject()
        obj.put("resourceId", line.resourceId)
        obj.put("userId", line.userId)
        obj.put("actionTime", line.actionTime)
        obj.put("endTime", line.endTime)
        obj.put("cost", line.cost)
        obj.toString
      })

      val df = spark.read.json(sc.parallelize(lineList))
      df.join(userInfo, Seq("userId")).registerTempTable("tempTable")
      spark.sql("insert into bd_video_logs select * from tempTable")
//        write.mode(SaveMode.Overwrite).parquet(properties.getProperty("parquet") + "/sxlogsdb_micro_course_point_log")
//      spark.read.parquet(properties.getProperty("parquet") + "/sxlogsdb_micro_course_point_log").show
  }
}
