package com.etiantian

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.avro.SchemaBuilder.ArrayBuilder
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

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Try

/**
 * Hello world!
 *
 */
case class Row(resourceId:String, userId:String, cTime:String, actionTime:String, point:Int, source:Int)
case class Line(resourceId:String, userId:String, startTime:String, endTime:String, startPoint: Int, endPoint:Int, cost:Int, source:Int)

object App {
  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 1) {
      Console.err.print("Missing a parameter: Properties' path")
    }
    val file = new File(args(0).substring(0, args(0).lastIndexOf("/")) + "/log4j.properties")
    if (file.exists()) {
      PropertyConfigurator.configure(args(0).substring(0, args(0).lastIndexOf("/")) + "/log4j.properties")
    }
    val logger = Logger.getLogger("mic-video-time-handler")


    val configFile = new File(args(0))
    if (!configFile.exists()) {
      logger.error("Missing config.properties file!")
    }

    val properties = new Properties()
    properties.load(new FileInputStream(configFile))

    val sparkConf = new SparkConf().setAppName("mic-video-time-handler")
    val sc = new SparkContext(sparkConf)
    val spark = new HiveContext(sc)


    if (args.length < 2 || args(1) == null || args(1).trim.length < 0) {
      logger.error("Missing tableList file!")
    }
    val tableList = Source.fromFile(args(1))

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var isDrop = false
    val userInfo = spark.sql("select user_id userId, ett_user_id jid from user_info_mysql where ett_user_id is not null")
      .rdd.filter(row => {
      val try1 = Try(row.get(0).toString.toInt)
      val try2 = Try(row.get(1).toString.toInt)
      try1.isSuccess && try2.isSuccess
    }).map(row => (row.getInt(0).toString, row.getInt(1).toString))
    for(table <- tableList.getLines()) {
      val array = table.split(",")
      val conf = new Configuration()
      conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.quorum"))
      conf.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zkPort"))

      conf.set(TableInputFormat.INPUT_TABLE, array(0))
      val scan = new Scan()
      scan.addFamily(array(1).getBytes)
      val proto = ProtobufUtil.toScan(scan)
      val scanToString = Base64.encodeBytes(proto.toByteArray)
      conf.set(TableInputFormat.SCAN, scanToString)

      var rdd = sc.newAPIHadoopRDD(conf,
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


          for (i <- 2 until array.length - 1) {
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
                    val str = value.substring(0, value.indexOf("."))
                    if (Try(str.toInt).isSuccess) {
                      point = str.toInt
                    }
                    else {
                      point = -1
                    }
                  }
                  else {
                    if (Try(value.toInt).isSuccess) {
                      point = value.toInt
                    }
                    else {
                      point = -1
                    }
                  }
                }
              }
            }
          }
        }
        (userId, Row(resourceId, userId, cTime, actionTime, point, array(8).toInt))
      }).filter(tuple =>
        !tuple._1.equals("") &&
          tuple._2.point >= 0 &&
          Try(tuple._2.cTime.toLong).isSuccess &&
          Try(tuple._2.actionTime.toLong).isSuccess)

      if (array(8) != null && array(8).toInt >= 2) {  //设定小鱼2为网校（存的是jid）
        rdd = rdd.join(userInfo).filter(tuple => {
          val isnull = (tuple._2._2 != null && tuple._2._2.trim.length > 0 && !tuple._2._2.equals("null"))
          isnull
        }).map(tuple => {
          val row = tuple._2._1
          val jid = tuple._2._2
          (row.userId, Row(row.resourceId, jid, row.cTime, row.actionTime, row.point, row.source))
        })
      }

      logger.warn(array(0)+"==========================================")
      val lineList = rdd.filter(tuple => !tuple._1.equals("")).
        map(tuple =>
        ((tuple._2.userId, tuple._2.resourceId), tuple._2))
        .groupByKey().map(tuple => {

        val arrayBuffer = ArrayBuffer[Row]()

        for (row <- tuple._2) {
          arrayBuffer.append(row)
        }
        val rowArray = arrayBuffer.toArray

        var isChanged = true
        for(i <- 0 until rowArray.length if isChanged) {
          var tmpRow = rowArray(0)
          isChanged = false

          for(j <- 0 until rowArray.length - i - 1){
            if(rowArray(j).actionTime > rowArray(j+1).actionTime){
              tmpRow = rowArray(j)
              rowArray(j) = rowArray(j + 1)
              rowArray(j+1) = tmpRow
              isChanged = true
            }
          }
        }

        val userId = tuple._1._1
        val resourceId = tuple._1._2
        val maxLength = Math.ceil(array(7).toInt * 1.05)
        val minLength = Math.floor(array(7).toInt * 0.95)
        var list = List[Line]()

        var actionTime1 = 0l
        var actionTime2 = new Date().getTime

        var point1 = 0
        var point2 = 0

        var startTime: Date = null
        var endTime: Date = null
        var startPoint = 0
        var endPoint = 0

        var length = 0

        var cTime1 = 0l
        var cTime2 = new Date().getTime

        var index = 0

        for (row <- rowArray) {
          actionTime2 = row.actionTime.toLong
          cTime2 = row.cTime.toLong
          point2 = row.point
          if (actionTime1 != 0l) {
            val cost = (actionTime2 - actionTime1) / 1000
            val pointLength = point2 - point1
            if (cost > maxLength
              || pointLength > maxLength
              || pointLength < 0
              || cost < minLength) {
              if (length > 0) {
                list = list :+ Line(resourceId, userId, format.format(startTime), format.format(endTime), startPoint, endPoint, length, row.source)
              }
              startTime = new Date(cTime2)
              startPoint = point2
              length = 0
            }
            else {
              length = length + pointLength
              endTime = new Date(cTime2)
              endPoint = point2
            }
          }
          else {
            startTime = new Date(cTime2)
            startPoint = point2
          }
          actionTime1 = actionTime2
          cTime1 = cTime2
          point1 = point2
          index = index + 1
          if (index == tuple._2.size) {
            if (length > 0) {
              list = list :+ Line(resourceId, userId, format.format(startTime), format.format(endTime), startPoint, endPoint, length, row.source)
            }
          }
        }

        list
      }).reduce((a, b) => {
        a ::: b
      })
      logger.warn("++++++++++++++++++++++++++++++++++++++++++++++")
      val lineListRDD = sc.parallelize(lineList).map(line => {
        val obj = new JSONObject()
        obj.put("resourceId", line.resourceId)
        obj.put("jid", line.userId)
        obj.put("startTime", line.startTime)
        obj.put("endTime", line.endTime)
        obj.put("startPoint", line.startPoint)
        obj.put("endPoint", line.endPoint)
        obj.put("cost", line.cost)
        obj.put("source", line.source)
        obj.toString
      })
      val df = spark.read.json(lineListRDD)
      df.registerTempTable("tempTable")
      if(!isDrop && args.length>=3 && args(2).equals("true")) {
        logger.warn("##################################################")
        spark.sql("drop table bd_video_logs")
        spark.sql("create table bd_video_logs stored as orc as select * from tempTable")
        isDrop = true
        logger.warn("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
      }
      else {
        logger.warn("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        spark.sql("insert into bd_video_logs select * from tempTable")
        logger.warn("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      }
    }
  }
}
