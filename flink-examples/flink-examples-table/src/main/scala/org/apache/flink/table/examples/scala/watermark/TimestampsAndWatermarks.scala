package org.apache.flink.table.examples.scala.watermark

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

import scala.util.Random

/**
  * Created on 2019-10-08.
  * original -> https://github.com/liyuxin221/flink-base/blob/c310223551c3f8dbd757bc5e54559884f12f923b/src/main/scala/com/itheima/flinkSQL/Stream_SQL.scala
  */
object TimestampsAndWatermarks {

  case class Order(id: String, userID: Int, money: Int, time: Long)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = StreamTableEnvironment.create(env)

    val waterMarkDataStream: DataStream[Order] = env.addSource(new mySource)
      // 每隔1秒生成一个订单，添加水印允许延迟2秒
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Order] {
      var currentTimestamp = 0L

      val timedInterval = 2000L

      // 获取水印时间
      override def getCurrentWatermark: Watermark = {
        val watermark: Watermark = new Watermark(currentTimestamp - timedInterval)
        watermark
      }

      // 抽取eventTimeStamp
      override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
        currentTimestamp = Math.max(element.time, currentTimestamp)
        currentTimestamp
      }
    })

    tableEnv.registerDataStream("streamTable", waterMarkDataStream, 'id, 'userID, 'money, 'ordertime.rowtime)

    // 编写SQL语句统计用户订单总数，最大金额，最小金额
    // 使用tableEnv.sqlQuery执行sql语句
    // 分组时要使用tumble(时间列，interval '窗口时间' second)来创建窗口
    val result: Table = tableEnv.sqlQuery(
      """
select
 userID,
 count(1) as totalCount,
 max(money) as maxMoney,
 min(money) as minMoney,
 sum(money) as totalMoney
from streamTable
group by
userID,
tumble(ordertime, interval '5' second)
      """.stripMargin
    )

    val resultDs: DataStream[Row] = tableEnv.toAppendStream[Row](result)
    resultDs.print()

    env.execute("TimestampsAndWatermarks")
  }

  class mySource extends RichSourceFunction[Order] {

    var flag: Boolean = true

    // 使用for循环生成1000个订单
    // 随机生成订单ID（UUID）
    // 随机生成用户ID（0-2）
    // 随机生成订单金额（0-100）
    // 时间戳为当前系统时间
    override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
      while (flag) {
        for (i <- 1 until 1000) {
          val order: Order = Order(UUID.randomUUID().toString, Random.nextInt(3), Random.nextInt(101), System.currentTimeMillis())
          ctx.collect(order)
          TimeUnit.SECONDS.sleep(1)
        }
      }
    }

    override def cancel(): Unit = {
      flag = false
    }
  }

}

