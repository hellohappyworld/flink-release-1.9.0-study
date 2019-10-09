package org.apache.flink.table.examples.scala.toHbase

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.examples.scala.toHbase.bean.{OrderAmount, Orders}
import org.apache.flink.table.examples.scala.toHbase.sink.HBaseSink
import org.apache.flink.table.examples.scala.toHbase.udf.Add
import org.apache.flink.types.Row

/**
  * Created on 2019-09-17.
  * original -> https://github.com/zhangxiaohui4565/bd/blob/af3cbd1078224a76e0855e429575b504a48246a9/demo_flink/src/main/scala/com/gupao/bd/sample/flink/realtime/sql/OrderPayCount.scala
  * 有效订单统计金额及其订单量统计
  * hbase:
  * create_namespace ''demo_flink''
  * create ''demo_flink:order_amount'',''info''
  *
  */
object OrderPayCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers", "10.80.28.154:9092")
    props.put("auto.offset.reset", "latest")
    props.setProperty("group.id", "OrderPayCount")

    val consumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("app_newsapp_xinjiqun", new SimpleStringSchema(), props)

    val orders: Table = env.addSource(consumer)
      .map(line => {
        try {
          val lineArr = line.split("\t")
          val ipStr: String = lineArr(1)
          val arr = ipStr.split("\\.")
          var isPay: Int = 0
          if (lineArr(7) == "wifi")
            isPay = 1
          Orders(arr(0).toInt, lineArr(6).split(",")(1), arr(1).toInt, arr(2).toDouble, isPay, arr(3).toInt, Timestamp.valueOf(lineArr(10).replace("+", " ")))
          //          Orders(arr(0).toInt, lineArr(7), arr(1).toInt, arr(2).toDouble, isPay, arr(3).toInt, Timestamp.valueOf(lineArr(10).replace("+", " ")))
        } catch {
          case e: Exception =>
            Orders(0, "err", 0, 0.0, 0, 0, Timestamp.valueOf("2019-09-24 15:19:21"))
        }
      })
      .assignAscendingTimestamps(_.orderTime.getTime)
      .toTable(tEnv, 'orderId, 'productName, 'productNum, 'amount, 'isPay, 'categoryId, 'orderTime.rowtime)

    tEnv.registerFunction("add", new Add(3))
    tEnv.registerTable("Orders", orders)

    val sql: String =
      """
select
 HOP_START(orderTime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE),
 add(categoryId),
 COUNT(orderId),
 SUM(productNum*amount)
 FROM Orders
 WHERE isPay=1
 GROUP BY HOP(orderTime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE), categoryId
    """.stripMargin

    val result: Table = tEnv.sqlQuery(sql)

    result.toAppendStream[Row]
      .print()

    //    result.toAppendStream[OrderAmount].addSink(new HBaseSink)

    env.execute("OrderPayCount")
  }
}
