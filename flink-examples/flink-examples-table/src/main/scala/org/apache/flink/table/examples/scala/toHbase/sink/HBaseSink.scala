package org.apache.flink.table.examples.scala.toHbase.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.examples.scala.toHbase.bean.OrderAmount
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created on 2019-09-27.
  * original -> https://github.com/zhangxiaohui4565/bd/blob/master/demo_flink/src/main/scala/com/gupao/bd/sample/flink/realtime/sink/HBaseSink.scala
  */
class HBaseSink extends RichSinkFunction[OrderAmount] {
  private var family: String = "info"
  private var connection: Connection = null

  override def open(parameters: Configuration): Unit = {
    if (connection == null) {
      val hbaseConfig = HBaseConfiguration.create()
      hbaseConfig.set("hbase.zookeeper.quorum", "localhost")
      hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
      connection = ConnectionFactory.createConnection(hbaseConfig)
    }

    connection
  }

  // 循环调用
  override def invoke(orders: OrderAmount, context: SinkFunction.Context[_]): Unit = {
    val table: Table = connection.getTable(TableName.valueOf("demo_flink:order_amount"))
    val rowkey: String = orders.dt.getTime + "_" + orders.categoryId
    val put: Put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("orderNum"), Bytes.toBytes(orders.orderNum + ""))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("orderAmount"), Bytes.toBytes(orders.orderAmount + ""))
    table.put(put)
    table.close()
  }

  override def close(): Unit = {

  }
}
