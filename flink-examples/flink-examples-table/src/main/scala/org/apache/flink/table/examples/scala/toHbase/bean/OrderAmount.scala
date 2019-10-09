package org.apache.flink.table.examples.scala.toHbase.bean

import java.sql.Timestamp

/**
  * Created on 2019-09-27.
  * original -> https://github.com/zhangxiaohui4565/bd/blob/master/demo_flink/src/main/scala/com/gupao/bd/sample/flink/realtime/bean/OrerAmount.scala
  */
case class OrderAmount(dt: Timestamp, categoryId: Int, orderNum: Long, orderAmount: Double)
