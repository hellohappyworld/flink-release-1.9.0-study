package org.apache.flink.table.examples.scala.toHbase.bean

import java.sql.Timestamp

/**
  * Created on 2019-09-24
  * original -> https://github.com/zhangxiaohui4565/bd/blob/master/demo_flink/src/main/scala/com/gupao/bd/sample/flink/realtime/bean/Orders.scala
  *
  * @param orderId
  * @param productName
  * @param productNum
  * @param amount
  * @param isPay
  * @param categoryId
  * @param orderTime
  */
case class Orders(orderId: Int, productName: String, productNum: Int, amount: Double, isPay: Int, categoryId: Int, orderTime: Timestamp)
