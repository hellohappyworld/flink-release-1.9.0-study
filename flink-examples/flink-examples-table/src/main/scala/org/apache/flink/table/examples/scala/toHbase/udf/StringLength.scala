package org.apache.flink.table.examples.scala.toHbase.udf

import org.apache.flink.table.functions.ScalarFunction

/**
  * Created on 2019-09-24
  * original -> https://github.com/zhangxiaohui4565/bd/blob/master/demo_flink/src/main/scala/com/gupao/bd/sample/flink/realtime/sql/udf/StringLength.scala
  * 自定义字符串长度函数
  */
class StringLength extends ScalarFunction {
  def eval(s: String): Long = {
    if (s.isEmpty)
      0
    else
      s.length
  }

  def eval(s1: String, s2: String): Long = eval(s1) + eval(s2)

}
