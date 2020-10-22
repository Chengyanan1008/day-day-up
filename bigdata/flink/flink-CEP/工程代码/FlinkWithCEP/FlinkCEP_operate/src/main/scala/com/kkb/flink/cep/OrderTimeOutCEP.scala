package com.kkb.flink.cep

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *  订单下单未支付检测
 */

case class OrderDetail(orderId:String,status:String,orderCreateTime:String,price :Double)

object OrderTimeOutCEP {


  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {


    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置数据的时间为准
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    import org.apache.flink.api.scala._
    //获取socket当中的数据
    val sourceStream: DataStream[String] = environment.socketTextStream("node01",9999)

    val keyedStream: KeyedStream[OrderDetail, String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      OrderDetail(strings(0), strings(1), strings(2), strings(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderDetail](Time.seconds(5)) {
      override def extractTimestamp(element: OrderDetail): Long = {
        format.parse(element.orderCreateTime).getTime
      }
    }).keyBy(x => x.orderId)

    //定义pattern，判断数据15分钟之内没有出现任何其他的操作
    val pattern: Pattern[OrderDetail, OrderDetail] = Pattern.begin[OrderDetail]("start")
      .where(order => order.status.equals("1"))
      .followedByAny("second")
      .where(x => x.status.equals("2"))
      .within(Time.minutes(15))

    val patternStream: PatternStream[OrderDetail] = CEP.pattern(keyedStream, pattern)


    val orderTimeOutOutputTag = new OutputTag[OrderDetail]("orderTimeOut")
    //如果对于一些超时的数据，可以通过侧输出流的方式进行获取到

    val selectResultStream: DataStream[OrderDetail] = patternStream.select(orderTimeOutOutputTag, new OrderTimeoutPatternFunction, new OrderPatternFunction)


    selectResultStream.print()

    //输出侧输出流
    selectResultStream.getSideOutput(orderTimeOutOutputTag).print()

    environment.execute()


  }

}





//订单超时检测   订单超时的数据，全部使用侧输出流的方式来进行捕获
class OrderTimeoutPatternFunction extends PatternTimeoutFunction[OrderDetail,OrderDetail]{
  override def timeout(pattern: util.Map[String, util.List[OrderDetail]], l: Long): OrderDetail = {
    val detail: OrderDetail = pattern.get("start").iterator().next()
    println("超时订单号为" + detail)
    detail
  }
}


class OrderPatternFunction extends PatternSelectFunction[OrderDetail,OrderDetail] {
  override def select(pattern: util.Map[String, util.List[OrderDetail]]): OrderDetail = {
    val detail: OrderDetail = pattern.get("second").iterator().next()
    println("支付成功的订单为" + detail)
    detail
  }
}