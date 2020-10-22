package com.kkb.flink.cep



import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

//定义温度信息pojo
case class DeviceDetail(sensorMac:String,deviceMac:String,temperature:String,dampness:String,pressure:String,date:String)

//报警的设备信息样例类
//传感器设备mac地址，检测机器mac地址，温度
case class AlarmDevice(sensorMac:String,deviceMac:String,temperature:String)

/**
 * 基于FlinkCEP的设备温度检测
 */
object FlinkTempeatureCEP {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //指定时间类型  默认数据的处理时间是processTime   实际工作当中都是使用EventTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    import org.apache.flink.api.scala._

    //接受数据
    val sourceStream: DataStream[String] = environment.socketTextStream("node01",9999)

    //第一步：获取数据流
    val deviceStream: KeyedStream[DeviceDetail, String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      DeviceDetail(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
    }).assignAscendingTimestamps(x =>{format.parse(x.date).getTime})
      .keyBy(x => x.sensorMac)  //按照传感器的mac地址进行分组


    //第二步：定义pattern的规则
    //to
    // do:定义Pattern,指定相关条件和模型序列
    //两个超过40度且1个超过50度  没有顺序，使用的是宽松近邻这种模式
    val pattern: Pattern[DeviceDetail, DeviceDetail] = Pattern.begin[DeviceDetail]("start")
      .where(x =>x.temperature.toInt >= 40)
      .followedByAny("second")
      .where(x =>x.temperature.toInt >= 50)
      .followedByAny("third")
      .where(x =>x.temperature.toInt >= 40)
      .within(Time.minutes(3))

    //todo:模式检测，将模式应用到流中
    val patternResult: PatternStream[DeviceDetail] = CEP.pattern(deviceStream,pattern)

    //todo:选取结果
    patternResult.select(new MyPatternResultFunction).print()

    //todo: 启动
    environment.execute("startTempeature")

  }
}

//自定义PatternSelectFunction
class MyPatternResultFunction extends PatternSelectFunction[DeviceDetail,AlarmDevice]{
  override def select(pattern: util.Map[String, util.List[DeviceDetail]]): AlarmDevice = {
    val startDetails: util.List[DeviceDetail] = pattern.get("start")
    val followDetails: util.List[DeviceDetail] = pattern.get("second")
    val thirdDetails: util.List[DeviceDetail] = pattern.get("third")

    val startResult: DeviceDetail = startDetails.iterator().next()
    val followResult: DeviceDetail = followDetails.iterator().next()
    val thirdResult: DeviceDetail = thirdDetails.iterator().next()

    println("第一条数据: "+startResult)
    println("第二条数据: "+followResult)
    println("第三条数据: "+thirdResult)

    AlarmDevice(thirdResult.sensorMac,thirdResult.deviceMac,thirdResult.temperature)
  }
}