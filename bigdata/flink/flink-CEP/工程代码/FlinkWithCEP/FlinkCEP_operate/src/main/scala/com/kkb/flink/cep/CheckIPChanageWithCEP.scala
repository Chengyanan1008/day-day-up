package com.kkb.flink.cep

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

case class UserLoginInfo(ip:String,username:String,operateUrl:String,time:String)

object CheckIPChanageWithCEP {
  def main(args: Array[String]): Unit = {
    //
    //获取flink的初始化的环境入口类
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    import org.apache.flink.api.scala._
    //接受数据
    val sourceStream = environment.socketTextStream("node01", 9999)

    //获取到了数据流
    val keyedStream: KeyedStream[(String, UserLoginInfo), String] = sourceStream.map(x => {
      val strings = x.split(",")

      (strings(1), UserLoginInfo(strings(0), strings(1), strings(2), strings(3)))
    }).keyBy(_._1)

    //第二步：定义规则
    val pattern: Pattern[(String, UserLoginInfo), (String, UserLoginInfo)] = Pattern.begin[(String, UserLoginInfo)]("start")
      .where(x => x._2.username != null)
      .next("second")
      .where(new IterativeCondition[(String, UserLoginInfo)] {
        //使用filter方法，返回一个true或者false结果
        override def filter(value: (String, UserLoginInfo), context: IterativeCondition.Context[(String, UserLoginInfo)]): Boolean = {
          var flag: Boolean = false

          val firstValues: util.Iterator[(String, UserLoginInfo)] = context.getEventsForPattern("start").iterator()
          while (firstValues.hasNext) {
            val tuple: (String, UserLoginInfo) = firstValues.next()
            //判断第一条数据与第二条数据是否相等
            if (!tuple._2.ip.equals(value._2.ip)) {
              flag = true
            }
          }
          flag
        }
      }).within(Time.seconds(180))


    //这里面得到的就是满足条件的数据
    val patternStream: PatternStream[(String, UserLoginInfo)] = CEP.pattern(keyedStream, pattern)
    //输出最终的结果流数据
    patternStream.select(new MyPatternSelectFunction)


    environment.execute("flink_cep")



  }


}




//自定义PatternSelectFunction类
class MyPatternSelectFunction extends PatternSelectFunction[(String,UserLoginInfo),(String,UserLoginInfo)]{
  override def select(map: util.Map[String, util.List[(String, UserLoginInfo)]]): (String, UserLoginInfo) = {
    // 获取Pattern名称为start的事件
    val startIterator= map.get("start").iterator()

    if(startIterator.hasNext){
      println("满足start模式中的数据："+startIterator.next())
    }


    //获取Pattern名称为second的事件
    val secondIterator = map.get("second").iterator()


    var tuple:(String,UserLoginInfo)=null

    if(secondIterator.hasNext){
      tuple=secondIterator.next()
      println("满足second模式中的数据："+ tuple)
    }

    tuple
  }
}




