package com.kkb.flink.cep

import java.util
import java.util.Collections

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector



case class UserLogin(ip:String,username:String,operateUrl:String,time:String)

object CheckIPChangeWithState {

  def main(args: Array[String]): Unit = {

    //获取flink的初始化的环境入口类
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    import org.apache.flink.api.scala._

    //todo:1、接受socket数据源
    val sourceStream: DataStream[String] = environment.socketTextStream("node01",9999)

    //todo:2、数据处理
    //map方法每一条数据都需要进行调用
    sourceStream.map(x =>{
      val strings: Array[String] = x.split(",")
      (strings(1),UserLogin(strings(0),strings(1),strings(2),strings(3)))
    } ).keyBy(x => x._1)  //  按照数据的key进行分组，将相同的key分到同一个组里面去了
      .process(new LoginCheckProcessFunction)   //高级函数
      .print()
    environment.execute("checkIpChange")


  }
}
//自定义KeyedProcessFunction类
class LoginCheckProcessFunction extends KeyedProcessFunction[String,(String,UserLogin),(String,UserLogin)]{

  //定义ListState  定义了一个listState，每个不同的key都会对应自己的一个listState，listState就是用来存储每一个key对应的数据的状态
  //简单理解listState就是一个list集合  初始化一个listState用于保存每个用户自己的数据的状态
  var listState:ListState[UserLogin]=_

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[UserLogin]("changeIp",classOf[UserLogin]))

  }

  //解析用户访问信息
  override def processElement(value: (String, UserLogin), ctx: KeyedProcessFunction[String, (String, UserLogin), (String, UserLogin)]#Context, out: Collector[(String, UserLogin)]): Unit = {
    val logins = new util.ArrayList[UserLogin]()

    //添加到list集合
    listState.add(value._2)

    import scala.collection.JavaConverters._
    val toList: List[UserLogin] = listState.get().asScala.toList
    //排序
    val sortList: List[UserLogin] = toList.sortBy(_.time)

    if(sortList.size ==2){
      val first: UserLogin = sortList(0)
      val second: UserLogin = sortList(1)

      if(!first.ip.equals(second.ip)){
        println("小伙子你的IP变了，赶紧回去重新登录一下")
      }
      //移除第一个ip，保留第二个ip
      logins.removeAll(Collections.EMPTY_LIST)
      logins.add(second)
      listState.update(logins)
    }

    out.collect(value)

  }

}
