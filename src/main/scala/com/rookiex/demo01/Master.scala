package com.rookiex.demo01

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.language.postfixOps
import scala.collection.mutable
import scala.concurrent.duration._

/**
 * @Author RookieX
 * @Date 2021/7/24 4:56 下午
 * @Description:
 */
class Master extends Actor{

  //创建HashMap集合, 用来存放workerId和内容ΩΩ
  val idToWorker = new mutable.HashMap[String, WorkerInfo]()


  override def preStart(): Unit = {

    //启动一个内部定时器
    import context.dispatcher
    context.system.scheduler.schedule(0 millisecond, 30000 millisecond, self, CheckTimeOutWorker )
  }

  //receive里边有偏函数, 用来实现模式匹配, 接收消息并处理消息
  override def receive: Receive = {
    case "Hi" =>{
      println("hi---")
    }
    case RegisterWorker(workerId, memory, cores) => {
      //接受到worker发送的消息后, 将对应Worker的数据保存起来
      val workerInfo = new WorkerInfo(workerId, memory, cores)
      //保存到HashMap中
      idToWorker(workerId) = workerInfo
      //返回注册成功的消息
      sender() ! RegisteredWorker
    }

      //Worker发送给Master的心跳消息, 通过定期更新最后一次的心跳时间
    case HeartBeat(workerId) => {
      if (idToWorker.contains(workerId)) {
        val workerInfo: WorkerInfo = idToWorker(workerId)
        workerInfo.lastHeartBeatTime = System.currentTimeMillis()
      }
    }
      //Master自己发送给自己的内部消息
    case CheckTimeOutWorker => {
      //筛选出超时的Worker
      val deadWorkers: Iterable[WorkerInfo] = idToWorker.values.filter(w => System.currentTimeMillis() - w.lastHeartBeatTime > 20000)
      deadWorkers.foreach(w => {
        idToWorker -= w.workerId
      })

      println(s"current alive worker size is ${idToWorker.size}")
    }
  }
}

object Master{
  def main(args: Array[String]): Unit = {
    //创建一个ActorSystem (单例)
    val config = ConfigFactory.parseString(
      """
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname = localhost
        |akka.remote.netty.tcp.port = 8888
        |""".stripMargin)
    //创建ActorSystem
    val masterActorSystem: ActorSystem = ActorSystem.apply("MasterActorSystem", config)
    //使用ActorSystem创建Actor
//    val actorRef: ActorRef = masterActorSystem.actorOf(Props[Master], "MasterActor")
    masterActorSystem.actorOf(Props[Master], "MasterActor")
    //自己给自己发消息(异步消息)
//    actorRef.!("Hi")
  }
}
