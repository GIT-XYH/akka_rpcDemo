package com.rookiex.demo02

import akka.actor.{Actor, ActorSystem, Props}
import com.rookiex.demo01.{CheckTimeOutWorker, HeartBeat, RegisterWorker, RegisteredWorker, WorkerInfo}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps


/**
 * @Author RookieX
 * @Date 2021/7/25 5:55 下午
 * @Description:
 */
class Master extends Actor{

  //  创建HashMap集合, 用来存放workerId和内容
  val idToWorker = new mutable.HashMap[String, WorkerInfo]()


  override def preStart(): Unit = {
    //启动一个内部定时器
    import context.dispatcher
    context.system.scheduler.schedule(0 millisecond, 30000 millisecond, self, CheckTimeOutWorker)
  }

  //receive中有偏函数, 用来实现模式匹配, 接收消息并处理消息
  override def receive: Receive = {
    case "Hi" => {
      println("Hi~~~")
    }
    case RegisterWorker(workerId, memory, cores) => {
      //接受到worker发送的消息后, 将对应的Worker信息保存起来
      val workerInfo = new WorkerInfo(workerId, memory, cores)
      //保存到HashMap中
      idToWorker(workerId) = workerInfo
      //返回注册成功的信息
      sender() ! RegisteredWorker
    }

      //Worker发送给Master的心跳消息, 通过定期更新最后一次的心跳时间
    case HeartBeat(workerId) => {
      if (idToWorker.contains(workerId)) {
        val workerInfo: WorkerInfo = idToWorker(workerId)
        workerInfo.lastHeartBeatTime = System.currentTimeMillis()
      }
    }

      //Master自己给自己发送内部消息
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

object Master {
  def main(args: Array[String]): Unit = {
    //配置信息, 主机名, 端口等
    val config: Config = ConfigFactory.parseString(
      """
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname = localhost
        |akka.remote.netty.tcp.port = 8888
        |""".stripMargin)
    //创建一个ActorSystem
    val masterActorSystem: ActorSystem = ActorSystem.apply("MasterActorSystem", config)
    //使用ActorSystem创建Actor
    masterActorSystem.actorOf(Props[Master], "MasterActor")
  }
}