package com.rookiex.demo02

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * @Author RookieX
 * @Date 2021/7/25 5:55 下午
 * @Description:
 */
class Worker extends Actor{

  val WORKER_ID: String = UUID.randomUUID().toString

  var masterRef: ActorSelection = _

  //生命周期方法, 在构造方法之后, receive方法执行之前一定执行一次
  override def preStart(): Unit = {
    //向Master建立连接
    masterRef = context.actorSelection("akka.tcp://MasterActorSystem@localhost:8888/user/MasterActor")
    //Worker发送给master的消息
    masterRef ! RegisterWorker(WORKER_ID, 10240, 16)

  }
  //生命周期方法, 在构造方法之后, receive方法之前一定执行一次

  override def receive: Receive = {
    case RegisterWorker => {
      //启动定时器, 定期向Master发送心跳报活
      import context.dispatcher
      //自己给自己发送定时消息
      context.system.scheduler.schedule(0 millisecond, 15000 millisecond, self, SendHeartBeat )
    }
    //自己给自己发送的定期消息
    case SendHeartBeat => {
      //进行逻辑判断
      //向Master发送心跳
      masterRef ! HeartBeat(WORKER_ID)
    }
  }
}

object Worker{
  def main(args: Array[String]): Unit = {
    //创建一个ActorSystem (单例)
    val config: Config = ConfigFactory.parseString(
      """
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname = localhost
        |akka.remote.netty.tcp.port = 9999
        |""".stripMargin)
    //创建ActorSystem
    val workerActorSystem: ActorSystem = ActorSystem.apply("WorkerActorSystem", config)
    //使用WorkerSystem创建Actor
    workerActorSystem.actorOf(Props[Worker], "WorkerActor")

  }
}
