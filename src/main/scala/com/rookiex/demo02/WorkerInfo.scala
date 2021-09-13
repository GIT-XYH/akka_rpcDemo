package com.rookiex.demo02

/**
 * @Author RookieX
 * @Date 2021/7/26 9:56 下午
 * @Description:
 */
case class WorkerInfo(workerId: String, memory: Int, cores: Int) {
  var lastHeartBeatTime: Long = _
}