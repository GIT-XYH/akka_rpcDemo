package com.rookiex.demo01

/**
 * @Author RookieX
 * @Date 2021/7/25 2:45 下午
 * @Description:
 */
class WorkerInfo (val workerId:String, val memory: Int, val cores: Int){
  var lastHeartBeatTime: Long = _
}
