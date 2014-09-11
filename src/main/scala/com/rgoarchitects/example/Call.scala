package com.rgoarchitects.example

import java.util.Calendar


/**
 * User: arnonrgo
 * Date: 9/5/14
 * Time: 9:52 PM
 */
object  Call  {
  def apply(line : String) : Call= {
    // no validations since the sample data we'd use is synthetic
    val values=line.split(",")
    val userId : Long = values(0).toLong
    val callTime : Long =values(1).toLong
    val duration : Long =values(2).toLong
    val pricing : Double = values(3).toDouble
    val balance : Double = values(4).toDouble
    val zipCode : Int = values(5).toInt
    val city : String = values(6)
    val state : String = values(7)
    val toUserId : Long = values(8).toLong
    val discount : Double = values(9).toDouble
    val toNumber : Long = values(10).toLong
    val eventType : String = values(11)
    val addCharge : Double = values(12).toDouble
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(callTime)
    val year : Int = cal.get(Calendar.YEAR)
    val month : Int = cal.get(Calendar.MONTH)+1
    val day : Int = cal.get(Calendar.DAY_OF_MONTH)
    val hour : Int = cal.get(Calendar.HOUR_OF_DAY)
    Call(userId,callTime,duration,pricing,balance,zipCode,city,state,toUserId,discount,toNumber,eventType,addCharge,year,month,day,hour)
  }
}

case class Call(userId : Long,
                callTime : Long,
                duration : Long,
                pricing : Double,
                balance : Double,
                zipCode : Int,
                city : String,
                state: String,
                toUserId : Long,
                discount : Double,
                toNumber: Long,
                eventType :String,
                addCharge : Double,
                year : Int,
                month: Int,
                day : Int,
                hour : Int)  {
  def getHourly = f"$year%04d-$month%02d-$day%02d-$hour%02d-$userId%012d"

  def getWeekly = f"$year%04d-$month%02d-$day%02d-$userId%012d"
}

