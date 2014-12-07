package com.rgoarchitects.example

import com.rgoarchitects.example.Aggregates.{PhoneEntry, Aggregate}
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
 * User: arnonrgo
 * Date: 9/7/14
 * Time: 2:47 PM
 */

object CalcAggregations   {

  def apply( calls : Iterable[(String,String,Call)]) = {

    val entryBuilder=PhoneEntry.newBuilder()
    val aggregateBuilder = Aggregate.newBuilder()
    val phoneBook = new mutable.HashMap[Long,PhoneEntry]()
    val balances = new ListBuffer[Double]()
    val hoursBreakdown = new Array[Int](24)

    def buildPhoneEntry(call: Call): PhoneEntry = {
      if (!phoneBook.contains(call.toNumber)) {
        entryBuilder.clear()
        entryBuilder.setCountCalls(1)
        entryBuilder.setToNumber(call.toNumber)
        entryBuilder.setToUserId(call.toUserId)
        entryBuilder.setZipCode(call.zipCode)
        entryBuilder.addPrices(call.pricing)
        entryBuilder.build()
      } else {
        val e = phoneBook(call.toNumber)
        entryBuilder.clear()
        entryBuilder.mergeFrom(phoneBook(call.toNumber))
        entryBuilder.addPrices(call.pricing)
        entryBuilder.setCountCalls(phoneBook(call.toNumber).getCountCalls + 1)
        entryBuilder.build()
      }
    }

    val (_,_,sample) = calls.head

    //use old fashioned aggregations to minimize iterations on calls Iterable
    // I guess the idiomatic way would have been to .cache the group and then
    // calculate each aggregation as a separate .map (or flatmaps) but it seems
    // to me that would be more wasteful (something to benchmark ...)
    var sumDuration = 0L
    var sumPricing = 0.0
    var minDuration = Long.MaxValue
    var maxDuration = Long.MinValue
    var hadAnyDiscount = false
    var sumDiscount = 0.0
    var sumAddCharge = 0.0
    var minBalance = Double.MaxValue
    var maxBalance = Double.MinValue

    for (callRecord<-calls) {
      val (_,_,call) = callRecord
      sumDuration += call.duration
      minDuration = if (call.duration<minDuration) call.duration else minDuration
      maxDuration = if (call.duration>maxDuration) call.duration else maxDuration

      sumPricing +=  call.pricing

      sumDiscount += call.discount
      hadAnyDiscount = if (call.discount>0) true else hadAnyDiscount

      sumAddCharge += call.addCharge

      minBalance = if (call.balance>minBalance) call.balance else minBalance
      maxBalance = if (call.balance>maxBalance) call.balance else maxBalance
      balances += call.balance

      phoneBook(call.toNumber) =  buildPhoneEntry(call)
      hoursBreakdown(call.hour) += 1
    }

    val count =balances.size // we can use balances as it is a
    val avgDuration = sumDuration.toDouble / count.toDouble
    val avgPricing = sumPricing / count
    val avgDiscount = sumDiscount / count
    val avgAddCharge = sumAddCharge / count
    val sortedBalances = balances.toList.sorted    // second pass just on balanced -> to get median
    val mid : Int= sortedBalances.size / 2
    val medianBalance = if (calls.size % 2 ==0) (sortedBalances(mid)+sortedBalances(mid+1)) /2 else sortedBalances(mid)

    aggregateBuilder.clear()
    aggregateBuilder.setYear(sample.year)
    aggregateBuilder.setMonth(sample.month)
    aggregateBuilder.setDay(sample.day)
    aggregateBuilder.setHour(sample.hour)
    aggregateBuilder.setUserId(sample.userId)
    aggregateBuilder.setCallCount(count)
    aggregateBuilder.setSumDuration(sumDuration)
    aggregateBuilder.setMaxDuration(maxDuration)
    aggregateBuilder.setMinDuration(minDuration)
    aggregateBuilder.setSumPricing(sumPricing)
    aggregateBuilder.setAvgPricing(avgPricing)
    aggregateBuilder.setAvgDiscount(avgDiscount)
    aggregateBuilder.setHadAnyDiscount(hadAnyDiscount)
    aggregateBuilder.setAvgAddCharge(avgAddCharge)
    aggregateBuilder.setMinBalance(minBalance)
    aggregateBuilder.setMaxBalance(maxBalance)
    aggregateBuilder.setMedianBalance(medianBalance)
    aggregateBuilder.addAllPhoneBook(phoneBook.values.asJava)
    hoursBreakdown.foreach(aggregateBuilder.addHoursBreakdown)

    aggregateBuilder.build()

  }
}


