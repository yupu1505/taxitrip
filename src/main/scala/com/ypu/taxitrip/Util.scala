package com.ypu.taxitrip

import org.joda.time._

object Util {
    def gridify(fromNum: Double, toNum: Double, steps: Int): List[List[Double]] = {
        val stepSize = (toNum - fromNum) / steps
        (1 to steps).toList.map(stepIndex => List(
          fromNum + (stepIndex -1) * stepSize,
          (fromNum + stepSize) + (stepIndex -1) * stepSize
        ))
    }

    def gridifyDateTime(fromDt: DateTime, toDt: DateTime, steps: Int): List[List[DateTime]] = {
      val millisInBetween = toDt.getMillis() - fromDt.getMillis()
      val millisStepSize = (millisInBetween / steps).toLong
      (1 to steps).toList.map(stepIndex => List(
        fromDt.plus((stepIndex -1) * millisStepSize),
        fromDt.plus(millisStepSize).plus((stepIndex -1) * millisStepSize)
      ))
    }
}
