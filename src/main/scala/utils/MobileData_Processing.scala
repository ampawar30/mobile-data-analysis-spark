package utils

import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MONTHS

import rddprocess.Mobile_Data

object MobileData_Processing {
  /**
    * parse each line which is tab separated.
    *
    * @param line contains line of Mobile Data.
    * parse the line and assign data to case class mobile data.
    * @return nothing
    */
  def parse(line: String): Mobile_Data = {
    val nline = line.split("\t")
    Mobile_Data(nline(0), nline(1), nline(2), nline(3).toDouble, nline(4).toInt, nline(5), nline(6).toDouble, nline(7).toInt, nline(8).toInt, nline(9).toInt, nline(10), nline(11).toInt, nline(12).toDouble)

  }
  /**
    * Calculate no. of months from given date.
    *
    * @param date1 contains date of first avialable.
    * calculate difference.
    */
  def noOfDays(date1: String): Int =
  {
    val date2 = "03/10/2019"
    val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    val ym1 = YearMonth.parse(date1, formatter)
    val ym2 = YearMonth.parse(date2, formatter)
    val months = ym1.until(ym2, MONTHS)
    months.toInt
  }
}
