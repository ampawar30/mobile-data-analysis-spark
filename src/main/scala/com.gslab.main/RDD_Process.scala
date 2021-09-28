package rddprocess
import java.io.FileWriter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{Dataset, SparkSession}
import javax.xml.catalog.CatalogFeatures.Feature
import utils.MobileData_Processing
/** Creates a Mobile_Data Contain Info. of Mobiles
  *
  *  @param model_name the name mobile model
  *  @param manufature the manufactures of model
  *  @param price the price of mobile
  *  @param date_first_avaliable the date on which avaialable for sell
  *  @param ram the RAM of mobile
  *  @param special_feature the features of mobile
  *  @param avg_rating  the avgrage rating by customer
  */
case class Mobile_Data(model_name:String,
                       manufature:String,
                       date_first_avaliable:String,
                       price:Double,
                       ram:Int,cpu:String,display_size:Double,
                       front_camera:Int,rear_camera:Int,battery:Int,
                       special_feature:String,unit_sold:Int,
                       avg_rating:Double)
{
}

object RDD_Process {


  val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("GSLab_Spark_RDD_Assignment")
  val sc: SparkContext = new SparkContext(conf)

  val fsc= sc.textFile("src/resources/data1.csv")
  val mobRdd: RDD[Mobile_Data] = fsc.map(MobileData_Processing.parse)

  /**
    * Calculate Mobiles phones which are avaiable before six months from now.
    *
    * @param mobiledatardd contains RDD of Mobile Data.
    * filter those mobiles which are avialable from 6 months.
    * @return modellist return the mobiles data
    */
  def modelPhonesMonthWise(mobiledatardd: RDD[Mobile_Data]): Array[Mobile_Data] = {
    val modelslist= mobiledatardd.filter(modellist=>MobileData_Processing.noOfDays(modellist.date_first_avaliable) <= 6).collect()
    modelslist
  }

  /**
    * Give Mobile phone containing FingerPrint Sensor.
    *
    * @param mobiledatardd contains RDD of Mobile Data.
    * filter those mobiles which contains fingerPrint Sensor.
    * @return sensormobiles return the mobiles data
    */
  def fingerPrintSensorMobiles(mobiledatardd:RDD[Mobile_Data]):Array[Mobile_Data]=
    {
      val sensormobiles=mobiledatardd.filter(sensormobiles=>containFingerPrintSensor(sensormobiles.special_feature)==true).collect()
      sensormobiles
    }
  /**
    * Checking for contain element.
    *
    * @param givenfeature contains RDD of Mobile Data.
    * Check that list contains feature.
    * @return True is contain given feature else false
    */
  def containFingerPrintSensor(givenfeature: String): Boolean =
  {
    val e=givenfeature.split(",")
    val r=e.toList
    val p=r.map(_.contains("Fingerprint Sensor"))
    p.contains(true)
  }
  /**
    * Find Mobiles phones which are less than 20,000.
    *
    * @param mobiledatardd contains RDD of Mobile Data.
    * filter those mobiles which contain required features.
    * @return modellist return the mobiles data
    */
  def findLess20KMobiles(mobiledatardd:RDD[Mobile_Data]):Array[Mobile_Data]=
  {
    val modellist=mobiledatardd.filter(modellist=>modellist.ram==4&&modellist.rear_camera==16&&modellist.front_camera==5&&modellist.price<=20000.0).collect()
    modellist
  }
  /**
    * Compaire Two Mobiles.
    *
    * @param mobiledatardd contains RDD of Mobile Data.
    * do nothing still.
    * @return nothing
    */
  def compaireTwoPhones(mobiledatardd: RDD[Mobile_Data]): Unit =
  {

    val phone1=mobiledatardd.take(0)
    val phone2=mobiledatardd.take(1)
  }
  /**
    * Convert Given RDD to CSV and Save those Files.
    *
    * @param mobiledatardd contains RDD of Mobile Data.
    * save the csv files by converting arrayofdata to RDDofData.
    * @return nothing
    */
  def rddToCsv(mobiledatardd:Array[Mobile_Data],path:String): Unit =
  {
    val tordd=sc.parallelize(mobiledatardd)
    tordd.saveAsTextFile(path)
    //tordd.saveAsObjectFile(path)
  }

  def main(args: Array[String]) {
    mobRdd

     val month=modelPhonesMonthWise(mobRdd)
    rddToCsv(month,"src/resources/monthwise")
    val finger=fingerPrintSensorMobiles(mobRdd)
    rddToCsv(finger,"src/resources/isfingerprint")
    val list=findLess20KMobiles(mobRdd)
    rddToCsv(list,"src/resources/less20kmobiles")
   val comp=compaireTwoPhones(mobRdd)


    sc.stop()
  }


}