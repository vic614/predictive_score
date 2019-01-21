import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types
//import org.apache.spark.sql.Row

object FeatureProcessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("victor")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val path = "file:///home/victor/PycharmProjects/predictivescore/databq"
    val featureProcessing = new FeatureProcessing(spark, path)
    var data=featureProcessing.dataCombination()
    data.cache()
    data = featureProcessing.EventDummy(data)
    data = featureProcessing.SimpleUserID(data)
    data = featureProcessing.addHisNumID(data)
    data = featureProcessing.DropUserStartWithConversion(data)
    println(data.count())
    data.show(5)
  }
}

class FeatureProcessing(private val spark: SparkSession, val path: String) {
  // For implicit conversions from RDDs to DataFrames

  val rawUserData = spark.read.option("inferSchema", true).option("header", true).csv(path + "/DT_data_internelproject_2FModel1_RandomSampling_07012017_01312018_trial2.csv")
  val rawCompaignData = spark.read.option("inferSchema", true).option("header", true).csv(path + "/DT_data_internelproject_2FMode1_Campaign_20170701_20180131.csv")
  val rawSiteData = spark.read.option("inferSchema", true).option("header", true).csv(path + "/DT_data_internelproject_2FMode1_Site_20170701_20180131.csv")
  val rawPlacementData = spark.read.option("inferSchema", true).option("header", true).csv(path + "/DT_data_internelproject_2FMode1_Placement_20170701_20180131.csv")


  def dataCombination(): DataFrame = {
    val rawCompaignData1 = rawCompaignData.dropDuplicates("Campaign_ID").select("Campaign_ID", "Campaign")
    var data = rawUserData.join(rawCompaignData1, Seq("Campaign_ID"))
    val rawSiteData1 = rawSiteData.dropDuplicates("Site_ID_DCM").select("Site_ID_DCM", "Site_DCM")
    data = data.join(rawSiteData1, Seq("Site_ID_DCM"))

    val rawPlacementData1 = rawPlacementData.dropDuplicates("Placement_ID").select("Placement_ID", "Placement")
    data = data.join(rawPlacementData1, Seq("Placement_ID"))
    data
  }
  def EventDummy(dataFrame: DataFrame):DataFrame = {
    val convert=functions.udf((arg:String)=>{if (arg=="CONVERSION") 1 else 0})
    val data=dataFrame.withColumn("Event_Dummy",col=convert(functions.col("Event_Type")))
    data
  }
  def SimpleUserID(dataFrame: DataFrame):DataFrame = {
    var dfUserIndex = rawUserData.select("User_ID").distinct()
    dfUserIndex = dfUserIndex.withColumn("ID",functions.monotonically_increasing_id()+1)
    val dfUserCount = rawUserData.groupBy("User_ID").count().withColumnRenamed("count","numID").select("User_ID","numID")
    var data = dataFrame.join(dfUserIndex,Seq("User_ID"))
    data = data.join(dfUserCount,Seq("User_ID"))
    // filter out users have only one record
    data = data.filter(functions.col("numID")>1)
    data
  }
  def addHisNumID(dataFrame: DataFrame): DataFrame = {
    val w = Window.partitionBy("ID").orderBy(functions.asc("Event_Time"))
    val functionHI: Integer => Integer = 1 - _
    val inverter=functions.udf(functionHI,types.IntegerType)
    var data = dataFrame.withColumn("HisNumID",functions.count("ID").over(w))
    data = data.withColumn("Event_Dummy_Reverse",inverter(functions.col("Event_Dummy")))
    data
  }
  def DropUserStartWithConversion(dataFrame: DataFrame): DataFrame = {
    println(dataFrame.count(),"!!!!!!!")
    val dfDrop = dataFrame.filter((functions.col("HisNumID")===1) && (functions.col("Event_Dummy")===1)).select("ID").withColumn("keep",functions.lit(1))
    var data = dataFrame.join(dfDrop,Seq("ID"))
    data = data.filter(data.col("keep")!==1).drop("keep")
    data
  }
}