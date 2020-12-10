package org.apache.camel.component

import org.apache.camel.Exchange
import org.apache.camel.component.Utils.getFirstNN
import org.apache.camel.support.DefaultProducer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}

class WriteSparkProducer (sparkEndpoint: SparkEndpoint) extends DefaultProducer(sparkEndpoint){

  val sparkSession:SparkSession = getEndpoint.getCamelContext.getRegistry.lookupByName("sparkSession").asInstanceOf[SparkSession]

  override def process(exchange: Exchange): Unit = {

    var target:String = sparkEndpoint.url

    if(Utils.isNullOrEmpty(target)){
      throw new IllegalArgumentException("File path url can not be empty")
    }

    var format:String = sparkEndpoint.format.toLowerCase

    if(!(Array("csv","parquet","xlsx","jdbc") contains format)){
      throw new IllegalArgumentException(s"illegal format($format). Supported formats: csv, parquet, jdbc, xlsx")
    }

    val dataFrame:DataFrame = exchange.getIn.getBody.asInstanceOf[DataFrame]

    if(dataFrame == null){
      throw new IllegalArgumentException("No dataframe to write")
    }

    val saveMode:String = {
      if(sparkEndpoint.saveMode.equalsIgnoreCase("NewRecords"))
        SaveMode.Append.toString
      else sparkEndpoint.saveMode
    }

    exchange.getIn().setHeader("RECORD_COUNT", dataFrame.count())

    var options:Map[String,String]= Map[String,String]()

    val finalDf:DataFrame = format match {
      case "csv" => {

        target = Utils.getPath(target,exchange.getIn.getHeader(Exchange.FILE_NAME).asInstanceOf[String])

        options += ("header" -> String.valueOf(sparkEndpoint.header))
        options += ("charset" -> sparkEndpoint.charSet)
        options += ("delimiter" -> sparkEndpoint.delimiter)
        dataFrame
      }
      case "xlsx" => {
        format = "com.crealytics.spark.excel"
        val sheetName:String = getFirstNN(sparkEndpoint.sheetName,exchange.getIn.getHeader("sheetName").asInstanceOf[String])

        options += ("dataAddress" -> s"'${sheetName}'!A1")
        options += ("header" -> "true")
        dataFrame
      }
      case "parquet" => {
        if(sparkEndpoint.saveMode.equalsIgnoreCase("NewRecords")){
          Utils.getFilteredDataframe(dataFrame,target,sparkSession,format,options)
        }else{
          dataFrame
        }
      }
      case "jdbc" => {

        options += ("url" -> target)
        options += ("user" -> sparkEndpoint.user)
        options += ("password" -> sparkEndpoint.password)
        options += Utils.getOption("dbTable",sparkEndpoint.dbTable,exchange)

        if(sparkEndpoint.driver != null && sparkEndpoint.driver.nonEmpty)
          options += ("driver" -> sparkEndpoint.driver)

        if(sparkEndpoint.saveMode.equalsIgnoreCase("NewRecords")){
          Utils.getFilteredDataframe(dataFrame,target,sparkSession,format,options)
        }else{
          dataFrame
        }
      }
    }

    exchange.getIn().setHeader("PROCESSED_RECORD_COUNT",finalDf.count())

    val writer:DataFrameWriter[Row] = {if (sparkEndpoint.repartition>0) finalDf.repartition(sparkEndpoint.repartition) else finalDf}
                                        .write
                                        .format(format)
                                        .options(options)
                                        .mode(saveMode)

    if(format.equalsIgnoreCase("jdbc"))
      writer.save()
    else
      writer.save(target)

  }

  private def getJdbcDataframe(dataFrame:DataFrame, target:String): DataFrame ={
    if(sparkEndpoint.saveMode.equalsIgnoreCase("NewRecords")){
      try{
        val currentRdd = sparkSession.read.format(sparkEndpoint.format)
          .option("url",target)
          .option("dbTable",sparkEndpoint.dbTable)
          .option("user", sparkEndpoint.user)
          .option("password", sparkEndpoint.password)
          .load().rdd

        val schema:StructType = dataFrame.schema
        sparkSession.createDataFrame(dataFrame.rdd.subtract(currentRdd),schema)

      }catch {
        case e:Exception =>dataFrame
      }
    }else{
      dataFrame
    }
  }

}
