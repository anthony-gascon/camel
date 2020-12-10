package org.apache.camel.component

import org.apache.camel.Exchange
import org.apache.camel.component.Utils.{getFirstNN,getOption}
import org.apache.camel.support.DefaultProducer
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}


class ReadSparkProducer(sparkEndpoint: SparkEndpoint) extends DefaultProducer(sparkEndpoint){

  val sparkSession:SparkSession = getEndpoint.getCamelContext.getRegistry.lookupByName("sparkSession").asInstanceOf[SparkSession]

  override def process(exchange: Exchange): Unit = {

    val source:String = if (sparkEndpoint.url == null || sparkEndpoint.url.trim.isEmpty) exchange.getIn.getHeader(Exchange.FILE_PATH).asInstanceOf[String] else sparkEndpoint.url

    if(source == null || source.trim.isEmpty){
      throw new IllegalArgumentException("File path can not be empty")
    }

    var format:String = sparkEndpoint.format.toLowerCase

    if(!(Array("csv","parquet","jdbc","xlsx") contains format)){
      throw new IllegalArgumentException(s"illegal format($format). Supported formats: csv, parquet, jdbc, xlsx")
    }

    var options:Map[String,String]= Map[String,String]()

    format match {
      case "csv" =>

        options += ("header" -> String.valueOf(sparkEndpoint.header))
        options += ("charset" -> sparkEndpoint.charSet)
        options += ("delimiter" -> sparkEndpoint.delimiter)

      case "xlsx" =>
        format = "com.crealytics.spark.excel"
        val sheetName:String = getFirstNN(sparkEndpoint.sheetName,exchange.getIn.getHeader("sheetName").asInstanceOf[String])

        options += ("dataAddress" -> s"'${sheetName}'!A1")
        options += ("header" -> "true")
        options += ("treatEmptyValuesAsNulls" -> "true")
        options += ("addColorColumns" -> "false")
        options += ("inferSchema" -> "false")

      case "jdbc" =>

        options += ("url" -> source)
        options += ("user" -> sparkEndpoint.user)
        options += ("password" -> sparkEndpoint.password)
        options += getOption("dbTable",sparkEndpoint.dbTable,exchange)

        if(sparkEndpoint.driver != null && sparkEndpoint.driver.nonEmpty)
          options += ("driver" -> sparkEndpoint.driver)

      case _ => null
    }

    val reader:DataFrameReader = sparkSession.read.format(format).options(options)

    val df:DataFrame = {
      if(format.equalsIgnoreCase("jdbc")) reader.load()
      else reader.load(source)
    }

    if(sparkEndpoint.viewName != null && sparkEndpoint.viewName.trim.nonEmpty){
      df.createOrReplaceTempView(sparkEndpoint.viewName)
    }

    if(sparkEndpoint.preview != null){
      df.show(sparkEndpoint.preview, false)
    }

    exchange.getIn().setBody(df)

  }
}
