package org.apache.camel.component

import org.apache.camel.Exchange
import org.apache.camel.support.DefaultProducer
import org.apache.spark.sql.{DataFrame, SparkSession}

class SqlSparkProducer (sparkEndpoint: SparkEndpoint) extends DefaultProducer(sparkEndpoint){

  val sparkSession:SparkSession = getEndpoint.getCamelContext.getRegistry.lookupByName("sparkSession").asInstanceOf[SparkSession]

  override def process(exchange: Exchange): Unit = {
    if(sparkEndpoint.query==null || sparkEndpoint.query.trim.isEmpty){
      throw new IllegalArgumentException("Query param can not be empty")
    }

    val df:DataFrame = sparkSession.sql(sparkEndpoint.query)

    if(sparkEndpoint.viewName != null && sparkEndpoint.viewName.trim.nonEmpty){
      df.createOrReplaceTempView(sparkEndpoint.viewName)
    }

    if(sparkEndpoint.preview != null){
      df.show(sparkEndpoint.preview, false)
    }

    exchange.getIn().setBody(df)
  }
}