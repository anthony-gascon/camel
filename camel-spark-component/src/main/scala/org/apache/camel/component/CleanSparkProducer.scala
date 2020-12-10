package org.apache.camel.component

import org.apache.camel.Exchange
import org.apache.camel.support.DefaultProducer
import org.apache.spark.sql.SparkSession

class CleanSparkProducer (sparkEndpoint: SparkEndpoint) extends DefaultProducer(sparkEndpoint){

  val sparkSession:SparkSession = getEndpoint.getCamelContext.getRegistry.lookupByName("sparkSession").asInstanceOf[SparkSession]

  override def process(exchange: Exchange): Unit = {
    sparkSession.sqlContext.tableNames().foreach(table => {
      sparkSession.sqlContext.dropTempTable(table)
      println(s"Temp table: $table has been deleted!!!")
    })
  }
}
