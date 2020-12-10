package org.apache.camel.component

import java.util

import org.apache.camel.Endpoint
import org.apache.camel.spi.annotations.Component
import org.apache.camel.support.DefaultComponent

@Component("spark")
class SparkComponent extends DefaultComponent{
  override def createEndpoint(uri: String, remaining: String, parameters: util.Map[String, AnyRef]): Endpoint = {
    val endpointType = remaining
    val answer:SparkEndpoint = new SparkEndpoint(uri,this,endpointType)
    setProperties(answer,parameters)
    answer
  }
}
