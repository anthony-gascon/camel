package org.apache.camel.component

import org.apache.camel.spi.{Metadata, UriEndpoint, UriParam, UriPath}
import org.apache.camel.support.DefaultEndpoint
import org.apache.camel.{Category, Consumer, Processor, Producer}


@UriEndpoint(firstVersion = "2.17.0", scheme = "spark", title = "Spark", syntax = "spark:endpointType", producerOnly = true, category = Array(Category.BIGDATA, Category.IOT))
class SparkEndpoint(endpointUri: String, component: SparkComponent, @UriPath @Metadata(required = true) var endpointType: String) extends DefaultEndpoint(endpointUri, component) {

  /*Spark Read - Write */
  @UriParam
  var url: String = _

  /*format: {csv, parquet, jdbc, xlsx}*/
  @UriParam(defaultValue = "csv")
  var format: String = "csv"

  /*Spark Read */
  @UriParam
  var viewName: String = _

  /*Spark Write */
  /*saveMode: {Append, Overwrite, NewRecords, ErrorIfExists, Ignore}*/
  @UriParam(defaultValue = "Overwrite")
  var saveMode: String = "Overwrite"


  /*jdbc*/
  @UriParam
  var dbTable: String = _
  @UriParam
  var user: String = _
  @UriParam
  var password: String = _
  @UriParam
  var driver: String = _

  /*csv*/
  @UriParam(defaultValue = "true")
  var header: Boolean = true
  @UriParam(defaultValue = "UTF-8")
  var charSet: String = "UTF-8"
  @UriParam(defaultValue = ",")
  var delimiter: String = ","
  @UriParam
  var repartition: Int = _

  /*sql*/
  @UriParam
  var query: String = _

  @UriParam
  var sheetName:String = _

  @UriParam
  var preview: Integer = _

  override def doInit(): Unit = {
    super.doInit()
  }


  override def createProducer(): Producer = {

    val producer:Producer = {
      if (endpointType.equalsIgnoreCase("read"))
        new ReadSparkProducer(this)
      else if (endpointType.equalsIgnoreCase("write"))
        new WriteSparkProducer(this)
      else if (endpointType.equalsIgnoreCase("sql"))
        new SqlSparkProducer(this)
      else if (endpointType.equalsIgnoreCase("clean"))
        new CleanSparkProducer(this)
      else null
    }
    producer
  }

  override def createConsumer(processor: Processor): Consumer = {
    throw new UnsupportedOperationException("Spark component supports producer endpoints only.")
  }

  override def getComponent: SparkComponent = super.getComponent.asInstanceOf[SparkComponent]


  def setUrl(url:String): Unit ={this.url = url}
  def setFormat(format:String): Unit ={this.format = format}
  def setViewName(viewName:String): Unit ={this.viewName = viewName}
  def setSaveMode(saveMode:String): Unit={this.saveMode = saveMode}

  def setHeader(header:Boolean): Unit ={this.header = header}
  def setCharSet(charSet:String): Unit ={this.charSet = charSet}
  def setDelimiter(delimiter:String): Unit ={this.delimiter = delimiter}
  def setRepartition(repartition:Int): Unit = {this.repartition=repartition}

  def setDbTable(dbTable:String):Unit ={this.dbTable = dbTable}
  def setUser(user:String):Unit ={this.user = user}
  def setPassword(password:String):Unit ={this.password = password}
  def setDriver(driver:String):Unit ={this.driver = driver}

  def setQuery(query:String):Unit={this.query=query}
  def setSheetName(sheetName:String):Unit={this.sheetName= sheetName}

  def setPreview(preview:Integer):Unit={this.preview = preview}
}
