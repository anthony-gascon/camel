package org.apache.camel.component

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.camel.Exchange
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.types.StructType

object Utils {
  val fs: FileSystem = FileSystem.get(new Configuration)

  def getFirstNN(fs: String*): String = fs.iterator.find(v =>{v.ne(null) && v.trim.nonEmpty}).getOrElse(null)

  def getFilteredDataframe(dataFrame:DataFrame, target:String, sparkSession: SparkSession,format:String , options:Map[String,String]): DataFrame ={

      try{

        val reader:DataFrameReader = sparkSession.read
          .format(format)
          .options(options)

        val currentRdd = {if(target == null) reader.load() else reader.load(target)}.rdd

        val schema:StructType = dataFrame.schema
        sparkSession.createDataFrame(dataFrame.rdd.subtract(currentRdd),schema)

      }catch {
        case e: Exception => dataFrame
      }
  }

  def getOption(name:String,value:String,exchange: Exchange): (String,String) ={
    name -> getFirstNN(value,exchange.getIn.getHeader(name).asInstanceOf[String])
  }

  def isNullOrEmpty(value:String):Boolean={
    value == null || value.trim.isEmpty
  }

  def getPath(path:String, fileName:String):String ={

    val tempName= getFirstNN(fileName,s"csvFile_${new SimpleDateFormat("yyyyMMddhhmmss").format(Calendar.getInstance().getTime())}.csv")

    val tempPath:String = {if(fs.isDirectory(new Path(path))){
      path
    }else{
      fs.getFileStatus(new Path(path)).getPath.getParent.toString
    }}

    s"${tempPath}/${tempName}"
  }

  private def refactorCSV(inputCSV: String,csvExtension:String): Unit = {
    val fileStatusListIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(inputCSV), true)
    while (fileStatusListIterator.hasNext) {
      val fileStatus: LocatedFileStatus = fileStatusListIterator.next
      val pathFile = fileStatus.getPath
      if (pathFile.toString.endsWith(".csv")) {
        fs.moveFromLocalFile(pathFile, new Path(s"$inputCSV$csvExtension"))
      }
    }

    fs.delete(new Path(inputCSV), true)
  }
}
