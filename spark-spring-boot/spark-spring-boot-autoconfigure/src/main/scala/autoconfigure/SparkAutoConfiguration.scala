package org.spark.spring.boot.autoconfigure

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.spark.spring.boot.autoconfigure.properties.SparkProperties
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.condition.{ConditionalOnBean, ConditionalOnClass, ConditionalOnMissingBean, ConditionalOnProperty}
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.{Bean, Configuration}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter


@Configuration
@ConditionalOnClass(value = Array(classOf[SparkConf],classOf[SparkSession]))
@EnableConfigurationProperties(Array(classOf[SparkProperties]))
class SparkAutoConfiguration {

	@Autowired(required=true)
	var sparkProperties:SparkProperties =_

	@Bean
	def sparkConf(): SparkConf ={
		var conf = new SparkConf()
			.setAppName(sparkProperties.appname)
			.setMaster(sparkProperties.master)

		sparkProperties.props.entrySet.asScala.foreach(entry =>{
			println(entry.getKey+":"+entry.getValue)
			conf.set(s"spark.${entry.getKey}",s"${entry.getValue}")
		})

		conf
	}

	@Bean
	def sparkSession():SparkSession ={
		SparkSession.builder()
			.appName(sparkProperties.appname)
  		.config(sparkConf())
			.getOrCreate()
	}


	def sparkContext():SparkContext ={
		sparkSession().sqlContext.sparkContext
	}

/*
	@Bean
	@ConditionalOnProperty(
		value = Array("spark.streamingContext"),
		havingValue = "yes",
		matchIfMissing = true
	)
	def streamingContext():StreamingContext ={
		val duration:Long = sparkProperties.streaming.get("duration").toString.toLong
		println(s"\n\n\n****************** duracion: ${duration} ****************")
		new StreamingContext(sparkConf(),Seconds(duration))
	}
*/

}
