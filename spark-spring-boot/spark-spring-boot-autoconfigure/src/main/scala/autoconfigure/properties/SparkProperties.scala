package org.spark.spring.boot.autoconfigure.properties;

import java.util.Properties

import javax.validation.constraints.NotNull
import org.springframework.boot.context.properties.ConfigurationProperties

import scala.beans.BeanProperty;

@ConfigurationProperties("spark")
class SparkProperties extends Serializable {

	@BeanProperty @NotNull var appname: String = _
	@BeanProperty @NotNull var master: String = _
	@BeanProperty @NotNull var props:Properties =_
	@BeanProperty @NotNull var streamingContext:String =_
	@BeanProperty @NotNull var streaming:Properties = _


}
