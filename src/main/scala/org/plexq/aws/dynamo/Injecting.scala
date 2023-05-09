package org.plexq.aws.dynamo

import scala.reflect.ClassTag
import com.google.inject.{Guice, Injector}

trait Injecting {
  var injector: Injector

  def instanceOf[T](implicit ct: ClassTag[T]): T = instanceOf(ct.runtimeClass.asInstanceOf[Class[T]])

  def instanceOf[T](clazz: Class[T]): T = injector.getInstance(clazz)

  def inject[T: ClassTag]: T = instanceOf
}
