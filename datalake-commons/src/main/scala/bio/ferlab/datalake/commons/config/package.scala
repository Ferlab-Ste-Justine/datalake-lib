package bio.ferlab.datalake.commons

import pureconfig.generic.semiauto.deriveEnumerationConvert
import pureconfig.generic.{EnumerationConfigReaderBuilder, EnumerationConfigWriterBuilder, ProductHint}
import pureconfig.{CamelCase, ConfigConvert, ConfigFieldMapping}
import shapeless.Lazy
package object config {
  implicit def hint[T <: Configuration]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  val fieldMapping: ConfigFieldMapping = ConfigFieldMapping(CamelCase, CamelCase)

  def enumConvert[T](implicit
                       readerBuilder: Lazy[EnumerationConfigReaderBuilder[T]],
                       writerBuilder: Lazy[EnumerationConfigWriterBuilder[T]]
  ): ConfigConvert[T] = deriveEnumerationConvert[T](fieldMapping)

}
