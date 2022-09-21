package bio.ferlab.datalake.commons

import pureconfig.{CamelCase, ConfigFieldMapping}
import pureconfig.generic.ProductHint

package object config {
  implicit def hint[T <: Configuration]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
}
