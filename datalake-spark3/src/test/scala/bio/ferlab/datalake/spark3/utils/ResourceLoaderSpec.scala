package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.spark3.utils.ResourceLoader.loadResource
import org.scalatest.flatspec.AnyFlatSpec

class ResourceLoaderSpec extends AnyFlatSpec {

  it should "return resource file content" in {
    val content = loadResource("utils/template1.json")
    assert(content.nonEmpty)
    assert(content.get.nonEmpty)
    assert(content.get.contains("index_patterns"))
  }

  it should "throw exception if resource not found" in {
    val error = loadResource("foo")
    assert(error.isEmpty)
  }

}
