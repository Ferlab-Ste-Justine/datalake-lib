package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.spark3.utils.ResourceLoader.loadResource
import org.scalatest.flatspec.AnyFlatSpec

class ResourceLoaderSpec extends AnyFlatSpec {

  it should "return resource file content" in {
    val content = loadResource("utils/template1.json")
    assert(content.nonEmpty)
    assert(content.contains("index_patterns"))
  }

  it should "throw exception if resource not found" in {
    val ex = intercept[RuntimeException] {
      loadResource("foo")
    }
    assert(ex.getMessage.contains("Failed to load resource: foo"))
  }

}
