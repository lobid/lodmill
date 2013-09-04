package tests
import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import play.api.libs.json.JsValue
import play.api.libs.json.Json

class ScalaTests extends AssertionsForJUnit {

  var json: JsValue = Json.toJson(
    Map[String, JsValue](
      "@graph" -> Json.toJson(List[Map[String, JsValue]](
        Map(
          "postal-code" -> Json.toJson("4056"),
          "locality" -> Json.toJson("Basle")),
        Map(
          "name" -> Json.toJson("Uni Basel"),
          "identifier" -> Json.toJson("SzBaU"),
          "http://purl.org/lobid/lv#contactqr" -> Json.toJson(
            Map("@id" -> "http://lobid.org/media/DE-605_contactqr.png")))))))

  @Test def accessNested() {
    assert((json \\ "postal-code").head.asOpt[String].get === "4056")
    assert((json \\ "locality").head.asOpt[String].get === "Basle")
    assert((json \\ "name").head.asOpt[String].get === "Uni Basel")
    assert((json \\ "identifier").head.asOpt[String].get === "SzBaU")
    assert(((json \\ "http://purl.org/lobid/lv#contactqr").head \ "@id").
      asOpt[String].get === "http://lobid.org/media/DE-605_contactqr.png")
  }
}