package tests
import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import play.api.libs.json.JsValue
import play.api.libs.json.Json

class ScalaTests extends AssertionsForJUnit {

  var json: JsValue = Json.toJson(Map(
    "@graph" -> List(
      Map(
        "postal-code" -> "4056",
        "locality" -> "Basle"),
      Map(
        "name" -> "Uni Basel",
        "identifier" -> "SzBaU"))))

  @Test def accessNested() {
    assert((json \\ "postal-code").head.asOpt[String].get === "4056")
    assert((json \\ "locality").head.asOpt[String].get === "Basle")
    assert((json \\ "name").head.asOpt[String].get === "Uni Basel")
    assert((json \\ "identifier").head.asOpt[String].get === "SzBaU")
  }
}