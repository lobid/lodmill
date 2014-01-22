package tests
import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import play.api.libs.json.JsValue
import play.api.libs.json.Json._

class ScalaTests extends AssertionsForJUnit {

  @Test def accessNested() {
    val json: JsValue = toJson(
      Map[String, JsValue](
        "@graph" -> toJson(List[Map[String, JsValue]](
          Map(
            "postal-code" -> toJson("4056"),
            "locality" -> toJson("Basle")),
          Map(
            "name" -> toJson("Uni Basel"),
            "identifier" -> toJson("SzBaU"),
            "http://purl.org/lobid/lv#contactqr" -> toJson(
              Map("@id" -> "http://lobid.org/media/DE-605_contactqr.png")))))))

    assert((json \\ "postal-code").head.asOpt[String].get === "4056")
    assert((json \\ "locality").head.asOpt[String].get === "Basle")
    assert((json \\ "name").head.asOpt[String].get === "Uni Basel")
    assert((json \\ "identifier").head.asOpt[String].get === "SzBaU")
    assert(((json \\ "http://purl.org/lobid/lv#contactqr").head \ "@id").
      asOpt[String].get === "http://lobid.org/media/DE-605_contactqr.png")
  }

  @Test def accessLabel() {
    import views.tags.TagHelper._
    val json: JsValue = toJson(
      Map[String, JsValue](
        "@graph" -> toJson(List[Map[String, JsValue]](
          Map("http://purl.org/lobid/lv#fundertype" -> toJson(
            Map("@id" -> toJson("http://purl.org/lobid/fundertype#n02")))),
          Map(
            "@id" -> toJson("http://purl.org/lobid/fundertype#n02"),
            "http://www.w3.org/2004/02/skos/core#prefLabel" -> toJson(List(
              Map(
                "@value" -> toJson("Federal State"),
                "@language" -> toJson("en")),
              Map(
                "@value" -> toJson("Land"),
                "@language" -> toJson("de"))))),
          Map("http://purl.org/lobid/lv#stocksize" -> toJson(
            Map("@id" -> toJson("http://purl.org/lobid/stocksize#n11")))),
          Map(
            "@id" -> toJson("http://purl.org/lobid/stocksize#n11"),
            "http://www.w3.org/2004/02/skos/core#prefLabel" -> toJson(List(
              Map(
                "@value" -> toJson("Einrichtung ohne Bestand"),
                "@language" -> toJson("de")),
              Map(
                "@value" -> toJson("Institution without a collection"),
                "@language" -> toJson("en")))))))))

    assert(getLabelValue("http://purl.org/lobid/lv#fundertype", "de", json)
      === Some("Land"))
    assert(getLabelValue("http://purl.org/lobid/lv#stocksize", "de", json)
      === Some("Einrichtung ohne Bestand"))
    assert(getLabelValue("http://purl.org/lobid/lv#fundertype", "en", json)
      === Some("Federal State"))
    assert(getLabelValue("http://purl.org/lobid/lv#stocksize", "en", json)
      === Some("Institution without a collection"))
  }

  @Test def accessSpecificTypeInGraph() {
    import views.tags.TagHelper._
    val json: JsValue = toJson(
      Map[String, JsValue](
        "http://xmlns.com/foaf/0.1/primaryTopic" -> toJson(Map("@id" -> "http://lobid.org/resource/HT002189125")),
        "@graph" -> toJson(List[Map[String, JsValue]](
          Map(
            "@id" -> toJson("http://d-nb.info/gnd/118580604"),
            "@type" -> toJson("http://d-nb.info/standards/elementset/gnd#DifferentiatedPerson")),
          Map(
            "@id" -> toJson("http://lobid.org/resource/HT002189125"),
            "@type" -> toJson(List(
              "http://purl.org/vocab/frbr/core#Manifestation",
              "http://purl.org/dc/terms/BibliographicResource",
              "http://purl.org/ontology/bibo/Book")))))))

    assert(getPrimaryTopicType(json)
      === Some(toJson(List(
        "http://purl.org/vocab/frbr/core#Manifestation",
        "http://purl.org/dc/terms/BibliographicResource",
        "http://purl.org/ontology/bibo/Book"))))
  }

  @Test def accessSpecificTypeNoGraph() {
    import views.tags.TagHelper._
    val json: JsValue = toJson(
      Map[String, JsValue](
        "@id" -> toJson("http://lobid.org/resource/HT002189125"),
        "@type" -> toJson(List(
          "http://purl.org/vocab/frbr/core#Manifestation",
          "http://purl.org/dc/terms/BibliographicResource",
          "http://purl.org/ontology/bibo/Book"))))

    assert(getPrimaryTopicType(json)
      === Some(toJson(List(
        "http://purl.org/vocab/frbr/core#Manifestation",
        "http://purl.org/dc/terms/BibliographicResource",
        "http://purl.org/ontology/bibo/Book"))))
  }
}