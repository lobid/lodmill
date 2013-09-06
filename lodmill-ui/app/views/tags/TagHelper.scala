package views.tags

import play.api.libs.json.JsValue

object TagHelper {
  def getLabelValue(objectId: String, language: String, node: JsValue): Option[String] = {
    val label = "http://www.w3.org/2004/02/skos/core#prefLabel"
    if ((node \\ objectId).isEmpty) return None
    val id = ((node \\ objectId).head \ "@id").as[String]
    val res = for (
      graphObject <- (node \ "@graph").as[List[JsValue]];
      if ((graphObject \ "@id").asOpt[String].getOrElse("") == id);
      labelObject <- (graphObject \ label).asOpt[List[JsValue]].getOrElse(Nil);
      if ((labelObject \ "@language").asOpt[String].getOrElse("") == language)
    ) yield (labelObject \ "@value").asOpt[String].getOrElse("")
    Some(res.mkString(","))
  }
}