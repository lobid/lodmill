/* Copyright 2013, 2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */
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
  def getPrimaryTopicType(node: JsValue): Option[JsValue] = {
    val primaryTopic = (node \ "http://xmlns.com/foaf/0.1/primaryTopic")
      .as[Map[String, String]].get("@id")
    val res = for (
      graphObject <- (node \ "@graph").as[List[JsValue]];
      if ((graphObject \ "@id").asOpt[String].getOrElse("") == primaryTopic.getOrElse(""))
    ) yield graphObject \ "@type"
    Some(res(0))
  }
}