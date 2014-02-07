/* Copyright 2013, 2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */
package views.tags

import play.api.libs.json.JsValue
import play.api.libs.json.JsObject
import play.api.libs.json.JsArray

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
    ((node \ "http://xmlns.com/foaf/0.1/primaryTopic").asOpt[JsValue] match {
      case None => Some(node)
      case Some(id) =>
        (node \ "@graph").as[List[JsValue]].find((v: JsValue) => (v \ "@id") == id \ "@id")
    }).map(_ \ "@type")
  }
  def valueFor(doc: JsValue, id: String, keys: Seq[String]): String = {
    for (elem <- (doc \ "@graph").as[Seq[JsValue]]; key <- keys) {
      if ((elem \ "@id").as[String] == id
        && elem.as[Map[String, JsValue]].contains(key))
        return (elem \ key).as[String]
    }
    id
  }
}