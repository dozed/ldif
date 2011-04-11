package ldif.util

import xml.Node

/**
 * Holds namespace prefixes.
 */
class Prefixes(private val prefixMap : Map[String, String])
{
  override def toString = "Prefixes(" + prefixMap.toString + ")"

  /**
   * Serializes all prefixes as XML.
   */
  def toXML =
  {
    <Prefixes>
    {
      for((key, value) <- prefixMap) yield
      {
        <Prefix id={key} namespace={value} />
      }
    }
    </Prefixes>
  }

  /**
   * Serializes all prefixes as SPARQL.
   */
  def toSparql =
  {
    var sparql = ""
    for ((key, value) <- prefixMap)
       {
         sparql += "PREFIX "+key+": <"+value +"> "
       }
    sparql
  }
}

object Prefixes
{
  /** Empty prefixes */
  val empty = new Prefixes(Map.empty)

  implicit def fromMap(map : Map[String, String]) = new Prefixes(map)

  implicit def toMap(prefixes : Prefixes) = prefixes.prefixMap

  def apply(map : Map[String, String]) = new Prefixes(map)

  def fromXML(xml : Node) =
  {
    new Prefixes((xml \ "Prefix").map(n => (n \ "@id" text, n \ "@namespace" text)).toMap)
  }
}