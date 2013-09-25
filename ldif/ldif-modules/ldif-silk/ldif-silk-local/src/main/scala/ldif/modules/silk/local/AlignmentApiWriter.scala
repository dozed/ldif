package ldif.modules.silk.local

import java.io.{FileWriter, File}
import de.fuberlin.wiwiss.silk.output.LinkWriter
import de.fuberlin.wiwiss.silk.entity.Link

/**
 * Writes links to a file in Alignment API format.
 */
class AlignmentApiWriter(file: File) extends LinkWriter {

  var writer = new FileWriter(file)

  val header = """<?xml version='1.0' encoding='utf-8' standalone='no'?>
                 |<rdf:RDF xmlns='http://knowledgeweb.semanticweb.org/heterogeneity/alignment#'
                 |         xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'
                 |         xmlns:xsd='http://www.w3.org/2001/XMLSchema#'
                 |         xmlns:align='http://knowledgeweb.semanticweb.org/heterogeneity/alignment#'>
                 |<Alignment>
                 |  <xml>yes</xml>
                 |  <level>0</level>
                 |  <type>?*</type>
                 |  <time>0</time>
                 |
                 |  <onto1>
                 |      <Ontology rdf:about="http://www.example.org/ontology1">
                 |          <location></location>
                 |          <formalism>
                 |              <Formalism name="INSTANCES" />
                 |          </formalism>
                 |        </Ontology>
                 |  </onto1>
                 |
                 |  <onto2>
                 |      <Ontology rdf:about="http://www.example.org/ontology2">
                 |          <location></location>
                 |          <formalism>
                 |              <Formalism name="INSTANCES"/>
                 |          </formalism>
                 |      </Ontology>
                 |  </onto2>
                 |
                 |""" .stripMargin

  val footer = """</Alignment>
                 |</rdf:RDF>
                 |""".stripMargin

  def cell(source: String, target: String, predicate: String, confidence: Double) =
              f"""  <map>
                 |    <Cell>
                 |        <entity1 rdf:resource="$source"/>
                 |        <entity2 rdf:resource="$target"/>
                 |      <relation>$predicate</relation>
                 |      <measure rdf:datatype="http://www.w3.org/2001/XMLSchema#float">$confidence</measure>
                 |    </Cell>
                 |  </map>
                 |
                 |""".stripMargin

  def write(link: Link, predicateUri: String) {
    val predicate = if (predicateUri == "http://www.w3.org/2002/07/owl#sameAs") "=" else predicateUri
    val confidence = link.confidence.getOrElse(0.0)

    writer.write(cell(link.source, link.target, predicate, confidence))
  }

  override def open() {
    writer.write(header)
  }

  override def close() {
    writer.write(footer)
    writer.close()
  }
}