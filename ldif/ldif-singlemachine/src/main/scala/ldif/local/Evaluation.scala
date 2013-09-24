package ldif.local

import de.fuberlin.wiwiss.silk.output.LinkWriter
import de.fuberlin.wiwiss.silk.plugins.writer.AlignmentFormatter
import de.fuberlin.wiwiss.silk.entity.Link

class AlignmentApiWriter extends LinkWriter {

  val formatter = new AlignmentFormatter

  def write(link: Link, predicateUri: String) {
    val r = formatter.format(link, predicateUri)
    println(r)
  }

}


class Evaluation {

  val alignmentWriter = new AlignmentApiWriter


}
