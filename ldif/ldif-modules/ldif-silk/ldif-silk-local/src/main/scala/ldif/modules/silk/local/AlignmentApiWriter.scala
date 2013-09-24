package ldif.modules.silk.local

import java.io.{FileWriter, File}
import de.fuberlin.wiwiss.silk.output.LinkWriter
import de.fuberlin.wiwiss.silk.plugins.writer.AlignmentFormatter
import de.fuberlin.wiwiss.silk.entity.Link

/**
 * Writes links to a file in Alignment API format.
 */
class AlignmentApiWriter(file: File) extends LinkWriter {

  var writer = new FileWriter(file)
  val formatter = new AlignmentFormatter

  def write(link: Link, predicateUri: String) {
    val r = formatter.format(link, predicateUri)
    writer.write(r + "\n")
  }


  override def open() {

  }

  override def close() {
    writer.close()
  }
}