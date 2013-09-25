package ldif.local

import java.io.{FileInputStream, PrintWriter, File}
import fr.inrialpes.exmo.align.impl.eval.SemPRecEvaluator
import fr.inrialpes.exmo.align.parser.AlignmentParser

object Evaluation {

  def eval(alignmentRef: File, alignmentOut: File, alignmentResults: File) {
    val aparser = new AlignmentParser(0)
    val a1 = aparser.parse(new FileInputStream(alignmentRef))
    val a2 = aparser.parse(new FileInputStream(alignmentOut))

    val e1 = new SemPRecEvaluator(a1, a2)
    e1.eval(System.getProperties)

    val writer = new PrintWriter(alignmentResults)
    e1.write(writer)
    writer.flush
    writer.close
  }

}
