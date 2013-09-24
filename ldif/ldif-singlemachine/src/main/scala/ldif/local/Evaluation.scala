package ldif.local

import java.io.{FileInputStream, PrintWriter, File}
import fr.inrialpes.exmo.align.impl.eval.SemPRecEvaluator
import java.util.Properties
import org.semanticweb.owl.align.Alignment
import fr.inrialpes.exmo.align.parser.AlignmentParser

class Evaluation(alignmentRef: File, alignmentOut: File) {


  def eval() {
    val aparser: AlignmentParser = new AlignmentParser(0)
    val a1 = aparser.parse(new FileInputStream(alignmentRef))
    val a2 = aparser.parse(new FileInputStream(alignmentOut))

    val e1 = new SemPRecEvaluator(a1, a2)
    e1.eval(System.getProperties)

    val writer: PrintWriter = new PrintWriter(System.out)
    e1.write(writer)
    writer.flush
    writer.close
  }

}
