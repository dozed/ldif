package ldif.local

import java.io.File
import fr.inrialpes.exmo.align.impl.eval.SemPRecEvaluator
import java.util.Properties

class Evaluation(refAlignment: File, foundAlignment: File) {

  val evaluator = new SemPRecEvaluator(???, ???)

  def eval() {
    evaluator.eval(new Properties())
  }

}
