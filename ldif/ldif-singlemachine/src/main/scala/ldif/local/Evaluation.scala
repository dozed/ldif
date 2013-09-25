package ldif.local

import java.io.{FileInputStream, PrintWriter, File}
import fr.inrialpes.exmo.align.impl.eval._
import fr.inrialpes.exmo.align.parser.AlignmentParser
import org.semanticweb.owl.align.{Alignment, Evaluator}

sealed trait EvaluationType
object PRecEvaluation extends EvaluationType
object SemPRecEvaluation extends EvaluationType
object ExtPREvaluation extends EvaluationType
object SymMeanEvaluation extends EvaluationType
object WeightedPREvaluation extends EvaluationType
object DiffEvaluation extends EvaluationType
object NoEvaluation extends EvaluationType

object Evaluation {

  def eval(evalType: EvaluationType, alignmentRef: File, alignmentOut: File, alignmentResults: File) {
    val aparser = new AlignmentParser(0)
    val a1 = aparser.parse(new FileInputStream(alignmentRef))
    val a2 = aparser.parse(new FileInputStream(alignmentOut))

    val e1 = evaluator(evalType, a1, a2)
    e1.eval(System.getProperties)

    val writer = new PrintWriter(alignmentResults)
    e1.write(writer)
    writer.flush
    writer.close
  }

  def evaluator(evalType: EvaluationType, a1: Alignment, a2: Alignment): Evaluator = evalType match {
    case PRecEvaluation => new PRecEvaluator(a1, a2)
    case SemPRecEvaluation => new SemPRecEvaluator(a1, a2)
    case ExtPREvaluation => new ExtPREvaluator(a1, a2)
    case SymMeanEvaluation => new SymMeanEvaluator(a1, a2)
    case WeightedPREvaluation => new WeightedPREvaluator(a1, a2)
    case DiffEvaluation => new DiffEvaluator(a1, a2)
    case NoEvaluation => throw new Error("No evaluation is scheduled.")
  }

}
