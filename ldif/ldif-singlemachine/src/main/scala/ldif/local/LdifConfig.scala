package ldif.local

import com.typesafe.config.Config

/**
 * Sample config, start with -Dconfig.file=/path/to/job.conf
 *
 * evaluation {
 *   active = true
 *   reference = "/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/align-reegle-ref.rdf"
 *   output = "/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/align-silk.rdf"
 *   results = "/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/align-results.rdf"
 *
 *   # one of: SemPRecEvaluation, PRecEvaluation, ExtPREvaluation, SymMeanEvaluation, WeightedPREvaluation, DiffEvaluation
 *   type = "SemPRecEvaluation"
 * }
 *
 * Currently only contains the configuration for the evaluation job.
 *
 * @param config
 */
class LdifConfig(config: Config) {

  object evaluation {
    val (active, reference, output, results, evaluationType) =
      if (config.hasPath("evaluation.active") && config.getBoolean("evaluation.active")) {
        (true,
          config.getString("evaluation.reference"),
          config.getString("evaluation.output"),
          config.getString("evaluation.results"),
          lookupEvaluationType(config.getString("evaluation.type")))
      } else {
        (false, "", "", "", NoEvaluation)
      }

    private def lookupEvaluationType(t: String): EvaluationType = t match {
      case "SemPRecEvaluation" => SemPRecEvaluation
      case "PRecEvaluation" => PRecEvaluation
      case "ExtPREvaluation" => ExtPREvaluation
      case "SymMeanEvaluation" => SymMeanEvaluation
      case "WeightedPREvaluation" => WeightedPREvaluation
      case "DiffEvaluation" => DiffEvaluation
      case _ => throw new Error(f"Invalid evaluation type: $t")
    }
  }

}
