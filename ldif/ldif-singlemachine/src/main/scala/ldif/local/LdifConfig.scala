package ldif.local

import com.typesafe.config.Config

/**
 * Sample config, start with -Dconfig.file=/path/to/job.conf
 *
 * evaluation {
 *   active = true
 *   reference = "/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/align-reegle-ref.rdf"
 *   output = "/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/align-silk.rdf"
 * }
 *
 * Currently only contains the configuration for the evaluation job.
 *
 * @param config
 */
class LdifConfig(config: Config) {

  object evaluation {
    val (active, reference, output) = if (config.hasPath("evaluation.active") && config.getBoolean("evaluation.active")) {
      (true, config.getString("evaluation.reference"), config.getString("evaluation.output"))
    } else {
      (false, "", "")
    }
  }

}
