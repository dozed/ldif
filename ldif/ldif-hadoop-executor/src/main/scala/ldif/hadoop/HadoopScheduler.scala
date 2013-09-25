/* 
 * LDIF
 *
 * Copyright 2011-2013 Freie Universität Berlin, MediaEvent Services GmbH & Co. KG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldif.hadoop

import ldif.config.SchedulerConfig
import org.slf4j.LoggerFactory
import java.util.{Date, Calendar}
import java.io._
import java.util.concurrent.ConcurrentHashMap
import org.apache.commons.io.FileUtils
import ldif.local.scheduler.ImportJob
import ldif.datasources.dump.QuadParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import ldif.config.IntegrationConfig
import ldif.util.{Consts, StopWatch, FatalErrorListener}

/*
 * A singlemachine scheduler/importer that uploads dumps to HDFS and runs Hadoop integration jobs
 */

class HadoopScheduler (val config : SchedulerConfig, debug : Boolean = false) {
  private val log = LoggerFactory.getLogger(getClass.getName)

  val conf = new Configuration
  val hdfs = FileSystem.get(conf)

  // load jobs
  private val importJobs = loadImportJobs(config.importJobsFiles)
  private val integrationJob : HadoopIntegrationJob = new HadoopIntegrationJob(config.integrationConfig, debug)

  // init status variables
  private var startup = true
  private var runningIntegrationJobs = false
  private val runningImportJobs = initRunningJobsMap

  /* Evaluate updates/integration */
  def evaluateJobs() {
    synchronized {
      evaluateIntegrationJob(true)
      evaluateImportJobs
      startup = false
    }
  }

  def evaluateIntegrationJob(inBackground : Boolean) {
    if (integrationJob != null && checkUpdate(integrationJob)) {
      runningIntegrationJobs = true
      if (inBackground)
        runInBackground{runIntegration()}
      else runIntegration()

    }
  }

  def evaluateImportJobs {
    for (job <- importJobs.filter(checkUpdate(_))) {
      // check if this job is already running
      if (!runningImportJobs.get(job.id)) {
        runImport(job)
      }
    }
  }

  /* Evaluate if all jobs should run only once (at startup) or never */
  def runOnce : Boolean = {
    if (config.properties.getProperty("oneTimeExecution", "false") == "true") {
      log.info("One time execution enabled")
      return true
    }
    for (job <- importJobs)
      if (job.refreshSchedule != "onStartup" && job.refreshSchedule != "never")
        return false
    if (integrationJob != null && (integrationJob.config.runSchedule != "onStartup" && integrationJob.config.runSchedule != "never"))
      return false
    true
  }

  def allJobsCompleted = !runningIntegrationJobs && !runningImportJobs.containsValue(true)

  /* Execute the integration job */
  def runIntegration() {
      integrationJob.runIntegration
      log.info("Integration Job completed")
      runningIntegrationJobs = false
  }

  /* Execute an import job, update local source */
  def runImport(job : ImportJob) {
    runInBackground
    {
      runningImportJobs.replace(job.id, true)
      val stopWatch = new StopWatch
      log.info("Import Job "+ job.id +" started ("+job.getType+" / "+job.refreshSchedule+")")
      stopWatch.getTimeSpanInSeconds()

      val tmpDumpFile = getTmpDumpFile(job)
      val tmpProvenanceFile = getTmpProvenanceFile(job)

      // create local dump
      val success = {
        try {
          job.load(new FileOutputStream(tmpDumpFile), getNumberOfQuads(job))
        } catch {
          case e: Exception => log.warn("There has been an unexpected error while processing job " + job +". Cause: " + e.getMessage)
            log.debug(e.getStackTraceString)
            false
        }
      }

      if(success) {
        // create provenance metadata
        val provenanceGraph = config.properties.getProperty("provenanceGraphURI", Consts.DEFAULT_PROVENANCE_GRAPH)
        job.generateProvenanceInfo(new OutputStreamWriter(new FileOutputStream(tmpProvenanceFile)), provenanceGraph)

        log.info("Job " + job.id + " loaded in "+ stopWatch.getTimeSpanInSeconds + "s")

        var loop = true
        val changeFreqHours = Consts.changeFreqToHours.get(job.refreshSchedule).get
        var maxWaitingTime = changeFreqHours.toLong * 60 * 60
        val waitingInterval = 1

        while(loop) {
          // if the integration job is not running
          if (!runningIntegrationJobs) {

            // replace old HDFS dumps with the new ones
            hdfs.copyFromLocalFile(new Path(tmpDumpFile.getCanonicalPath), getDumpFile(job))
            hdfs.copyFromLocalFile(new Path(tmpProvenanceFile.getCanonicalPath), getProvenanceFile(job))
            log.info("Updated HDFS dumps for job "+ job.id)

            runningImportJobs.replace(job.id, false)
            loop = false
          }
          else {
            // wait for integration job to be completed, but not more than the job refreshSchedule
            maxWaitingTime -= waitingInterval
            if (maxWaitingTime < 0) {
              // waited too long, dump is outdates
              log.info("The dump loaded for job "+ job.id +" expired, a new import is required. \n"+ tmpDumpFile.getCanonicalPath)
              if (!debug) {
                FileUtils.deleteQuietly(tmpDumpFile)
                FileUtils.deleteQuietly(tmpProvenanceFile)
              }
              runningImportJobs.replace(job.id, false)
              loop = false
            }
            Thread.sleep(waitingInterval * 1000)
          }
        }
      }
      else {
        if (!debug) {
          FileUtils.deleteQuietly(tmpDumpFile)
          FileUtils.deleteQuietly(tmpProvenanceFile)
        }
        runningImportJobs.replace(job.id, false)
        log.warn("Job " + job.id + " has not been imported - see log for details")
      }
    }
  }

  private def initRunningJobsMap = {
    val map = new ConcurrentHashMap[String, Boolean](importJobs.size)
    for(job <- importJobs)
      map.putIfAbsent(job.id, false)
    map
  }

  /* Check if an update is required for the integration job */
  def checkUpdate(job : HadoopIntegrationJob = integrationJob) : Boolean = {
    checkUpdate(job.config.runSchedule, job.getLastUpdate)
  }

  /* Check if an update is required for the import job */
  def checkUpdate(job : ImportJob) : Boolean = {
    checkUpdate(job.refreshSchedule, getLastUpdate(job))
  }


  private def checkUpdate(schedule : String, lastUpdate : Calendar) : Boolean = {
    if (schedule == "onStartup") {
      if (startup)
        true
      else
        false
    }
    else if (schedule == "never") {
      false
    }
    else {
      val changeFreqHours = Consts.changeFreqToHours.get(schedule)
      // Get last update run
      val nextUpdate = Calendar.getInstance

      // Figure out if update is required
      if (changeFreqHours != None) {
        if (lastUpdate == null) {
          true
        } else {
          nextUpdate.setTimeInMillis(lastUpdate.getTimeInMillis)
          nextUpdate.add(Calendar.HOUR, changeFreqHours.get)
          Calendar.getInstance.after(nextUpdate)
        }
      }
      else
        false
    }
  }

  /* Retrieve last update from provenance info */
  private def getLastUpdate(job : ImportJob) : Calendar = {
    val provenanceFile = getProvenanceFile(job)
    if (hdfs.exists(provenanceFile)) {
      val provenanceFileLocal = getTmpProvenanceFile(job)
      // copy to a tmp local file and evaluate
      hdfs.copyToLocalFile(provenanceFile, new Path(provenanceFileLocal.getCanonicalPath))
      val lines = scala.io.Source.fromFile(provenanceFileLocal).getLines
      val parser = new QuadParser
      // loop and stop as the first lastUpdate quad is found
      for (quad <- lines.toTraversable.map(parser.parseLine(_))){
        if (quad.predicate.equals(Consts.lastUpdateProp))  {
          val lastUpdateStr = quad.value.value
          if (lastUpdateStr.length != 25)  {
            log.warn("Job "+job.id+" - wrong datetime format for last update metadata")
            return null
          }
          else {
            val sb = new StringBuilder(lastUpdateStr).deleteCharAt(22)
            val lastUpdateDate = Consts.xsdDateTimeFormat.parse(sb.toString)
            return dateToCalendar(lastUpdateDate)
          }
        }
      }
      log.warn("Job "+job.id+" - provenance file does not contain last update metadata")
      null
    }
    else {
      //log.warn("Job "+job.id+" - provenance file not found at "+provenanceFile.getCanonicalPath)
      null
    }
  }

  /* Retrieve the number of imported quads from provenance info */
  private def getNumberOfQuads(job : ImportJob) : Option[Double] = {
    val provenanceFile = getProvenanceFile(job)
    if (hdfs.exists(provenanceFile)) {
      val provenanceFileLocal = getTmpProvenanceFile(job)
      hdfs.copyToLocalFile(provenanceFile, new Path(provenanceFileLocal.getCanonicalPath))
      val lines = scala.io.Source.fromFile(provenanceFileLocal).getLines
      val parser = new QuadParser
      // loop and stop as the first numberOfQuads property is found
      for (quad <- lines.toTraversable.map(parser.parseLine(_))){
        if (quad.predicate.equals(Consts.numberOfQuadsProp))
          return Some(quad.value.value.toDouble)
      }
      log.warn("Job "+job.id+" - provenance file does not contain last update metadata")
      None
    }
    else {
      //log.warn("Job "+job.id+" - provenance file not found at "+provenanceFile.getCanonicalPath)
      None
    }
  }

  private def loadImportJobs(files : Traversable[File]) : Traversable[ImportJob] =
    files.map(ImportJob.load(_))

  // Build local files for the import job
  private def getDumpFile(job : ImportJob) = new Path(config.dumpLocationDir, job.id +".nq")
  private def getTmpDumpFile(job : ImportJob) = File.createTempFile(job.id+"_"+Consts.simpleDateFormat.format(new Date()),".nq")
  private def getProvenanceFile(job : ImportJob) = new Path(config.dumpLocationDir, job.id +".provenance.nq")
  private def getTmpProvenanceFile(job : ImportJob) = File.createTempFile(job.id+"_provenance_"+Consts.simpleDateFormat.format(new Date()),".nq")


  // Move source File to dest File
  private def moveFile(source : File, dest : File) {
    // delete dest (if exists)
    FileUtils.deleteQuietly(dest)
    if (!debug) {
      try {
        FileUtils.moveFile(source, dest)
      } catch {
        case ex:IOException => log.error("IO error occurs moving a file: \n" +source.getCanonicalPath+ " -> " +dest.getCanonicalPath)
      }
    }
    else {
      // if debugMode, keep tmp file
      try {
        FileUtils.copyFile(source, dest)
      } catch {
        case ex: IOException =>  {
          log.error("IO error occurs copying a file: \n"+source.getCanonicalPath+" to "+dest.getCanonicalPath)
        }
      }
    }
  }

  /**
   * Evaluates an expression in the background.
   */
  private def runInBackground(function : => Unit) {
    val thread = new Thread {
      private val listener: FatalErrorListener = FatalErrorListener

      override def run() {
        try {
          function
        } catch {
          case e: Exception => listener.reportError(e)
        }
      }
    }
    thread.start()
  }

  /* Convert Date to a Calendar   */
  private def dateToCalendar(date : Date) : Calendar = {
      val cal = Calendar.getInstance
      cal.setTime(date)
      cal
  }

  def getImportJobs = importJobs
  def getIntegrationJob = integrationJob
}

