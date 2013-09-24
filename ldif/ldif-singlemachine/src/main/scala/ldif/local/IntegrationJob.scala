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

package ldif.local

import datasources.dump.{QuadFileLoader, DumpLoader}
import rest.MonitorServer
import runtime._
import impl._
import ldif.modules.r2r.local.R2RLocalExecutor
import ldif.modules.r2r.{R2RModule, R2RConfig}
import ldif.modules.silk.SilkModule
import ldif.entity.EntityDescription
import ldif.{EntityBuilderModule, EntityBuilderConfig}
import java.util.{Calendar, Properties}
import java.io._
import java.math.BigInteger
import de.fuberlin.wiwiss.r2r.{JenaModelSource, EnumeratingURIGenerator, FileOrURISource, Repository}
import org.slf4j.LoggerFactory
import ldif.util._
import ldif.modules.sieve.fusion.{FusionModule, EmptyFusionConfig, FusionConfig}
import ldif.modules.sieve.quality.{QualityConfig, QualityModule, EmptyQualityConfig}
import ldif.config._
import ldif.modules.silk.local.SilkLocalExecutor
import ldif.modules.sieve.local.{SieveLocalQualityExecutor, SieveLocalFusionExecutor}
import ldif.modules.sieve.SieveConfig
import util.{ImportedDumpsUtils, StringPool}
import scala.collection.mutable.{Set => MSet}
import ldif.runtime.{Quad, QuadReader, QuadWriter}

case class IntegrationJob (config : IntegrationConfig, debugMode : Boolean = false) {

  private val log = LoggerFactory.getLogger(getClass.getName)
  private var reporter = new IntegrationJobStatusMonitor

  // Object to store all kinds of configuration data
  private var configParameters: ConfigParameters = null
  private val stopWatch = new StopWatch

  private var lastUpdate : Calendar = null

  // Config properties (from Java property file)
  private val outputProvenance = config.properties.getProperty("outputProvenance", "true").equals("true")
  private val useExternalSameAsLinks = config.properties.getProperty("useExternalSameAsLinks", "true").toLowerCase=="true"
  private val rewriteURIs = config.properties.getProperty("rewriteURIs", "true").toLowerCase=="true"

  // Which phases should be skipped
  val skipR2R = config.properties.getProperty("mappings.skip", "false")=="true"
  val skipSilk = config.properties.getProperty("linkSpecifications.skip", "false")=="true"
  val skipSieve = config.properties.getProperty("sieve.skip", "false")=="true"

  // The number of quads contained in the input dumps - use for progress estimations
  var dumpsQuads : Double = getSourcesQuads

  private def updateReaderAfterR2RPhase(r2rReader: Option[scala.Seq[QuadReader]]): Option[scala.Seq[QuadReader]] = {
    if (isIntialQuadReader(r2rReader.get)) {
      val inputQuadsFile = TemporaryFileCreator.createTemporaryFile("ldif-filtered-input", ".bin")
      inputQuadsFile.deleteOnExit()
      val fileQuadWriter = new FileQuadWriter(inputQuadsFile)
      copyQuads(r2rReader.get.head, fileQuadWriter)
      fileQuadWriter.finish()
      Some(Seq(new FileQuadReader(inputQuadsFile, numberOfQuads = fileQuadWriter.size)))
    } else r2rReader
  }

  private def initNewRun() {
    //reporter = new IntegrationJobStatusMonitor
    //JobMonitor.addPublisher(reporter)
    IntegrationJobMonitor.value = reporter
    val sourceNumber = config.sources.size

    log.info("Integration Job started")
    log.info("- Input < " + sourceNumber + " source(s) found " + config.sources.mkString(" "))
    log.info("- Output > " + config.outputs.toString)
    log.info("- Properties ")
    for (key <- config.properties.keySet.toArray)
      log.info("  - " + key + " : " + config.properties.getProperty(key.toString))

    stopWatch.getTimeSpanInSeconds
  }

  private def validateConfiguration() {
    // Validate configuration
    val fail = ConfigValidator.validateConfiguration(config)
    if (fail) {
      log.warn("Validation phase failed")
      throw new RuntimeException("Validation failed for integration job. Config: " + config)
    } else {
      log.info("Validation phase succeeded in " + stopWatch.getTimeSpanInSeconds + "s")
    }
  }

  private def setupSourceQuadReader(terms: MSet[String] = MSet.empty[String]): Seq[QuadReader] = {

    val dumpQuadReader = new DumpQuadReader(new MultiQuadReader(loadDumps(config.sources): _*), configParameters)
    dumpQuadReader.reporter.setInputQuads(dumpsQuads)
    if (terms.size == 0 || skipR2R)
      Seq(dumpQuadReader)
    else {
      val copyQuadsReader = new CopyQuadsQuadReader(dumpQuadReader, terms.toSet, configParameters.passOnToSieveWriter)
      Seq(copyQuadsReader)
    }

  }

  def runIntegration {
    reporter.setStartTime
    if (config.sources == null || config.sources.size == 0)
      log.info("Integration Job skipped - No data source files found")

    else
      synchronized {
        initNewRun()

        validateConfiguration()

        setupConfigParameters()
        val terms = if(!skipSieve)
          SieveConfig.getUsedProperties(config.sieveSpecDir)
        else MSet.empty[String]

        // Load source data sets and wrap the quad readers in a filter quad reader
        val quadReaders = setupSourceQuadReader(terms)

        // Execute mapping phase
        var r2rReader: Option[Seq[QuadReader]] = executeMappingPhase(config, quadReaders, skipR2R)

        r2rReader = updateReaderAfterR2RPhase(r2rReader)

        // Execute linking phase
        val sameAsLinkReader = new QuadQueue
        val otherLinkReader = new QuadQueue
        executeLinkingPhase(config, r2rReader.get, sameAsLinkReader, otherLinkReader, skipSilk)

        // Setup sameAs reader and otherQuads reader
        configParameters.otherQuadsWriter.finish()
        val otherQuadsReader = new FileQuadReader(configParameters.otherQuadsWriter)
        configParameters.provenanceQuadsWriter.finish()
        val provenanceQuadReader = new FileQuadReader(configParameters.provenanceQuadsWriter)
        configParameters.sameAsWriter.finish()
        val sameAsReader = {
          if(useExternalSameAsLinks)
            new FileQuadReader(configParameters.sameAsWriter)
          else
            new QuadQueue
        }
        configParameters.passOnToSieveWriter.finish()
        val passOnToSieveReader = new FileQuadReader(configParameters.passOnToSieveWriter)

        val clonedR2rReader = setupQuadReader(r2rReader.get)

        val allQuads = if(skipR2R)
          new MultiQuadReader(clonedR2rReader: _*) // These are the source data sets
        else
          new MultiQuadReader(clonedR2rReader.head, otherQuadsReader, passOnToSieveReader, otherLinkReader)

        val  allSameAsLinks = if(skipSilk)
          sameAsReader
        else
          new MultiQuadReader(sameAsLinkReader, sameAsReader)

//        // Execute evaluation
//        val calculatedAlignment = new File("align-calc.rdf")
//        val alignmentWriter = new AlignmentApiWriter
//        for (link <- allSameAsLinks.cloneReader) {
//          link
//          alignmentWriter.write()
//        }
//
//        val refAlignment = new File("align-ref.rdf")
//        val eval = new Evaluation(allSameAsLinks.cloneReader, refAlignment)
//        eval.writeAlignment(calculatedAlignment)


        /***  Execute URI Clustering/Translation ***/
        var integratedReader: QuadReader = null
        if(rewriteURIs)
          integratedReader = executeURITranslation(allQuads, allSameAsLinks, config.properties)
        else integratedReader = new MultiQuadReader(allQuads, allSameAsLinks) // TODO: Really add allSameAsLinks here (already)?

        if (!skipSieve) {
          /***  Execute sieve (quality and fusion) ***/
          val sieveInput = Map("all"->integratedReader, "provenance"->provenanceQuadReader)//val sieveInput = Seq(integratedReader)
          integratedReader = executeSieve(config, sieveInput)
        }

        lastUpdate = Calendar.getInstance

        writeOutput(config, integratedReader)
        reporter.setFinishTime
      }
  }

  private def splitByLinkType(quadReader: QuadReader, sameAsLinkWriter: QuadWriter, otherLinkWriter: QuadWriter) {
    while (quadReader.hasNext) {
      val quad = quadReader.read
      quad match {
        case Quad(e1, Consts.SAMEAS_URI, e2, _) => sameAsLinkWriter.write(quad)
        case Quad(e1, _, e2, _) => otherLinkWriter.write(quad)
      }
    }
    sameAsLinkWriter.finish()
    otherLinkWriter.finish()
  }

  private def copyQuads(reader: QuadReader, writer: QuadWriter) {
    for(quad <- reader)
      writer.write(quad)
  }

  private def isIntialQuadReader(quadReaders: Seq[QuadReader]): Boolean = {
    quadReaders.length==1 && quadReaders.head.isInstanceOf[DumpQuadReader]
  }

  private def setupQuadReader(_clonedR2rReader: Seq[QuadReader]): Seq[QuadReader] = {
    var clonedR2rReader: Seq[QuadReader] = _clonedR2rReader
    if (clonedR2rReader.length==1 && clonedR2rReader.head.isInstanceOf[FileQuadReader]) {
      clonedR2rReader.head.asInstanceOf[FileQuadReader].close()
      clonedR2rReader = Seq(new FileQuadReader(clonedR2rReader.head.asInstanceOf[FileQuadReader].inputFile))
    }
    clonedR2rReader
  }

  private def cloneQuadReaders(originalReaders: Seq[QuadReader]): Seq[QuadReader] = {
    originalReaders.map(qReader => qReader match {
      case cloneable: CloneableQuadReader => {
        cloneable.cloneReader
      }
      case _ => {
        log.error("Could not clone QuadReader. Results will not include triples from this reader.")
        new QuadQueue
      }
    })
  }

  // Setup config parameters
  def setupConfigParameters() {
    config.properties.getProperty("ldif.working.dir") match {
      case path: String => TemporaryFileCreator.setNewTempDir(new File(path))
      case _ =>
    }
    // Quads that are not used in the integration flow, but should still be output
    val otherQuadsFile = TemporaryFileCreator.createTemporaryFile("ldif-other-quads", ".bin")
    // Quads that contain external sameAs links
    val sameAsQuadsFile = TemporaryFileCreator.createTemporaryFile("ldif-sameas-quads", ".bin")
    // Quads that lie in the provenance graph
    val provenanceQuadsFile = TemporaryFileCreator.createTemporaryFile("ldif-provenance-quads", "bin")
    // Quads that are already in target vocabulary representation
    val noMapNeedQuadsFile = TemporaryFileCreator.createTemporaryFile("ldif-nomapneed-quads", "bin")

    otherQuadsFile.deleteOnExit()
    sameAsQuadsFile.deleteOnExit()
    provenanceQuadsFile.deleteOnExit()
    noMapNeedQuadsFile.deleteOnExit()

    val otherQuads: FileQuadWriter = new FileQuadWriter(otherQuadsFile)
    val sameAsQuads: FileQuadWriter = new FileQuadWriter(sameAsQuadsFile)
    val provenanceQuads: FileQuadWriter = new FileQuadWriter(provenanceQuadsFile)
    val noMapNeedQuads: FileQuadWriter = new FileQuadWriter(noMapNeedQuadsFile)

    configParameters = ConfigParameters(config.properties, otherQuads, sameAsQuads, provenanceQuads, noMapNeedQuads)

    // Setup LocalNode (to pool strings etc.)
    LocalNode.reconfigure(config.properties)

  }

  private def executeMappingPhase(config: IntegrationConfig, quadReaders: Seq[QuadReader], skip: Boolean): Option[Seq[QuadReader]] = {
    if(skip) {
      log.info("Skipping R2R phase.")
      Some(quadReaders) // Skip R2R phase
    }
    else {
      val r2rReader = mapQuads(config, quadReaders)
      log.info("Time needed to map data: " + stopWatch.getTimeSpanInSeconds + "s")

      // Outputs intermadiate results
      Some(Seq(writeCopy(config, r2rReader.get.head, DT)))
    }
  }

  private def executeLinkingPhase(config: IntegrationConfig,
                                  quadReader: Seq[QuadReader],
                                  sameAsLinkWriter : QuadWriter,
                                  otherLinkWriter : QuadWriter,
                                  skip: Boolean) {
    if(skip) {
      log.info("Skipping Silk phase.")
      quadReader // Skip Silk phase
    }
    else {
      var linkReader = generateLinks(config.linkSpecDir, quadReader)
      log.info("Time needed to link data: " + stopWatch.getTimeSpanInSeconds + "s")
      log.info("Number of links generated by silk: " + linkReader.size)
      splitByLinkType(linkReader, sameAsLinkWriter, otherLinkWriter)

      // Outputs intermediate results
      linkReader = writeCopy(config, linkReader, IR)
    }
  }

  private def executeSieve(config: IntegrationConfig, inputQuadsReaders : Map[String,QuadReader]) : QuadReader = {

    val outputQualityScores : Boolean = config.properties.getProperty("outputQualityScores", "false").equals("true")
    val qualityFromProvenanceOnly : Boolean = config.properties.getProperty("qualityFromProvenanceOnly", "false").equals("true")

    val qualityInput = if (qualityFromProvenanceOnly) Seq(inputQuadsReaders("provenance")) else inputQuadsReaders.values.toSeq
    val qualityModule = QualityModule.load(config.sieveSpecDir)
    val sieveQualityReader = qualityModule.config.qualityConfig match {
      case e: EmptyQualityConfig => {
        log.info("No Sieve Quality configuration found. No quality assessment will be performed.")
        new QuadQueue() // return empty queue
      }
      case c: QualityConfig => {
        executeQualityPhase(config, qualityInput, qualityModule)
      }
    }

    // Now the scores from the quality assessment live in sieveQualityReader, and we need to get it into the fusion stuff.
    // If these modules are run separately, QualityModule.qualityAssessmentProvider can read up the values, and build a queriable structure.
    // This is the loosely coupled solution.
    // However, we also provide a simple (tightly coupled) solution for the time being.
    // If the two modules are run together, then qualitymodule stores scores when they are computed
    val fusionInput : Seq[QuadReader] = cloneQuadReaders(inputQuadsReaders.values.toSeq)
    val fusionModule = FusionModule.load(config.sieveSpecDir, qualityModule)
    val sieveFusionReader = fusionModule.config.fusionConfig match {
      case e: EmptyFusionConfig => {
        log.info("No Sieve Fusion configuration found. No fusion will be performed.")
        if(outputProvenance)
          new MultiQuadReader(fusionInput:_*)
        else
          inputQuadsReaders("all")
      }
      case c: FusionConfig => {
        executeFusionPhase(config, fusionInput, fusionModule)
      }
    }

    if (outputQualityScores) {
      new MultiQuadReader(sieveFusionReader, sieveQualityReader) // return both quality and fused quads
    } else {
      sieveFusionReader // return only fused quads
    }
  }

  private def executeQualityPhase(config: IntegrationConfig, inputQuadsReader: Seq[QuadReader], qualityModule: QualityModule): QuadReader = {
    val sieveQualityReader = assessQuality(config.sieveSpecDir, inputQuadsReader, qualityModule)
    val sieveAggregatedQualityReader = aggregateQuality(config.sieveSpecDir, Seq(sieveQualityReader), qualityModule)
    val allQualityReader = new MultiQuadReader(sieveQualityReader,sieveAggregatedQualityReader)
    log.info("Time needed to assess data quality: " + stopWatch.getTimeSpanInSeconds + "s")
    log.info("Number of quality-assessment scores generated by sieve: " + allQualityReader.size)
    allQualityReader
  }

  private def executeFusionPhase(config: IntegrationConfig, inputQuadsReader: Seq[QuadReader], fusionModule: FusionModule): QuadReader = {
    val sieveFusionReader = fuseQuads(config.sieveSpecDir, inputQuadsReader, fusionModule)
    log.info("Time needed to fuse data: " + stopWatch.getTimeSpanInSeconds + "s")
    log.info("Number of quads output by sieve fusion phase: " + sieveFusionReader.size)
    sieveFusionReader
  }

  private def executeURITranslation(inputQuadReader: QuadReader, linkReader: QuadReader, configProperties: Properties): QuadReader = {
    val integratedReader = URITranslator.translateQuads(inputQuadReader, linkReader, configProperties)

    log.info("Time needed to translate URIs: " + stopWatch.getTimeSpanInSeconds + "s")
    integratedReader
  }

  /**
   * Loads the dump files. //TODO move to a DumpLoaderUtil object? This method is useful in other places too
   */
  private def loadDumps(sources : Traversable[String]) : Seq[QuadReader] = {
    var quadQueues = Seq.empty[QuadReader]
    for (source <-  sources) {
      val sourceFile = new File(source)
      if(sourceFile.isDirectory) {
        for (dump <- sourceFile.listFiles.filterNot(_.isHidden))  {
          quadQueues = loadDump(dump) +: quadQueues
        }
      }
      else
        quadQueues = loadDump(sourceFile) +: quadQueues
    }
    quadQueues
  }

  /**
   * Loads a dump file. //TODO move to a DumpLoaderUtil object? This method is useful in other places too
   */
  private def loadDump(dump : File) : QuadReader = {
    // Integration component expects input data to be represented as Named Graphs, other formats are skipped
    //   if (ContentTypes.getLangFromExtension(dump.getName)!=ContentTypes.langNQuad) {
    //     log.warn("Input source skipped, format not supported: " + dump.getCanonicalPath)
    //     new QuadQueue
    //   }  else
    val quadQueue = new BlockingQuadQueue(Consts.DEFAULT_QUAD_QUEUE_CAPACITY)
    val discardFaultyQuads = config.properties.getProperty("discardFaultyQuads", "false").toLowerCase=="true"
    val loadDumpsMT = config.properties.getProperty("loadDumpsMT", "false").toLowerCase=="true"
    runInBackground
    {
      val inputStream = DumpLoader.getFileStream(dump)
      val bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
      val quadParser = new QuadFileLoader(dump.getName, discardFaultyQuads)
      if (loadDumpsMT) {
        quadParser.readQuadsMT(bufferedReader, quadQueue)
      } else {
        quadParser.readQuads(bufferedReader, quadQueue)
      }
      quadQueue.finish
    }
    quadQueue
  }

  /**
   * Transforms the Quads
   */
  private def mapQuads(integrationConfig: IntegrationConfig, readers: Seq[QuadReader]) : Option[Seq[QuadReader]] = {
    val mappingSource = new FileOrURISource(integrationConfig.mappingDir)
    val uriGenerator = new EnumeratingURIGenerator("http://www4.wiwiss.fu-berlin.de/ldif/imported", BigInteger.ONE)
    val importedMappingModel = Repository.importMappingDataFromSource(mappingSource, uriGenerator)
    val repository = new Repository(new JenaModelSource(importedMappingModel))
    val r2rConfig = new R2RConfig(repository)
    val module = new R2RModule(r2rConfig)

    val entityDescriptions = for(task <- module.tasks) yield task.mapping.entityDescription
    val entityReaders = buildEntities(readers, entityDescriptions.toSeq, ConfigParameters(config.properties))
    StringPool.reset
    log.info("Time needed to load dump and build entities for mapping phase: " + stopWatch.getTimeSpanInSeconds + "s")

    val outputFile = TemporaryFileCreator.createTemporaryFile("ldif-mapped-quads", ".bin")
    outputFile.deleteOnExit
    val executor = new R2RLocalExecutor
    reporter.addPublisher(executor.reporter)
    reporter.setStatus("Data Translation")
    val writer = new FileQuadWriter(outputFile)

    executor.reporter.mappingsTotal = module.tasks.size

    //runInBackground
    for((r2rTask, reader) <- module.tasks.toList zip entityReaders)
      executor.execute(r2rTask, Seq(reader), writer)
    writer.finish
    executor.reporter.setFinishTime()

    Some(Seq(new FileQuadReader(writer.asInstanceOf[FileQuadWriter])))
  }

  /**
   * Generates links.
   */
  private def generateLinks(linkSpecDir : File, readers : Seq[QuadReader]) : QuadReader = {
    val silkModule = SilkModule.load(linkSpecDir)
    val inmemory = config.properties.getProperty("entityBuilderType", "in-memory")=="in-memory"
    val alignmentOutFile = Option(config.properties.getProperty("alignmentOutFile")).map(new File(_))
    val silkExecutor = if(inmemory)
      new SilkLocalExecutor(alignmentApiOutput = alignmentOutFile)
    else
      new SilkLocalExecutor(useFileInstanceCache = true, alignmentApiOutput = alignmentOutFile)
    reporter.addPublisher(silkExecutor.reporter)
    reporter.setStatus("Identity Resolution")

    val entityDescriptions = silkModule.tasks.toIndexedSeq.map(silkExecutor.input).flatMap{ case StaticEntityFormat(ed) => ed }
    val entityReaders = buildEntities(readers, entityDescriptions, ConfigParameters(config.properties))
    StringPool.reset
    log.info("Time needed to build entities for linking phase: " + stopWatch.getTimeSpanInSeconds + "s")

    val outputQueue = new QuadQueue

    //runInBackground
    {
      for((silkTask, readers) <- silkModule.tasks.toList zip entityReaders.grouped(2).toList)
      {
        silkExecutor.execute(silkTask, readers, outputQueue)
      }
    }
    silkExecutor.reporter.setFinishTime()
    outputQueue
  }



  /**
   * Performs quality assessment
   */
  private def assessQuality(sieveSpecDir : File, inputQuadsReader : Seq[QuadReader], qualityModule: QualityModule) : QuadReader = {
    log.info("[QUALITY]")
    log.debug("Sieve will perform quality assessment, config=%s.".format(sieveSpecDir.getAbsolutePath))

    // create a mapping between quality task and entity reader for corresponding entities
    // todo: this could also work if we created only one entitydescription for all task? bad for distributed computing, maybe?
    val entityDescriptions = qualityModule.tasks.toSeq.map(_.qualitySpec.entityDescription)
    val ebe = new EntityBuilderExecutor(ConfigParameters(config.properties))
    val readers = buildEntities(cloneQuadReaders(inputQuadsReader), entityDescriptions, ebe)

    log.info("Time needed to build entities for quality assessment phase: " + stopWatch.getTimeSpanInSeconds + "s")

    val singleScoresOutput = new QuadQueue
    val qualityExecutor = new SieveLocalQualityExecutor
    reporter.addPublisher(qualityExecutor.reporter)
    reporter.setStatus("Sieve - Quality")
    qualityExecutor.reporter.entitiesTotal = ebe.reporter.entitiesTotal.intValue()
    for((task, reader) <- qualityModule.tasks.toSeq zip readers)  {
      log.debug("\n\tMetric: %s\n\tFunction: %s\n\tEntityDescription: %s".format(task.qualitySpec.outputPropertyNames,task.qualitySpec.scoringFunctions,reader.entityDescription))
      qualityExecutor.execute(task, Seq(reader), singleScoresOutput)
    }
    qualityExecutor.reporter.setFinishTime()

    //new MultiQuadReader(output, entityBuilderExecutor.getNotUsedQuads) //andrea todo
    singleScoresOutput
  }

  private def aggregateQuality(sieveSpecDir : File, singleScoresOutput : Seq[QuadReader], qualityModule: QualityModule) : QuadReader = {

    // aggregate quality scores
    val entityDescriptions = qualityModule.aggregationTasks.toSeq.map(_.qualitySpec.entityDescription)
    val readers = buildEntities(cloneQuadReaders(singleScoresOutput), entityDescriptions, ConfigParameters(config.properties))

    StringPool.reset
    log.info("Time needed to build entities for quality aggregation phase: " + stopWatch.getTimeSpanInSeconds + "s")

    val aggregatedScoresOutput = new QuadQueue
    val qualityExecutor = new SieveLocalQualityExecutor
    //reporter.addPublisher(qualityExecutor.reporter)
    for((task, reader) <- qualityModule.aggregationTasks.toSeq zip readers)  {
      log.debug("\n\tMetric: %s\n\tFunction: %s\n\tEntityDescription: %s".format(task.qualitySpec.outputPropertyNames,task.qualitySpec.scoringFunctions,readers.map(r => r.entityDescription)))
      qualityExecutor.aggregate(task, readers, aggregatedScoresOutput)
    }
    // another way (perhaps not hadoop friendly)
    //qualityModule.aggregationTasks.foreach( task => qualityExecutor.aggregate(task, qualityAssessment, output) )

    // qualityExecutor.reporter.setFinishTime()

    aggregatedScoresOutput
  }


  /**
   * Performs data fusion
   */
  private def fuseQuads(sieveSpecDir : File, inputQuadsReader : Seq[QuadReader], fusionModule: FusionModule) : QuadReader = {
    log.info("[FUSION]")
    log.debug("Sieve will perform fusion, config=%s.".format(sieveSpecDir.getAbsolutePath))

    val outputFusedOnly = config.properties.getProperty("output", "mapped-only").toLowerCase=="fused-only"

    val entityDescriptions = fusionModule.tasks.head.sieveConfig.fusionConfig.entityDescriptions
    // why build more entity queues (<- entity description) if we only consume one for each task? (see below)
    //val entityDescriptions = fusionModule.tasks.toIndexedSeq.map(fusionExecutor.input).flatMap{ case StaticEntityFormat(ed) => ed }

    // setup sieve config parameters
    val irrelevantQuadsFile = TemporaryFileCreator.createTemporaryFile("ldif-fusion-quads", "bin")
    irrelevantQuadsFile.deleteOnExit()
    val irrelevantQuadsWriter = new FileQuadWriter(irrelevantQuadsFile)
    val fusionConfigParam =
      if (!outputFusedOnly)  {
        ConfigParameters(configProperties = config.properties, otherQuadsWriter = irrelevantQuadsWriter, collectNotUsedQuads = true)
      }
      else ConfigParameters(configProperties = config.properties)

    val entityBuilderExecutor = getEntityBuilderExecutor(fusionConfigParam)
    val entityReaders = buildEntities(inputQuadsReader, entityDescriptions, entityBuilderExecutor)

    StringPool.reset
    log.info("Time needed to build entities for fusion phase: " + stopWatch.getTimeSpanInSeconds + "s")

    val outputQueue = new QuadQueue
    val fusionExecutor = new SieveLocalFusionExecutor
    reporter.addPublisher(fusionExecutor.reporter)
    reporter.setStatus("Sieve - Fusion")
    fusionExecutor.reporter.entitiesTotal = entityBuilderExecutor.reporter.entitiesTotal.intValue()

    //runInBackground
    for((fusionTask, reader) <- fusionModule.tasks.toList zip entityReaders.toList)
    {
      log.debug("fusionTask: %s; reader: %s.".format(fusionTask.name, reader.entityDescription))
      // consume one entity queue for each task
      fusionExecutor.execute(fusionTask, Seq(reader), outputQueue)
    }

    fusionExecutor.reporter.setFinishTime()

    // build quad reader to be returned
    irrelevantQuadsWriter.finish()
    if (outputFusedOnly)
      outputQueue
    else {
      val otherQuadsReader = new FileQuadReader(irrelevantQuadsWriter)
      new MultiQuadReader(outputQueue, entityBuilderExecutor.getNotUsedQuads, otherQuadsReader)
    }
  }

  private def getEntityBuilderExecutor(configParameters: ConfigParameters) = {
    new EntityBuilderExecutor(configParameters)
  }

  /**
   * Build Entities.
   */
  private def buildEntities(readers : Seq[QuadReader], entityDescriptions : Seq[EntityDescription], entityBuilderExecutor : EntityBuilderExecutor) : Seq[EntityReader] = {
    var entityWriters: Seq[EntityWriter] = null
    val entityQueues = entityDescriptions.map(new EntityQueue(_, Consts.DEFAULT_ENTITY_QUEUE_CAPACITY))
    val fileEntityQueues = for(eD <- entityDescriptions) yield {
      val file = TemporaryFileCreator.createTemporaryFile("ldif_entities", ".dat")
      file.deleteOnExit
      new FileEntityWriter(eD, file, enableCompression = true)
    }

    val inmemory = config.properties.getProperty("entityBuilderType", "in-memory")=="in-memory"

    //Because of memory problems circumvent with FileQuadQueue */
    if(inmemory)
      entityWriters = entityQueues
    else
      entityWriters = fileEntityQueues

    val ebReporter = entityBuilderExecutor.reporter
    ebReporter.setInputQuads(dumpsQuads)
    reporter.addPublisher(ebReporter)

    try
    {
      val entityBuilderConfig = new EntityBuilderConfig(entityDescriptions.toIndexedSeq)
      val entityBuilderModule = new EntityBuilderModule(entityBuilderConfig)
      val entityBuilderTask = entityBuilderModule.tasks.head
      entityBuilderExecutor.execute(entityBuilderTask, readers, entityWriters)
    } catch {
      case e: Throwable => {
        e.printStackTrace
        throw e
      }
    }

    if(inmemory)
      entityQueues
    else
    {
      ebReporter.setFinishTime()
      fileEntityQueues.map((entityWriter) => new FileEntityReader(entityWriter))
    }
  }

  private def buildEntities(readers : Seq[QuadReader], entityDescriptions : Seq[EntityDescription], configParameters: ConfigParameters) : Seq[EntityReader] = {
    buildEntities(readers, entityDescriptions, new EntityBuilderExecutor(configParameters))
  }

  /**
   * Evaluates an expression in the background.
   */
  private def runInBackground(function : => Unit) {
    val thread = new Thread {
      private val listener: FatalErrorListener = FatalErrorListener

      override def run {
        try {
          function
        } catch {
          case e: Exception => listener.reportError(e)
        }
      }
    }
    thread.start
  }

  private def writeOutput(config : IntegrationConfig, reader : QuadReader) {
    val writers = config.outputs.getByPhase(COMPLETE)
    if(writers.size > 0 ){
      var count = 0
     //TODO consider adding a QuadWriter.getDescription method
      // log.info("Writing output to "+config.output.description)
      log.info("Writing integration output...")
      while(reader.hasNext) {
        val next = reader.read
        writers.foreach(_.write(next))
        count += 1
      }
      writers.foreach(_.finish())
      log.info(count + " Quads written") }
    else
      log.info("No final output has been specified for the Integration Job. Integration output is discarded.")
  }

  // Writes the content of #reader to all output writers defined for #phase in #config
  private def writeCopy(config : IntegrationConfig, reader: QuadReader, phase : IntegrationPhase) : QuadReader = {
    val writers = config.outputs.getByPhase(phase)
    if(writers.size == 0 )
      reader
    else {
      val readerCopy = new QuadQueue
      while(reader.hasNext) {
        val next = reader.read
        writers.foreach(_.write(next))
        readerCopy.write(next)
      }
      writers.foreach(_.finish())
      readerCopy
    }
  }

  def getLastUpdate = lastUpdate

  // Retrieve the number of quads in the sources (excluding provenance quads)
  def getSourcesQuads = config.sources.map(ImportedDumpsUtils(_).getNumberOfQuads).sum

  def toXML : xml.Node = config.toXML
}


object IntegrationJob {
  LogUtil.init
  private val log = LoggerFactory.getLogger(getClass.getName)

  def load (configFile : File, debug : Boolean = false) : IntegrationJob = {
    if(configFile != null)  {
      var config : IntegrationConfig = null
      try {
        config = IntegrationConfig.load(configFile)
      }
      catch {
        case e:ValidationException => {
          log.error("Invalid Integration Job configuration: "+e.toString +
            "\n- More details: " + Consts.xsdIntegration)
          System.exit(1)
        }
      }
      if(!config.hasValidOutputs) {
        log.error("No valid output has been specified for the Integration Job.")
        System.exit(1)
      }

      for (source <- config.sources.map(new File(_))){
        if(!source.exists()){
          log.error("An invalid source path has been specified for the Integration Job: "+ source)
          System.exit(1)
          }
      }

      val integrationJob = IntegrationJob(config, debug)
      log.info("Integration job loaded from "+ configFile.getCanonicalPath)
      integrationJob
    }
    else {
      log.warn("Integration job configuration file not found")
      null
    }
  }

  def main(args : Array[String]) {
    if(args.length == 0) {
      Ldif.printHelpAndExit()
    }

    var debug = false
    val configFile = new File(args(args.length-1))

    if(args.length>=2 && args(0)=="--debug")
      debug = true

    val integrator = IntegrationJob.load(configFile, debug)

    val runStatusMonitor = integrator.config.properties.getProperty("runStatusMonitor", "true").toLowerCase=="true"
    val statusMonitorURI = integrator.config.properties.getProperty("statusMonitorURI", Consts.DefaultStatusMonitorrURI)

    // Start REST HTTP Server
    if(runStatusMonitor)
      MonitorServer.start(statusMonitorURI)

    integrator.runIntegration
    sys.exit(0)
  }
}





