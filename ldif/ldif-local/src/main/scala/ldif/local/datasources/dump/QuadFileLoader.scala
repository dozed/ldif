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

package ldif.local.datasources.dump

import ldif.local.runtime.LocalNode
import ldif.local.runtime.impl.{FileQuadReader, FileQuadWriter, DummyQuadWriter}
import ldif.datasources.dump.QuadParser
import ldif.runtime.QuadWriter
import ldif.util.Consts
import ldif.util.TemporaryFileCreator
import ldif.runtime.Quad

import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

import java.io._
import akka.actor._
import akka.routing.{Broadcast, SmallestMailboxRouter}
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import akka.util.Timeout

/**
 * Created by IntelliJ IDEA.
 * User: andreas
 * Date: 09.06.11
 * Time: 17:15
 * To change this template use File | Settings | File Templates.
 */

class QuadFileLoader(graphURI: String = Consts.DEFAULT_GRAPH, discardFaultyQuads: Boolean = false) {

  private val log = LoggerFactory.getLogger(getClass.getName)

  private val quadParser = new QuadParser(graphURI)

  // set timeout, the other option would be to block on something
  implicit val timeout: Timeout = 5 hours

  def validateQuads(input: BufferedReader): Seq[Pair[Int, String]] = {
    val errorList = new ArrayBuffer[Pair[Int, String]]
    var lineNr = 1

    var line: String = input.readLine()
    while (line != null) {
      quadParser.parseLineAsOpt(line) match {
        case Some(quad) =>
        case None =>
          errorList += Pair(lineNr, line)
      }
      line = input.readLine()
      lineNr += 1
    }

    errorList
  }

  def readQuads(input: BufferedReader, quadQueue: QuadWriter) {
    var loop = true
    var faultyRead = false

    var counter = 1;
    var nrOfErrors = 0;

    while (loop) {
      val line = input.readLine()

      if (line == null) {
        loop = false
      } else {
        quadParser.parseLineAsOpt(line) match {
          case Some(quad) => quadQueue.write(quad)
          case None =>
            if (!discardFaultyQuads)
              faultyRead = true
            nrOfErrors += 1
            log.warn("Parse error found at line " + counter + ". Input line: " + line)
        }
      }
      counter += 1
    }
    if (faultyRead)
      throw new RuntimeException("Found errors while parsing NT/NQ file. Found " + nrOfErrors + " parse errors. See log file for more details.")
  }

  def validateQuadsMT(input: BufferedReader): Seq[Pair[Int, String]] = {
    LocalNode.setUseStringPool(false)

    val errorMessage = processQuads(input, new QuadValidationActor())

    LocalNode.setUseStringPool(true)
    errorMessage.quadErrors
  }

  def readQuadsMT(input: BufferedReader, quadWriter: QuadWriter) {
    val errorMessage = processQuads(input, new QuadWriterActor(quadWriter))

    val errors = errorMessage.quadErrors

    if (errors.size > 0)
      throw new RuntimeException("Found errors while parsing NT/NQ file. Found " + errors.size + " parse errors. Please set 'validateSources' to true and rerun for error details.")
  }

  def processQuads(input: BufferedReader, processorActor: => Actor): Errors = {
    val numParsers = 10
    val doneLatch = new CountDownLatch(numParsers)

    // setup actor system
    import system.dispatcher
    val system = ActorSystem("QuadFileLoaderSystem")

    val quadWriterActor = system.actorOf(Props[Actor](processorActor))

    val routees = (1 to numParsers) map { i =>
      system.actorOf(Props[QuadParserActor](new QuadParserActor(quadWriterActor, graphURI, doneLatch)))
    }

    val smallestMailboxRouter = system.actorOf(
      Props.empty.withRouter(SmallestMailboxRouter(routees)), "parserRouter")

    var loop = true
    var counter = 0
    var nextCounterStep = 0
    var lines = new ArrayBuffer[String]

    while (loop) {
      val line = input.readLine()

      // aggregate lines
      if (line == null) {
        loop = false
      } else {
        counter += 1
        lines.append(line)
      }

      // send out lines to parser
      if (counter % 100 == 0 || loop == false) {
        smallestMailboxRouter ! QuadStrings(nextCounterStep, lines)
        lines = new ArrayBuffer[String]
        nextCounterStep += 100
      }
    }

    // wait until all parser actors are finished
    smallestMailboxRouter ! Broadcast(Finish)
    doneLatch.await

    // query errors
    val f = akka.pattern.ask(quadWriterActor, GetErrors).mapTo[Errors]
    val errorMessage = Await.result(f, Duration.Inf)

    // shutdown actor system
    system.shutdown()

    errorMessage
  }

}

object MTTest {
  def main(args: Array[String]) {
    println("Starting...")
    val start = System.currentTimeMillis
    //    val reader = new BufferedReader(new FileReader("/home/andreas/cordis_dump.nt"))
    val reader = new BufferedReader(new FileReader("/home/stefan/Code/diplom-code/ldif-geo/geo/integrated_reegle_geonames.nt"))
    val loader = new QuadFileLoader("irrelevant")
    val quadWriter = new QuadWriter {
      def write(quad: Quad) {
        println(quad)
      }

      def finish() {}
    }
    loader.readQuadsMT(reader, quadWriter)
    //    val results = loader.validateQuadsMT(reader)
    //    if(results.size>0)
    //      println(results.size + " errors found.")
    //    for(result <- results)
    //      println("Error: Line " + result._1 + ": " + result._2)
    println("That's it. Took " + (System.currentTimeMillis - start) / 1000.0 + "s")
  }
}

object QuadFileLoader {
  def loadQuadsIntoTempFileQuadQueue(file: File, graph: String, dicardFaultyQuads: Boolean): FileQuadReader = {
    val reader = new BufferedReader(new FileReader(file))
    loadQuadsIntoTempFileQuadQueue(reader, graph, dicardFaultyQuads)
  }

  def loadQuadsIntoTempFileQuadQueue(inputStream: InputStream, graph: String = Consts.DEFAULT_GRAPH, dicardFaultyQuads: Boolean = false): FileQuadReader = {
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    loadQuadsIntoTempFileQuadQueue(reader, graph, dicardFaultyQuads)
  }

  def loadQuadsIntoTempFileQuadQueue(reader: Reader, graph: String, dicardFaultyQuads: Boolean): FileQuadReader = {
    val bufReader = new BufferedReader(reader)
    loadQuadsIntoTempFileQuadQueue(bufReader, graph, dicardFaultyQuads)
  }

  def loadQuadsIntoTempFileQuadQueue(reader: BufferedReader, graph: String, dicardFaultyQuads: Boolean): FileQuadReader = {
    val output = TemporaryFileCreator.createTemporaryFile("ldif-quadqueue", ".dat")
    output.deleteOnExit()
    val writer = new FileQuadWriter(output)
    val loader = new QuadFileLoader(graph, dicardFaultyQuads)
    loader.readQuads(reader, writer)
    writer.finish()
    return new FileQuadReader(output)
  }
}

class QuadParserActor(quadConsumer: ActorRef, graphURI: String, doneLatch: CountDownLatch) extends Actor {
  private val parser = new QuadParser(graphURI)

  private def parseQuads(globalIndex: Int, lines: scala.Seq[String]): Unit = {
    val errors = new ArrayBuffer[Pair[Int, String]]
    val quads = new ArrayBuffer[Quad]

    for ((line, index) <- lines.zipWithIndex) {
      parser.parseLineAsOpt(line) match {
        case Some(quad) => quads.append(quad)
        case None =>
          val idx: Int = globalIndex + index + 1 // TODO check
          errors.append(Pair(idx, line))
      }
    }

    if (errors.size > 0)
      quadConsumer ! Errors(errors)
    else
      quadConsumer ! QuadsMessage(quads)
  }

  def receive = {
    case QuadStrings(c, lines) => parseQuads(c, lines)
    case Finish => doneLatch.countDown()
  }
}

class QuadValidationActor() extends Actor {

  private val allErrors = new ArrayBuffer[Pair[Int, String]]

  def receive = {
    case Errors(quadErrors) => allErrors ++= quadErrors // accumulate errors
    case GetErrors => sender ! Errors(allErrors.sortWith(_._1 < _._1)) // reply with errors
    case _ => // ignore other messages
  }

}

class QuadWriterActor(quadWriter: QuadWriter) extends Actor {

  private val allErrors = new ArrayBuffer[Pair[Int, String]]

  def receive = {
    case QuadsMessage(quads) => for (quad <- quads) quadWriter.write(quad)
    case Errors(quadErrors) => allErrors ++= quadErrors
    case GetErrors => sender ! Errors(allErrors.sortWith(_._1 < _._1))
    case _ =>
  }
}

case class QuadsMessage(val quads: Iterable[Quad])

case class QuadStrings(val counter: Int, val quads: Seq[String])

case class Errors(val quadErrors: Seq[Pair[Int, String]])

case object GetErrors

case object Finish
