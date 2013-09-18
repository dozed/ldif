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
import ldif.datasources.dump.{ParseError, NoResult, QuadResult, QuadParser}
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
import akka.dispatch.{RequiresMessageQueue, BoundedMessageQueueSemantics}
import com.typesafe.config.ConfigFactory

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
      quadParser.parseLineAsParseResult(line) match {
        case QuadResult(q) =>
        case NoResult =>
        case ParseError(e) =>
          errorList += Pair(lineNr, line)
      }
      line = input.readLine()
      lineNr += 1
    }

    errorList
  }

  def readQuads(input: BufferedReader, quadQueue: QuadWriter): LoaderResult = {
    var loop = true

    var counter = 1
    var importedQuads = 0
    val errorList = ArrayBuffer[(Int, String)]()

    while (loop) {
      val line = input.readLine()

      if (line == null) {
        loop = false
      } else {
        quadParser.parseLineAsParseResult(line) match {
          case QuadResult(q) =>
            quadQueue.write(q)
            importedQuads += 1
          case NoResult =>
          case ParseError(e) =>
            errorList += ((counter, line))
            log.warn(f"Parse error found at line $counter. Input line: $line")
        }
      }
      counter += 1
    }
    if (!discardFaultyQuads && errorList.size > 0)
      throw new RuntimeException(f"Found errors while parsing NT/NQ file. Found ${errorList.size} parse errors. See log file for more details.")

    LoaderResult(importedQuads, errorList)
  }

  def validateQuadsMT(input: BufferedReader): Seq[Pair[Int, String]] = {
    LocalNode.setUseStringPool(false)

    val errorMessage = processQuads(input, new DummyQuadWriter)

    LocalNode.setUseStringPool(true)
    errorMessage.errors
  }

  def readQuadsMT(input: BufferedReader, quadWriter: QuadWriter): LoaderResult = {
    val result = processQuads(input, quadWriter)

    if (result.invalidQuads > 0)
      throw new RuntimeException(f"Found errors while parsing NT/NQ file. Found ${result.invalidQuads} parse errors. Please set 'validateSources' to true and rerun for error details.")

    result
  }

  private def processQuads(input: BufferedReader, quadWriter: QuadWriter): LoaderResult = {
    val numParsers = 4
    val doneLatch = new CountDownLatch(numParsers)

    // bounded consumer mailbox prevents that the mailbox overflows (OutOfMemory)
    // since the consumer is slower compared to the producers due to Quad -> Byte conversion and disk I/O
    val config: String = """bounded-mailbox {
                           |    mailbox-type = "akka.dispatch.BoundedMailbox"
                           |    mailbox-capacity = 200
                           |    mailbox-push-timeout-time = 10s
                           |}
                           |
                           |akka.actor.mailbox.requirements {
                           |    "akka.dispatch.BoundedMessageQueueSemantics" = bounded-mailbox
                           |}
                           |
                           | """.stripMargin

    // setup actor system
    val system = ActorSystem("QuadFileLoaderSystem", ConfigFactory.load(ConfigFactory.parseString(config)))

    val quadWriterActor = system.actorOf(Props[QuadWriterActor](new QuadWriterActor(quadWriter)))

    val routees = (1 to numParsers) map {
      i =>
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
    val f = akka.pattern.ask(quadWriterActor, GetLoaderResult).mapTo[LoaderResult]
    val result = Await.result(f, Duration.Inf)

    // shutdown actor system
    system.shutdown()

    result
  }

}

object MTTest {
  def main(args: Array[String]) {
    println("Starting...")
    val start = System.currentTimeMillis
    //    val reader = new BufferedReader(new FileReader("/home/andreas/cordis_dump.nt"))
    val reader = new BufferedReader(new FileReader("/home/andreas/aba.nt"))
    val loader = new QuadFileLoader("irrelevant")
    val writer = new OutputStreamWriter(new FileOutputStream("/tmp/1"))

    val result = loader.readQuadsMT(reader, new QuadWriter {
      def finish() {}

      def write(quad: Quad) {
        writer.write(quad.toLine)
      }
    })
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
      val idx: Int = globalIndex + index + 1 // TODO check
      parser.parseLineAsParseResult(line) match {
        case QuadResult(q) => quads.append(q)
        case NoResult =>
        case ParseError(e) => errors.append(Pair(idx, line))
      }
    }

    if (errors.size > 0) {
      quadConsumer ! Errors(errors)
    } else {
      quadConsumer ! QuadsMessage(quads)
    }
  }

  def receive = {
    case QuadStrings(c, lines) => parseQuads(c, lines)
    case Finish => doneLatch.countDown()
  }
}

class QuadWriterActor(quadWriter: QuadWriter) extends Actor with RequiresMessageQueue[BoundedMessageQueueSemantics] {

  private val allErrors = new ArrayBuffer[Pair[Int, String]]

  private var importedQuads = 0

  def receive = {
    case QuadsMessage(quads) =>
      importedQuads += quads.size
      for (quad <- quads) quadWriter.write(quad)
    case Errors(quadErrors) => allErrors ++= quadErrors
    case GetLoaderResult => sender ! LoaderResult(importedQuads, allErrors.sortWith(_._1 < _._1))
    case _ =>
  }
}

case class QuadsMessage(val quads: Seq[Quad])

case class QuadStrings(val counter: Int, val quads: Seq[String])

case class Errors(val quadErrors: Seq[Pair[Int, String]])

case class LoaderResult(val importedQuads: Int, val errors: Seq[Pair[Int, String]]) {
  val invalidQuads = errors.size
}

case object GetLoaderResult

case object Finish
