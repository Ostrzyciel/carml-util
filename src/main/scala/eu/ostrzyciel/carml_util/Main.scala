package eu.ostrzyciel.carml_util

import io.carml.engine.rdf.RdfRmlMapper
import io.carml.logicalsourceresolver.JsonPathResolver
import io.carml.util.RmlMappingLoader
import io.carml.vocab.Rdf

import scala.collection.mutable.ListBuffer
//import com.taxonic.carml.engine.RmlMapper
//import com.taxonic.carml.logical_source_resolver.JsonPathResolver
//import com.taxonic.carml.util.RmlMappingLoader
//import com.taxonic.carml.vocab.Rdf
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.*
import org.apache.pekko.util.ByteString
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import java.io.{ByteArrayInputStream, FileOutputStream}
import java.nio.file.Paths
import java.text.Normalizer.Form
import scala.concurrent.Await

object Main:
  implicit val as: ActorSystem[Nothing] = ActorSystem[Nothing](Behaviors.empty, "carml-util")
  implicit val ec: scala.concurrent.ExecutionContext = as.executionContext

  @main def run(mappingFile: String, input: String, outputDir: String): Unit =
    println("Loading mapping...")

    val mapping = RmlMappingLoader.build()
      .load(RDFFormat.TURTLE, Paths.get(mappingFile))

//    val mapper = RmlMapper.newBuilder()
//      .setLogicalSourceResolver(Rdf.Ql.JsonPath, new JsonPathResolver())
//      .iriUnicodeNormalization(Form.NFKC)
//      .iriUpperCasePercentEncoding(false)
//      .build()
//
    val mapper = RdfRmlMapper.builder()
      .triplesMaps(mapping)
      .setLogicalSourceResolver(Rdf.Ql.JsonPath, () => JsonPathResolver.getInstance())
      .iriUnicodeNormalization(Form.NFKC)
      .iriUpperCasePercentEncoding(false)
      .build()

    val fut = FileIO.fromPath(Paths.get(input))
      .via(Framing.delimiter(ByteString("\n"), 1_000_000, allowTruncation = true))
      .map(bs => ByteArrayInputStream(bs.toArray))
      .buffer(100, OverflowStrategy.backpressure)
      .async
      .map(mapper.mapToModel)
//      .map(bs => {
//        mapper.bindInputStream(bs)
//        mapper.map(mapping)
//      })
      .buffer(10, OverflowStrategy.backpressure)
      .async
      .zipWithIndex
      .runForeach((m, i) => {
        if (i % 1000 == 0) println(s"Processed $i")
        val num = f"$i%010d"
        var dir = ""
        val extraDirs = ListBuffer[String]()
        for d <- -2 until 0 do
          val currentLevel = num.slice(10 + 3 * (d - 1), 10 + 3 * d) + "/"
          dir += currentLevel
          if num.slice(10 + 3 * d, 10).toInt == 0 then
            extraDirs += dir

        for d <- extraDirs do
          val f = new java.io.File(outputDir + f"/$d")
          if !f.exists() then
            f.mkdir()

        val os = new FileOutputStream(outputDir + "/" + dir + f"$i%010d.ttl")
        try Rio.write(m, os, RDFFormat.TURTLE)
        finally os.close()
      })
      .recover(e => println(e))

    println("Mapping...")
    Await.ready(fut, scala.concurrent.duration.Duration.Inf)
