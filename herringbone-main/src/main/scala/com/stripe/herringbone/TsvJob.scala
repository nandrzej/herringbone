package com.stripe.herringbone

import java.io.{BufferedWriter, OutputStreamWriter}

import com.stripe.herringbone.flatten.{
  FlatConverter,
  ParquetFlatConf,
  ParquetFlatMapper,
  TypeFlattener
}
import com.stripe.herringbone.util.ParquetUtils
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.util._
import org.apache.parquet.example.data._
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.example._
import org.apache.parquet.schema._

class TsvMapper extends ParquetFlatMapper[Text] {
  def valueOut(value: Group) = {
    val tsvLine =
      FlatConverter.groupToTSV(value, flattenedSchema, separator, renameId)
    new Text(tsvLine)
  }
}

class TsvJob extends Configured with Tool {
  override def run(args: Array[String]) = {
    val conf = new ParquetFlatConf(args)
    val fs = FileSystem.get(getConf)
    val inputPath = new Path(conf.inputPath())
    val outputPathString = conf.outputPath.get
      .getOrElse(conf.inputPath().stripSuffix("/").concat("-tsv"))
    val outputPath = new Path(outputPathString)
    val previousPath = conf.previousPath.get.map { new Path(_) }

    val separator = conf.separator()
    getConf.set(ParquetFlatMapper.SeparatorKey, separator)

    val renameId = conf.renameId()
    getConf.set(ParquetFlatMapper.RenameIdKey, renameId.toString)

    if (fs.exists(outputPath)) {
      println(s"Deleting existing $outputPath")
      fs.delete(outputPath, true)
    }

    val flattenedSchema = TypeFlattener.flatten(
      ParquetUtils.readSchema(inputPath),
      previousPath.map { ParquetUtils.readSchema(_) },
      separator,
      renameId
    )

    val jobName = "tsv " + conf.inputPath() + " -> " + outputPathString
    val job = new Job(getConf, jobName)

    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    ParquetInputFormat.setReadSupportClass(job, classOf[GroupReadSupport])
    ExampleOutputFormat.setSchema(job, flattenedSchema)

    job.setInputFormatClass(classOf[CompactGroupInputFormat])
    job.setOutputFormatClass(
      classOf[TextOutputFormat[Text, Text]].asInstanceOf[Class[Nothing]])
    job.setMapperClass(classOf[TsvMapper])
    job.setJarByClass(classOf[TsvJob])
    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")
    job.getConfiguration
      .setBoolean(ParquetInputFormat.TASK_SIDE_METADATA, false)
    job.setNumReduceTasks(0)

    if (job.waitForCompletion(true)) {
      val headerPath = new Path(outputPathString + "/_header.tsv")
      writeHeader(fs, headerPath, flattenedSchema)
      0
    } else {
      1
    }
  }

  def writeHeader(fs: FileSystem, outputPath: Path, schema: MessageType) {
    val header = FlatConverter.constructHeader(schema)
    val writer = new BufferedWriter(
      new OutputStreamWriter(fs.create(outputPath, true)))
    writer.write(header)
    writer.write("\n")
    writer.close()
  }
}

object TsvJob {
  def main(args: Array[String]) = {
    val result = ToolRunner.run(new Configuration, new TsvJob, args)
    System.exit(result)
  }
}
