package com.cloudera.spark

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import org.apache.avro.io.DecoderFactory
import org.apache.avro.generic.GenericDatumReader

object AvroToSolr {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val p_schema = args(0)
    val p_inputdata = args(1)
    val b_schema = sc.broadcast(p_schema)

    val rdd = sc.textFile(p_inputdata)

    def convertToAvro(data: String, schema: Schema): (String, GenericRecord) = {
      val decoder = DecoderFactory.get().jsonDecoder(schema, data)
      val reader = new GenericDatumReader[GenericRecord](schema)
      val datum = reader.read(null, decoder)
      (datum.get("id").toString, datum)
    }

    def convertToSolrDoc(data: (String, GenericRecord)): (String, ListBuffer[String], ListBuffer[String], ListBuffer[String]) = {
      val id = data._1
      val avroRec = data._2

      val names = avroRec.get("Names").asInstanceOf[org.apache.avro.generic.GenericData.Array[GenericRecord]]
      val addresses = avroRec.get("Addresses").asInstanceOf[org.apache.avro.generic.GenericData.Array[GenericRecord]]

      var fname_list = new ListBuffer[String]
      var lname_list = new ListBuffer[String]
      var address_list = new ListBuffer[String]

      var iter = names.iterator()
      while (iter.hasNext) {
        var name = iter.next()
        fname_list += name.get("fname").toString
        lname_list += name.get("lname").toString
      }

      iter = addresses.iterator()
      while (iter.hasNext) {
        var address = iter.next
        address_list += address.get("streetno").toString.concat(" ").
          concat(address.get("streetname").toString).concat(" ").
          concat(address.get("city").toString)
      }
      (id, fname_list, lname_list, address_list)
    }

    val avrordd = rdd.mapPartitions{ iter =>
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val path = new Path(b_schema.value)
      val in = fs.open(path)
      //val schema = new Schema.Parser().parse(b_schema.value)
      val schema = new Schema.Parser().parse(in).getTypes().get(2)
      iter.map(x => convertToAvro(x, schema))
    }

    val solrRdd = avrordd.mapPartitions{ iter =>
      iter.map(x => convertToSolrDoc(x))
    }

    println(solrRdd.count)
  }
}
