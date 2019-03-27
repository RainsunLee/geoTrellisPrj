package example

import geotrellis.raster.Tile
import geotrellis.spark.io.{FilteringLayerReader, LayerHeader}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerReader}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.spark.io._

object EtlResultReaderDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(false).setMaster("local[*]").setAppName("EtlResultReaderDemo")
    implicit val context = new SparkContext(conf)
    val catalogPath : String = "E:/矩形区域/"
    val level = 14
    val layername = "juxingquyu"
    val layerID = LayerId(layername,level)
//    val row =
    try{
      val attrReader = FileAttributeStore(catalogPath)
      println(attrReader.attributeDirectory.toString)

      val tileReader :FilteringLayerReader[LayerId] = FileLayerReader(catalogPath)

      val header = tileReader.attributeStore.readHeader[LayerHeader](layerID)
      println(header.keyClass)
      println(header.valueClass)
      val attrKeys = tileReader.attributeStore.availableAttributes(layerID)
//      for(attrkey <- attrKeys){
//        tileReader.attributeStore.read(layerID,attrkey)
//      }

      val rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
        tileReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerID)

      rdd.first()._2.renderPng().write("E:/juxing-14-fisrt.png")

      //      val rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]  =
//        tileReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](LayerId("juxingquyu", 10))
    }finally {
      context.stop()
    }

  }

}
