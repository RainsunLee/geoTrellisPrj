package example

import geotrellis.raster.Tile
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RastSparkExample {
  def `Having an RDD[(ProjectedExtent,Tile)] ingest it as a Structured COG layer and Query it` :Unit = {
    val conf : SparkConf = new SparkConf(false)
    implicit val sc : SparkContext = new SparkContext(conf)
//    val rdd : RDD[(ProjectedExtent,Tile)] = new HadoopGeo
  }
}
