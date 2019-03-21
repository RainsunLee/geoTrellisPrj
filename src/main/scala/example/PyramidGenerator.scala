package main

import geotrellis.proj4.WebMercator
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.reproject.Reproject.Options
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.spark.{ContextRDD, SpatialKey, TileLayerMetadata, withProjectedExtentReprojectMethods, withTilerMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD

import scala.io.StdIn

object PyramidGenerator extends App {

  val tiffpath: String = "file://E:/geo.tif"
  val catalogpath: String = "E:/geotrellis/catalog"

  val conf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("PyramidGenerator-rect")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

  val sc = new SparkContext(conf)

  var layerMetadatas: Map[Int, TileLayerMetadata[SpatialKey]] = Map()

  try {
    run(sc)

    println("finish run(sc),press any key to exit!")
    StdIn.readLine()
  } finally {
    sc.stop()
  }

  def run(implicit sc: SparkContext): Unit = {
    // static
    var level = -1
    var row = -1
    var col = -1
    // globe
    val inputRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(tiffpath)
    val layoutScheme = ZoomedLayoutScheme(WebMercator, 256)

    var readIn = StdIn.readLine()

    var lmetadata: Option[TileLayerMetadata[SpatialKey]] = Option.empty
    while (readIn != "quit") {
      if (readIn.equalsIgnoreCase("print")) {
        println("影像元信息如下:")
        var i = 0
        for (tile <- inputRdd) {
          i += 1
          println(s"第${i}个partition的范围为:${tile._1.extent},瓦片像素大小为:${tile._2.dimensions}")
        }
      }else if (readIn.startsWith("layout-")) {
        val strs = readIn.split("-")
        val l = strs(1).toInt
        val extent = layoutScheme.levelForZoom(l).layout.mapTransform.keyToExtent(SpatialKey(strs(2).toInt,strs(3).toInt))
        println(s"第${l}层的${strs(2).toInt}-${strs(3).toInt}瓦片的范围为${extent}")
        println(s"第${l}层的行列范围为:${layoutScheme.levelForZoom(l).layout.tileLayout.layoutDimensions}")
      } else if (readIn.contains("-")) { // row-col
        if (level != -1 || lmetadata.isEmpty) {
          var strs = readIn.split("-")
          if (strs.length != 2) {
            println("请继续输入行列号，以row-col的格式输入")
          } else {
            try {
              row = strs(0).toInt
              col = strs(1).toInt
              val layer = inputRdd.tileToLayout(lmetadata.get)
              val tile = layer.lookup(SpatialKey(row, col))
              if (tile.size != 1) {
                println(s"对应层行列${level}-${row}-${col}的瓦片不存在!")
              } else {
                val filepath = s"E:/geo-${level}-${row}-${col}.jpg"
                println(s"已找到对应瓦片并写入${filepath}")
                tile(0).renderJpg().write(filepath)
              }
              println("请先输入整数层级，范围为0-30,或者输入quit退出")
            } catch {
              case e => {
                e.printStackTrace()
                println(s"当前输入的行列号${readIn}无法解析，请按row-col格式重新输入!")
                row = -1
                col = -1
              }
            }
          }
        } else {
          println("请先输入整数层级，范围为0-50")
        }
      } else { // level
        try {
          level = readIn.toInt
          if (level > 50 || level < 0) {
            println("当前输入层级超出范围，请输入0-50的层级")
            level = -1
          } else {
            lmetadata = layerMetadatas.get(level)
            if (lmetadata.isEmpty) {
              val metadata = TileLayerMetadata.fromRDD(inputRdd, layoutScheme.levelForZoom(level).layout)
              //              inputRdd.tileToLayout(metadata).cache()
              lmetadata = Some(metadata)
              layerMetadatas += (level -> metadata)
            }
            println(s"层级${level}的行列范围为${lmetadata.get.bounds.get.minKey}到${lmetadata.get.bounds.get.maxKey}")
            println("请继续输入行列号，以row-col的格式输入")
          }
        } catch {
          case e => {
            println("当前输入层级不为整形")
            level = -1
          }
        }
      }
      readIn = StdIn.readLine()
    }
    //session
    //    val layout = layoutScheme.levelForZoom(level).layout
    //    val metadata = TileLayerMetadata.fromRDD(inputRdd, layout)
    //    println(s"第${level}层的layoutDimensions为：${metadata.tileLayout.layoutDimensions}")
    //    println(s"第${level}层的maxKey为：${metadata.bounds.get.maxKey}")
    //    println(s"第${level}层的minKey为：${metadata.bounds.get.minKey}")
    //    println(s"第${level}层的tileDimensions为：${metadata.tileLayout.tileDimensions}")
    //    val layer = inputRdd.tileToLayout(metadata)
    //    val tile = layer.lookup(SpatialKey(row, col))
    //    tile(0).renderJpg().write(s"E:/geo-${level}-${row}-${col}.jpg")
  }
}
