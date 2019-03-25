package example

import geotrellis.raster.{CellType, MultibandRaster, RasterExtent}
import geotrellis.raster.io.geotiff.{OverviewStrategy, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample.{CubicConvolution, ResampleMethod}
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.tags.codes.ColorSpace
import geotrellis.raster.render.jpg.Settings
import geotrellis.raster.render.png.PngColorEncoding
import geotrellis.raster.render.{ColorMaps, ColorRamp, ColorRamps, RGB}
import geotrellis.spark.TileLayerMetadata

/**
  * 此处如何进行重采样暂时没有实现
  */
object GeoTiffsReaderTest {
  def main(args: Array[String]): Unit = {
//    val path: String = "E:/geo.tif"
//    val geoTiff = GeoTiffReader.readSingleband(path)

        val path: String = "E:/矩形区域.tif"
        val geoTiff = GeoTiffReader.readMultiband(path)
    println("tiff的tag信息:")
    println("其中headTags如下所示")
    for ((key, value) <- geoTiff.tags.headTags) {
      println(s"${key}:${value}")
    }
    println("其中bandTags如下所示")
    var i = 1
    for (bandtag <- geoTiff.tags.bandTags) {
      println(s"第${i}组bandTags如下所示")
      bandtag.foreach(t => println(s"${t._1}:${t._2}"))
      i += 1
    }


    val tiffType = geoTiff.options.tiffType
    println(s"本tiff的tiffType为:${tiffType}")

    var colorspaceStr: String = ""
    geoTiff.options.colorSpace match {
      case 0 => colorspaceStr = "WhiteIsZero模式"
      case 1 => colorspaceStr = "BlackIsZero模式"
      case 2 => colorspaceStr = "RGB模式"
      case 3 => colorspaceStr = "Palette模式"
      case 4 => colorspaceStr = "TransparencyMask模式"
      case 5 => colorspaceStr = "CMYK模式"
      case 6 => colorspaceStr = "YCbCr模式"
      case _ => colorspaceStr = "不常见的其他模式"
    }
    println(s"本tiff的colorspace为:${colorspaceStr}")


    val colormap = geoTiff.options.colorMap
    if (colormap.isEmpty) {
      println("本tiff无colormap")
    } else {
      println(s"本tiff的colormap为:${colormap.get}")
    }


    println("tiff基本范围和像素信息")
    println(geoTiff.crs)
    println(geoTiff.extent)
    println(geoTiff.tile.dimensions)
    println(geoTiff.tile.rows)
    println(geoTiff.tile.cols)


    println("tiff波段信息:")
    println(geoTiff.imageData.bandCount)
    println(geoTiff.bandCount)

    println("像素单位信息:")
    println(geoTiff.cellSize.height)
    println(geoTiff.cellSize.width)
    println(geoTiff.cellType)

    val zoomLevel = 2
    val newCols = geoTiff.tile.cols / zoomLevel
    val newRows = geoTiff.tile.rows / zoomLevel
    val newCellWidth = geoTiff.cellSize.width * zoomLevel
    val newCellHeight = geoTiff.cellSize.height * zoomLevel
    val extent: RasterExtent = new RasterExtent(geoTiff.extent, newCellWidth, newCellHeight, newCols, newRows)
    val tile : MultibandRaster = geoTiff.resample(extent, ResampleMethod.DEFAULT, AutoHigherResolution)

    /**
      * 默认情况下：
      * [多波段数据和属性组织]
      * 多波段颜色模式多为RGB模式 ，以R(red),G(green),B(blue)为波段顺序，每个波段均为为相同范围的tile
      * [多波段渲染——默认过程]
      * 进行MulitBandTile.renderXXX()时,底层实现如下:
      *   1) 先执行MulitBandTile.color()；将多波段按顺序合并成RGBA的单波段；
      *       （red,green,blue,alpha）
      *   2）将合并后的tile直接写成jpg或者png
      * [存在问题]
      *   jpg或者png是以ARGB的顺序进行渲染，因此默认过程下渲染的图片色彩出现异常。
      * [解决措施]
      *   对color合成后的tile的值进行换位，将RGBA ——> ARGB,
      *       i => (i >> 8) | (i & 0xFF) << 24
      *   再进行渲染生成png或者jpg
      */
    tile.tile.renderJpg()
    tile.tile.color().map(i => (i >> 8) | (i &0xFF) << 24).renderJpg().write("E:/juxing_copy.jpg")

    /**
      *
      * [单波段数据和属性组织]
      * 单波段颜色模式多为 Palette模式，波段(tile)中仅存储单个值，但在属性中存储colormap，tile中的值依据colormap找到对应rgb
      * * ，再进行颜色显示。
      * [单波段渲染-默认过程]
      * 进行Tile.renderXXX()，直接将单波段的值写入jgp或者png
      * [存在问题]
      *  jpg或者png显示时，读取像素值[alpha-red-green-blue](共32位)，而单波段写入的时候，相当于只将值写入了最低位（blue），
      *  因此在jpg显示时，仅渲染蓝色，整个图片呈现出蓝色，颜色异常。
      * [解决措施]
      * renderXXX()时，将元数据中的colormap作为参数传入，底层会将像素值通过colormap映射为RGB值，再将RGB值转成ARGB，最后写入
      * png或者jpg，从而显示正常。
      */
    //    tile.tile.renderJpg().write("E:/geo_default.jpg")
//    tile.tile.renderJpg(colormap.get).write("E:/geo_colormap.jpg")
//    tile.tile.renderPng().write("E:/geo_default.png")
//    tile.tile.renderPng(colormap.get).write("E:/geo_colormap.png")


    //    println(tile.extent)
    //    tile.data.color().renderJpg(ColorRamp(RGB(255,255,255),RGB(0,0,0)).stops(16)).write("E:/jux-true.jpg")
    //    geoTiff.tile.renderJpg().write("E:/geo_copy.jpg")

    //    var bandNum:Int = 1
    //    for(bandTile <- geoTiff.tile.bands){
    //      bandTile.renderJpg().write(s"E:/junxing-band-${bandNum}.jpg")
    //      bandTile.renderJpg(Settings.DEFAULT)
    //      bandNum += 1
    //    }

  }
}
