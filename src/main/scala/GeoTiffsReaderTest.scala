package example

import geotrellis.raster.RasterExtent
import geotrellis.raster.io.geotiff.{OverviewStrategy, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample.{CubicConvolution, ResampleMethod}
import geotrellis.raster.io.geotiff._
import geotrellis.spark.TileLayerMetadata

/**
  * 此处如何进行重采样暂时没有实现
  */
object GeoTiffsReaderTest {
  def main(args: Array[String]): Unit = {
//    val path :String = "E:/矩形区域.tif"
    val path :String = "E:/geo.tif"
    val geoTiff = GeoTiffReader.readMultiband(path)
    println(geoTiff.crs)
    println(geoTiff.extent)
    println(geoTiff.tile.dimensions)
    println(geoTiff.tile.rows)
    println(geoTiff.tile.cols)
    println(geoTiff.cellSize.height)
    println(geoTiff.cellSize.width)
    println(geoTiff.cellType)
    println(geoTiff.bandCount)
    val zoomLevel = 2
    val newCols = geoTiff.tile.cols / zoomLevel
    val newRows = geoTiff.tile.rows / zoomLevel
    val newCellWidth = geoTiff.cellSize.width * zoomLevel
    val newCellHeight = geoTiff.cellSize.height * zoomLevel
    val extent:RasterExtent = new RasterExtent(geoTiff.extent,newCellWidth,newCellHeight,newCols,newRows)
    val tile = geoTiff.resample(extent,ResampleMethod.DEFAULT,AutoHigherResolution)
    println(tile.extent)
    tile.tile.renderJpg().write("E:/geotiff6.jpg")
  }
}
