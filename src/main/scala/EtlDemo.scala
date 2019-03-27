import geotrellis.raster.{CellSize, CellType, Tile, UByteCells}
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.{LayerId, SpatialKey}
import geotrellis.spark.etl.Etl
import geotrellis.spark.etl.config._
import geotrellis.vector.ProjectedExtent
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object EtlDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(false).setAppName("etl-app").setMaster("local[*]");
    implicit val context = new SparkContext(conf)
    try{
//      Etl.ingest[ProjectedExtent, SpatialKey, Tile](args)
//      implicit def classTagK = ClassTag(typeTag[SpatialKey].mirror.runtimeClass(typeTag[SpatialKey].tpe)).asInstanceOf[ClassTag[SpatialKey]]
//      implicit def classTagV = ClassTag(typeTag[Tile].mirror.runtimeClass(typeTag[Tile].tpe)).asInstanceOf[ClassTag[Tile]]
      val input = Input("juxingquyu","geotiff",Backend(HadoopType,HadoopPath("file:///E:/矩形区域.tif")),
        Some(StorageLevel.NONE),Some(0.0))
      val backend = Backend(FileType,HadoopPath("E:/矩形区域/"))
      val pngBackend = Backend(BackendType.fromString("render"),UserDefinedPath("E:/矩形区域png/{name}/{z}-{x}-{y}.png"))
      val output = Output(backend,
        Bilinear,BufferedReproject,IngestKeyIndexMethod("zorder"),256,true,None,Some("zoomed"),None,Some("EPSG:3857"),
        Some(0.1),Some(CellSize(256.0,256.0)),Some(CellType.fromName("int8")),Some("png"),None,Some(15),None,None);
      val etlConf = new EtlConf(input,output)
      val etl = new Etl(etlConf,Etl.defaultModules)
      val sourceTiles = etl.load[ProjectedExtent,Tile]()
      val (zoomed,tile) = etl.tile[ProjectedExtent, Tile, SpatialKey](sourceTiles)
      etl.save[SpatialKey,Tile](LayerId(etl.input.name,zoomed),tile)
    } finally {
      context.stop()
    }
  }

}
