package example

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.{ContextRDD, LayerId, SpatialKey, TileLayerMetadata, withTilerMethods}
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutScheme, ZoomedLayoutScheme}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.io.StdIn

/**
  * 从tiff中动态瓦片获取逻辑：(暂时需要继续跟进)
  * 1. tiff仅有tile以及extent信息
  * 2. 转成RDD结构，自动进行瓦片partition操作
  * 3. 静态提前切割成pyramids或者仅构建metadata
  * 4. 根据请求层号，获取指定层级layer
  * 5. 构建layer的metadata，主要包括layout，支持从行列号到地理范围的映射
  * 6. 根据请求行列号从指定layer中获取tile数据
  * 7. 转成png/jpg返回前台
  */
object TMSServer {


  class LayoutSchemeActor(val schema: ZoomedLayoutScheme) extends Actor {
    override def receive: Receive = {
      case _ => println("not impl")
    }
  }

  def renderPngTile(level: Int, row: Int, col: Int)(implicit rdd: RDD[(ProjectedExtent, Tile)], layoutScheme: ZoomedLayoutScheme): Option[Array[Byte]] = {
    if (level < 0 || level > 30 || row < 0 || col < 0) {
      return None
    }
    /**
      * 根据层级计算对应层级的切片规则
      */
    val layerLayout = layoutScheme.levelForZoom(level)

    /**
      * 获取tiff在对应层级的layermetadata
      */
    val layerMetadata = TileLayerMetadata.fromRDD(rdd, layerLayout.layout)

    /**
      * 判断行列号是否在tiff的对应层级的layer范围内
      */
    val minKey = layerMetadata.bounds.get.minKey
    val maxKey = layerMetadata.bounds.get.maxKey
    if (minKey.row > row || minKey.col > col
      || maxKey.row < row || maxKey.col < col) {
      return None
    }

    /**
      * 调用geotrellis的向指定图层切片的算子进行切片，此过程耗时较长
      */
    val tilesRdd = rdd.tileToLayout(layerMetadata)

    /**
      * 依据列号和行号生成索引，并在切片后图层rdd中查找对应瓦片
      */
    val key = SpatialKey(col, row)
    val tile = tilesRdd.lookup(key)

    /**
      * 对瓦片进行图片生成并返回字节流
      */
    if (tile.isEmpty) {
      None
    } else {
      Some(tile(0).renderJpg().bytes)
    }
  }


  def releaseSparkEnv()(implicit context: SparkContext): Unit = {
    context.stop()
  }

  def main(args: Array[String]): Unit = {
    /**
      * 指定原始数据
      */
    val tiffpath = "file://E:/矩形区域.tif"

    /**
      * 初始化Actor环境
      */
    implicit val system = ActorSystem("Gxxs-TMS")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    /**
      * 初始化Spark环境
      */
    val sparkConf = new SparkConf(false).setMaster("local[4]").setAppName("GeotrellisTMSServer")
    implicit val context = new SparkContext(sparkConf)

    /**
      * 初始化Geotrellis环境
      */
    implicit val rdd = context.hadoopGeoTiffRDD(tiffpath)
    implicit val layoutScheme = ZoomedLayoutScheme(WebMercator, 256)
    implicit val layoutSchemeActorRef = system.actorOf(Props(classOf[LayoutSchemeActor], layoutScheme), "layout")


    /**
      * 构造http handler
      */
    val tmsHandler = pathPrefix("tms" / IntNumber) {
      level =>
        pathPrefix(IntNumber / IntNumber) {
          (row, col) =>
            complete {
              val res = renderPngTile(level, row, col)
              if (res.isEmpty) {
                StatusCodes.NotFound
              } else {
                HttpEntity(ContentType(MediaTypes.`image/jpeg`), res.get)
              }
            }
        }
    }

    /**
      * 构造http路由
      */
    val route: Route = get {
      tmsHandler
    }

    /**
      * 启动server
      */
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    println("TMSServer started")

    StdIn.readLine()

    /**
      * 注销actor资源
      */
    bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())

    /**
      * 注销spark资源
      */
    releaseSparkEnv()
  }
}
