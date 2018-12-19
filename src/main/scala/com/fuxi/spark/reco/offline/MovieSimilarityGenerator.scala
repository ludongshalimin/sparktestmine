package com.fuxi.spark.reco.offline

import com.fuxi.spark.util.RedisClient
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.training.spark.proto.Spark.{ItemList, ItemSimilarities, ItemSimilarity}
object MovieSimilarityGenerator {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[*]"
    var dataPath = "data/ml-1m/ratings.dat"
    if(args.length > 1) {
      masterUrl = args(0)
    }else if(args.length > 1) {
      dataPath = args(1)
    }

    val conf = new SparkConf().setMaster(masterUrl).setAppName("filmSimiarityGenerator")
    val sc = new SparkContext(conf)

    val inputFile = dataPath
    val threhold = 0.1

    //Load and parse the data from file
    // 数据格式：userID,movieId,rating
    val rows = sc.textFile(inputFile).map(_.split("::")).map{ p=>
      (p(0).toLong,p(1).toInt,p(2).toDouble)
    }

    //因为movieId是连续的，在之前就进行了编码
    val maxMovieId = rows.map(_._2).max() + 1

    val rowRdd = rows.map {p =>
      (p._1,(p._2,p._3))
    }.groupByKey().map{ kv =>
      Vectors.sparse(maxMovieId,kv._2.toSeq)
    }.cache()
    val mat = new RowMatrix(rowRdd)
    println(s"mat row/col number: ${mat.numRows()},${mat.numCols()}")

    //计算列之间的相似度
    //计算相似度这里使用了评分作为依据。来构建
    //compute similar columns perfectly,with brute force
    //similarities:CoordinateMatrix
    val similarities = mat.columnSimilarities(threhold)

    //这里的i,j代表similarities的第i列和第j列的相似度
    //MatrixEntry(i,j,u)
    //save movie-movie similarity to redis
    val result = similarities.entries
    similarities.entries.map {case MatrixEntry(i,j,u) =>
      (i,(j,u))
    }.groupByKey(2).map{kv =>
      (kv._1,kv._2.toSeq.sortWith(_._2 > _._2).take(20).toMap)  //取前20个相似的物品
    }.mapPartitions{iter =>
      val jedis = RedisClient.pool.getResource
      iter.map{ case(i,j) =>
        val key = ("II:%d").format(i)
        val builder = ItemSimilarities.newBuilder()
        j.foreach {item =>
          val itemSimilarity = ItemSimilarity.newBuilder().setItemId(item._1).setSimilarity(item._2)
          builder.addItemSimilarites(itemSimilarity)
        }
        val value = builder.build()
        println(s"key:${key},value:${value.toString}")
        jedis.set(key.getBytes(),value.toByteArray)  //进行序列化
      }
    }.count

    //save user-movieId list to redis
    //userid,movieId,rating
    //groupByKey(2):resulting RDD with into `numPartitions` partitions
    rows.map {case(i,j,k) =>
      (i,j)
    }.groupByKey(2).mapPartitions{iter =>
      val jedis = RedisClient.pool.getResource
      iter.map {case(i,j) =>
      val key = ("UI:%d").format(i)
          val builder = ItemList.newBuilder()
          j.foreach{id =>
            builder.addItemIds(id)
          }
          val value = builder.build()
          println(s"key:${key},value:${value.toString}")
          jedis.append(key.getBytes(),value.toByteArray)
      }
    }.count

    sc.stop()

  }
}
