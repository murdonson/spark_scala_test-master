package chapter3

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD


object RunRecommender {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Recommender").setMaster("local[2]"))
    val base = "hdfs://hadoop000:8088/ds/"
    /** userId artistId playcount  */
    val rawUserArtistData = sc.textFile(base + "user_artist_data_20000.txt")
    val rawArtistData = sc.textFile(base + "artist_data.txt")
    val rawArtistAlias = sc.textFile(base + "artist_alias.txt")

    preparation(rawUserArtistData, rawArtistData, rawArtistAlias)


    model(sc, rawUserArtistData, rawArtistData, rawArtistAlias)


    evaluate(sc, rawUserArtistData, rawArtistAlias)


    recommend(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
  }

  /**
    *
    * @param rawArtistData
    * @return  (id,name) rdd
    */
  def buildArtistByID(rawArtistData: RDD[String]) =
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

  /**
    *
    * @param rawArtistAlias
    * @return (badId,goodId)  map
    */
  def buildArtistAlias(rawArtistAlias: RDD[String]): Map[Int,Int] =
    rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()

  def preparation(
                   rawUserArtistData: RDD[String],
                   rawArtistData: RDD[String],
                   rawArtistAlias: RDD[String]) = {
    val userIDStats = rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
    val itemIDStats = rawUserArtistData.map(_.split(' ')(1).toDouble).stats()
    println(userIDStats)
    println(itemIDStats)

    val artistByID = buildArtistByID(rawArtistData)
    val artistAlias = buildArtistAlias(rawArtistAlias)

    val (badID, goodID) = artistAlias.head
    println(artistByID.lookup(badID) + " -> " + artistByID.lookup(goodID))
  }

  /**
    *
    * @param rawUserArtistData
    * @param bArtistAlias
    * @return  RDD[Rating(userID, finalArtistID, count)]  count当做rating用
    */
  def buildRatings(
                    rawUserArtistData: RDD[String],
                    bArtistAlias: Broadcast[Map[Int,Int]]) = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      //建立goodId bArtistAlias.value 从广播变量中取出map
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  }

  def model(
             sc: SparkContext,
             rawUserArtistData: RDD[String],
             rawArtistData: RDD[String],
             rawArtistAlias: RDD[String]): Unit = {

    // 广播一个map
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val trainData = buildRatings(rawUserArtistData, bArtistAlias).cache()

    val model = ALS.trainImplicit(trainData, 2, 2, 0.01, 1.0)

    trainData.unpersist()

    println(model.userFeatures.mapValues(_.mkString(", ")).first())

    val userID = 1000002
    val recommendations = model.recommendProducts(userID, 5)
    recommendations.foreach(println)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
      filter { case Array(user,_,_) => user.toInt == userID }

    val existingProducts = rawArtistsForUser.map { case Array(_,artist,_) => artist.toInt }.
      collect().toSet

    val artistByID = buildArtistByID(rawArtistData)

//    artistByID.filter { case (id, name) => existingProducts.contains(id) }.
//      values.collect().foreach(println)


    artistByID.filter { case (id, name) => recommendedProductIDs.contains(id) }.
      values.collect().foreach(println)

    unpersist(model)
  }

  def areaUnderCurve(
                      positiveData: RDD[Rating],
                      bAllItemIDs: Broadcast[Array[Int]],
                      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    // Make predictions for each of them, including a numeric score, and gather by user    predictFunction(positiveUserProducts).collect()  Rating(1000002,1000025,179.0)
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.
    //  positivePredictions   (1000112,CompactBuffer(Rating(1000112,1000200,23.0), Rating(1000112,1000724,104.0), Rating(1000112,1001341,9.0), Rating(1000112,1001353,5.0), Rating(1000112,10017164,0.0), Rating(1000112,1001779,600.0), Rating(1000112,1001807,12.0), Rating(1000112,1001866,13.0), Rating(1000112,1002097,37.0), Rating(1000112,1002320,2.0), Rating(1000112,1002667,0.0), Rating(1000112,1002912,12.0), Rating(1000112,1002977,1.0), Rating(1000112,10030326,0.0), Rating(1000112,10032587,0.0), Rating(1000112,10037237,0.0), Rating(1000112,1003885,1.0), Rating(1000112,1004174,1.0), Rating(1000112,10043828,0.0), Rating(1000112,1006354,85.0), Rating(1000112,1007423,0.0), Rating(1000112,1007569,15.0), Rating(1000112,1010499,4.0), Rating(1000112,1011188,0.0), Rating(1000112,1011323,1.0), Rating(1000112,1011584,0.0), Rating(1000112,1012419,0.0), Rating(1000112,1016670,68.0), Rating(1000112,1019232,0.0), Rating(1000112,1019495,0.0), Rating(1000112,1020554,0.0), Rating(1000112,1022870,0.0), Rating(1000112,1028958,0.0), Rating(1000112,1030513,0.0), Rating(1000112,1033333,0.0), Rating(1000112,1037970,85.0), Rating(1000112,1041036,0.0), Rating(1000112,1043743,0.0), Rating(1000112,1050048,0.0), Rating(1000112,1056291,0.0), Rating(1000112,1062351,4.0), Rating(1000112,1070524,0.0), Rating(1000112,1076540,0.0), Rating(1000112,1115212,0.0), Rating(1000112,1127994,0.0), Rating(1000112,1158539,0.0), Rating(1000112,1171,114.0), Rating(1000112,1217111,0.0), Rating(1000112,1233196,147.0), Rating(1000112,1233321,43.0), Rating(1000112,1239362,16.0), Rating(1000112,1240822,0.0), Rating(1000112,1240860,0.0), Rating(1000112,1245234,0.0), Rating(1000112,1250361,0.0), Rating(1000112,1258438,0.0), Rating(1000112,1261311,1.0), Rating(1000112,1275756,0.0), Rating(1000112,1284039,2.0), Rating(1000112,1348968,0.0), Rating(1000112,1411,385.0), Rating(1000112,1777,1.0), Rating(1000112,2002,0.0), Rating(1000112,2041801,0.0), Rating(1000112,6814631,15.0), Rating(1000112,2104677,0.0), Rating(1000112,2164491,0.0), Rating(1000112,2164708,0.0), Rating(1000112,2814,40.0), Rating(1000112,295,0.0), Rating(1000112,3402,1296.0), Rating(1000112,402,4.0), Rating(1000112,478,197.0), Rating(1000112,6629023,0.0), Rating(1000112,6736184,0.0), Rating(1000112,6877343,0.0), Rating(1000112,6914803,0.0), Rating(1000112,799,39.0), Rating(1000112,831,2764.0), Rating(1000112,948,48.0)))
    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      // mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        // Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about as many negative examples per user as positive.
          // Duplicates are OK
          while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          // Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)  //本来每个rdd每个元素是arraybuffer  flatmap 之后 每个元素是tuple
    // flatMap breaks the collections above down into one big set of tuples

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    // Join positive and negative by user  positivePredictions.join(negativePredictions).values  [CompactBuffer[]]
    positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>
        // AUC may be viewed as the probability that a random positive item scores
        // higher than a random negative one. Here the proportion of all positive-negative
        // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
        var correct = 0L
        var total = 0L
        // For each pairing,
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          // Count the correctly-ranked pairs
          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }
        // Return AUC: fraction of pairs ranked correctly
        correct.toDouble / total
    }.mean() // Return mean AUC over users
  }

  def predictMostListened(sc: SparkContext, train: RDD[Rating])(allData: RDD[(Int,Int)]): RDD[Rating] = {
    val bListenCount =
      sc.broadcast(train.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
    allData.map { case (user, product) =>
      Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }

  def evaluate(
                sc: SparkContext,
                rawUserArtistData: RDD[String],
                rawArtistAlias: RDD[String]): Unit = {

    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val allData = buildRatings(rawUserArtistData, bArtistAlias)

    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))

    trainData.cache()
    cvData.cache()
    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)

    // predictMostListened(sc, trainData) 是偏函数 返回一个函数
    val mostListenedAUC = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))
    println(mostListenedAUC)

    val evaluations =
      for (rank   <- Array(10,  50);
           lambda <- Array(1.0, 0.0001);
           alpha  <- Array(1.0, 40.0))
        yield {
          val model = ALS.trainImplicit(trainData, 2, 2, lambda, alpha)
          val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)
          unpersist(model)
          ((rank, lambda, alpha), auc)
        }

    evaluations.sortBy(_._2).reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
  }

  def recommend(
                 sc: SparkContext,
                 rawUserArtistData: RDD[String],
                 rawArtistData: RDD[String],
                 rawArtistAlias: RDD[String]): Unit = {

    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData, bArtistAlias).cache()
    val model = ALS.trainImplicit(allData, 50, 10, 1.0, 40.0)
    allData.unpersist()

    val userID = 2093760
    val recommendations = model.recommendProducts(userID, 5)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter { case (id, name) => recommendedProductIDs.contains(id) }.
      values.collect().foreach(println)

    val someUsers = allData.map(_.user).distinct().take(100)
    val someRecommendations = someUsers.map(userID => model.recommendProducts(userID, 5))
    someRecommendations.map(
      recs => recs.head.user + " -> " + recs.map(_.product).mkString(", ")
    ).foreach(println)

    unpersist(model)
  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    // At the moment, it's necessary to manually unpersist the RDDs inside the model
    // when done with it in order to make sure they are promptly uncached
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }




}
