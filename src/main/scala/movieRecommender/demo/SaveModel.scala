package movieRecommender.demo

import movieRecommender.ModelTest.TestModel.computeRmse
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SaveModel {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("TestModel")
      .setMaster("local[4]")
      .set("spark.local.dir","D:\\spark")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("d://sparkCheck")

    val trainlines=sc.textFile("data/train/train.txt/*")
    val train =trainlines.map(line=> {
        val newLine=line.substring(1,line.length-1)
        val fields = newLine.split(",")
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    ).cache()

    val testLines=sc.textFile("data/text/text.txt/*")
    val test =testLines.map(line=> {
      val newLine=line.substring(1,line.length-1)
      val fields = newLine.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }
    ).cache()

    val valLines=sc.textFile("data/validation/validation.txt/*")
    val validation =valLines.map(line=> {
      val newLine=line.substring(1,line.length-1)
      val fields = newLine.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }
    ).cache()

    train.checkpoint()
    validation.checkpoint()
    test.checkpoint()

    val ranks=List(3,4,5)
    val numIters=List(3,4,5)
    val lambdas=List(0.2,0.3)

    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

    for(rank<-ranks;lambda<-lambdas;numIter<-numIters){
      val model = ALS.train(train, rank, numIter, lambda)
      //训练模型
      //model.save("")

      val validationRmse = computeRmse(model, validation)

      println(validationRmse + " " + rank + " " + numIter + " " + lambda)

      if(validationRmse<bestValidationRmse)
      {
        bestModel=Some(model)
        bestValidationRmse=validationRmse
        bestRank=rank
        bestLambda=lambda
        bestNumIter=numIter
      }
    }

    val testRmse = computeRmse(bestModel.get, test)
    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")
    ALS.train(train, bestRank, bestNumIter, bestLambda).save(sc, "D://outmodel")


  }




  //RMSE计算
  def computeRmse(model: MatrixFactorizationModel, value: RDD[Rating]): Double ={
    //创建用户id-影片id RDD
    val usersProducts = value.map{case Rating(user, product,rating) => (user, product)}

    //创建（用户id,影片id） - 预测评分RDD
    val predictions = model.predict(usersProducts).map{
      case Rating(user, product, rating) => ((user, product), rating)
    }

    //创建用户-影片实际评分RDD,并将其与上面创建的预测评分RDD join起来
    val ratingsAndPredictions = value.map{
      case Rating(user, product, rating) => ((user, product),rating)
    }.join(predictions)

    //创建预测评分-实际评分RDD
    val predictedAndTrue = ratingsAndPredictions.map{case((user, product),(actual,
    predicted)) => (actual, predicted)}

    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    regressionMetrics.rootMeanSquaredError
  }

}
