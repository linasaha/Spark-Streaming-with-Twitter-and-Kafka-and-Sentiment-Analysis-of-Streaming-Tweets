import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.regression.{DecisionTreeRegressor, GBTRegressor}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD

object part2
{
  def main(args: Array[String]): Unit = {
    var path="./tweets.csv"
    System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop")
    val spark = SparkSession.builder.appName("Twitter Processing and Classification").master("local").getOrCreate()
    //val spark = SparkSession.builder.appName("Airline sentiment Classification").getOrCreate()

    val sc = spark.sparkContext
    var rdd :RDD[String]= null
    val sql = spark.sqlContext

    import spark.implicits._
    //var dataframe=spark.read.option("header","true").csv(args(0))
    var dataframe=spark.read.option("header","true").csv(path)
    dataframe=dataframe.filter($"text".isNotNull)

    val tokenizer=new Tokenizer().setInputCol("text").setOutputCol("words")
    val withoutStopWords = new StopWordsRemover().setInputCol(tokenizer.getOutputCol).setOutputCol("without_stopwords")
    val hashing_TF = new HashingTF().setInputCol(withoutStopWords.getOutputCol).setOutputCol("rawFeatures").setNumFeatures(20)
    val index = new StringIndexer().setInputCol("airline_sentiment").setOutputCol("label")

    val Array(train, test) = dataframe.randomSplit(Array(0.8, 0.2))

    val randomForest = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("rawFeatures").setNumTrees(10)
    val logisticRegression = new LogisticRegression().setMaxIter(10).setLabelCol("label").setFeaturesCol("rawFeatures")
//    val gradientBoostedTree = new GBTRegressor().setLabelCol("label").setFeaturesCol("rawFeatures").setMaxIter(10).setMaxDepth(10)
//    val decisionTree = new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("rawFeatures").setMaxDepth(10)

    val pipe1 = new Pipeline().setStages(Array(tokenizer, withoutStopWords, hashing_TF, index, randomForest))
    val pipe2 = new Pipeline().setStages(Array(tokenizer, withoutStopWords, hashing_TF, index, logisticRegression))
    //val pipe3 = new Pipeline().setStages(Array(tokenizer, withoutStopWords, hashing_TF, index, gradientBoostedTree))
    //val pipe4 = new Pipeline().setStages(Array(tokenizer, withoutStopWords, hashing_TF, index, decisionTree))

    val grid_randomForest = new ParamGridBuilder().addGrid(hashing_TF.numFeatures, Array(10, 100, 1000)).addGrid(randomForest.maxDepth, Array(20,30)).build()
    val grid_logisticRegression = new ParamGridBuilder().addGrid(hashing_TF.numFeatures, Array(10, 100, 1000)).addGrid(logisticRegression.regParam, Array(0.1, 0.01)).build()
    //val grid_gradientBoostedTree = new ParamGridBuilder().addGrid(hashing_TF.numFeatures, Array(10, 100, 1000)).addGrid(gradientBoostedTree.maxDepth, Array(5, 10)).addGrid(gradientBoostedTree.maxIter, Array(10, 100)).build()
    //val grid_decisionTree = new ParamGridBuilder().addGrid(hashing_TF.numFeatures, Array(10, 100, 1000)).addGrid(decisionTree.maxDepth, Array(5, 10)).build()

    val eval = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")

    val crossVal_randomForest = new CrossValidator().setEstimator(pipe1).setEvaluator(eval).setEstimatorParamMaps(grid_randomForest).setNumFolds(2)
    val cross_randomForest = crossVal_randomForest.fit(train)
    val cross_randomForest_transform = cross_randomForest.transform(test)

    val crossVal_logisticRegression = new CrossValidator().setEstimator(pipe2).setEvaluator(eval).setEstimatorParamMaps(grid_logisticRegression).setNumFolds(2)
    val cross_logisticRegression = crossVal_logisticRegression.fit(train)
    val cross_logisticRegression_transform = cross_logisticRegression.transform(test)

//    val crossVal_gradientBoostedTree = new CrossValidator().setEstimator(pipe3).setEvaluator(eval).setEstimatorParamMaps(grid_gradientBoostedTree).setNumFolds(2)
//    val cross_gradientBoostedTree  = crossVal_gradientBoostedTree.fit(train)
//    val cross_gradientBoostedTree_transform = cross_gradientBoostedTree.transform(test)
//
//    val crossVal_decisionTree = new CrossValidator().setEstimator(pipe3).setEvaluator(eval).setEstimatorParamMaps(grid_decisionTree).setNumFolds(2)
//    val cross_decisionTree = crossVal_decisionTree.fit(train)
//    val cross_decisionTree_transform = cross_decisionTree.transform(test)

    var accuracy_randomForest = 0.0
    var precision_randomForest = 0.0
    var recall_randomForest = 0.0
    var score_randomForest = 0.0

    val prediction_randomForest = cross_randomForest_transform.select("prediction", "label").rdd.map{case Row(prediction: Double, label: Double) => (prediction, label)}
    val metric_randomForest = new MulticlassMetrics(prediction_randomForest)
    accuracy_randomForest = metric_randomForest.accuracy
    precision_randomForest = metric_randomForest.weightedPrecision
    recall_randomForest = metric_randomForest.weightedRecall
    score_randomForest = metric_randomForest.weightedFMeasure

    var accuracy_logisticRegression = 0.0
    var precision_logisticRegression = 0.0
    var recall_logisticRegression = 0.0
    var score_logisticRegression = 0.0

    val prediction_logisticRegression = cross_logisticRegression_transform.select("prediction", "label").rdd.map{case Row(prediction: Double, label: Double) => (prediction, label)}
    val metric_logisticRegression = new MulticlassMetrics(prediction_logisticRegression)
    accuracy_logisticRegression = metric_logisticRegression.accuracy
    precision_logisticRegression = metric_logisticRegression.weightedPrecision
    recall_logisticRegression = metric_logisticRegression.weightedRecall
    score_logisticRegression = metric_logisticRegression.weightedFMeasure

//    var accuracy_gradientBoostedTree = 0.0
//    var precision_gradientBoostedTree = 0.0
//    var recall_gradientBoostedTree = 0.0
//    var score_gradientBoostedTree = 0.0
//
//    val prediction_gradientBoostedTree = cross_gradientBoostedTree_transform.select("prediction", "label").rdd.map{case Row(prediction: Double, label: Double) => (prediction, label)}
//    val metric_gradientBoostedTree = new MulticlassMetrics(prediction_gradientBoostedTree)
//    accuracy_gradientBoostedTree = metric_gradientBoostedTree .accuracy
//    precision_gradientBoostedTree = metric_gradientBoostedTree.weightedPrecision
//    recall_gradientBoostedTree = metric_gradientBoostedTree.weightedRecall
//    score_gradientBoostedTree = metric_gradientBoostedTree.weightedFMeasure

//    var accuracy_decisionTree = 0.0
//    var precision_decisionTree = 0.0
//    var recall_decisionTree = 0.0
//    var score_decisionTree = 0.0
//
//    val prediction_decisionTree = cross_decisionTree_transform.select("prediction", "label").rdd.map{case Row(prediction: Double, label: Double) => (prediction, label)}
//    val metric_decisionTree = new MulticlassMetrics(prediction_decisionTree)
//    accuracy_decisionTree = metric_decisionTree.accuracy
//    precision_decisionTree = metric_decisionTree.weightedPrecision
//    recall_decisionTree = metric_decisionTree.weightedRecall
//    score_decisionTree = metric_decisionTree.weightedFMeasure


    var output = ""
    output+= " Random Forest:\n"
    output+= "    Accuracy - " + accuracy_randomForest + "\n"
    output+= "    Precision - "  + precision_randomForest + "\n"
    output+= "    Recall - " + recall_randomForest + "\n"
    output+= "    Score - " + score_randomForest + "\n\n"

    output += " Logistic Regression::\n"
    output += "    Accuracy - " + accuracy_logisticRegression + "\n"
    output += "    Precision - "  + precision_logisticRegression + "\n"
    output += "    Recall - " + recall_logisticRegression + "\n"
    output += "    Score - " + score_logisticRegression + "\n\n\n"

//    output += " Gradient Boosted Tree:\n"
//    output += "    Accuracy- " + accuracy_gradientBoostedTree + "\n"
//    output += "    Precision - "  + precision_gradientBoostedTree + "\n"
//    output += "    Recall - " + recall_gradientBoostedTree + "\n"
//    output += "    Score - " + score_gradientBoostedTree + "\n\n"
//
//    output += " Decision Tree:\n"
//    output += "    Accuracy- " + accuracy_decisionTree + "\n"
//    output += "    Precision - "  + precision_decisionTree + "\n"
//    output += "    Recall - " + recall_decisionTree + "\n"
//    output += "    Score - " + score_decisionTree + "\n\n"

    println("acccuracy of random forest is:" + accuracy_randomForest)
//    println("acccuracy of gradient boosted tree is:" + accuracy_gradientBoostedTree)
//    println("accuracy of decision tree is:"+accuracy_decisionTree)
    println("accuracy of logistic regression is:"+accuracy_logisticRegression)

    output+="\n"+"The best accuracy is of  "
    if(accuracy_randomForest>accuracy_logisticRegression)
      output += accuracy_randomForest + "\n"
    else
      output += accuracy_logisticRegression + "\n"

    rdd = sc.parallelize(List(output))
    //rdd.coalesce(1,true).saveAsTextFile(args(1))
    rdd.coalesce(1,true).saveAsTextFile("./airport")
    sc.stop()
  }
}
