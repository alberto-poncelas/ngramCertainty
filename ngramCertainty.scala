import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io._
import com.typesafe.config.{ Config, ConfigFactory }
import scala.util.Random
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel



object NgramCertainty {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("NgramCertainty")
    val sc = new SparkContext(conf)

	val pathFrom = args(0)
	val pathTo = args(1)
	val pathTestSet = args(2)
	val outputPath = args(3)

	val NGRAM_ORDER = 3

	import sys.process._
	def saveRDD (rdd:RDD[String],outputPath:String) : Unit = {
	val outputFilename="output"
	val tmpOutputPath = outputPath+"/tmp"
	val foo1 = ("rm -r "+tmpOutputPath !)
	rdd.repartition(1).saveAsTextFile(tmpOutputPath)
	val cmnd = "ls "+tmpOutputPath
	val filename = (cmnd #| "grep part"!!).replace("\n","")
	val mcCmd = "mv "+tmpOutputPath+"/"+filename+"  "+outputPath+"/"+outputFilename
	val foo2 = (mcCmd !)
	val foo3 = ("rm -r "+tmpOutputPath ! )
	}

	// get an array of the ngrams of a sentence (s:sentence, n size of ngrams)
	def getNgrams(s:String,n: Int) : Array[String] = {
	return (1 to n).toArray.flatMap(i=> s.split(' ').sliding(i) ).map(m => m.mkString(" ") ).toSet.toArray
	}

	def getFeatSentFilt(sentence:String,feats:Set[String]):Set[(String)] = feats.filter(x=>(" "+sentence+" ").contains(" "+x+" ") )

	//Load files
	val fileFrom = sc.textFile(pathFrom).zipWithIndex.map(_.swap)
	val fileTo = sc.textFile(pathTo).zipWithIndex.map(_.swap)
	val pairedText = fileFrom.join(fileTo).map(x=> (x._2._1,x._2._2,x._1) )
	val testSet = sc.textFile(pathTestSet)

	//obtain the n-grams
	val featTest = sc.broadcast(testSet.flatMap(x=>getNgrams(x,NGRAM_ORDER)).distinct().collect().toSet)

	val featSent = pairedText.map{case(from,to,idx)=>{ // (trg, Set of features)
	(to,getFeatSentFilt(from,featTest.value))
	}}.flatMap{case(sentTo,setFeat)=>{
	setFeat.map(feat=>(feat,sentTo))
	}}

	def reduceFeatSent(featSent:(String,String) ) : List[((String, String), Int)] = featSent._2.split(" ").map(x=>(featSent._1,x)).groupBy(identity).mapValues(_.size).toList
	val featTrgword = featSent.flatMap(featSentTo=>reduceFeatSent(featSentTo)  )

	//Count the number of words (in the target side) associated to the feature
	val wordCountByKey = featTrgword.map(x=>(x._1,x._2.toDouble)).reduceByKey(_+_).map(x=>(x._1._1,x._1._2,x._2))
	val keyCountMap = wordCountByKey.map(x=>(x._1,x._3)).reduceByKey(_+_).collectAsMap()

	//Compute the probability of the target words associated to the feature
	val keyCount = sc.broadcast(keyCountMap)
	val featWordCount = wordCountByKey.map(x=>(x._1,x._3,keyCount.value.getOrElse(x._1,1.0) )).map(x=>(x._1,x._2/x._3))

	//Compute the entropy
	val featEntr = featWordCount.map(x=>(x._1,x._2*math.log(x._2))).reduceByKey(_+_).map(x=>(x._1,-1*x._2))

	//Normalize the entropy
	val resNorm = featEntr.
	map(x=>(x._1,x._2,keyCount.value.getOrElse(x._1,0.0))).
	map(x=>(x._1,x._2/math.log(x._3)))

	//Output to file
	saveRDD(resNorm.map(x=>(x._1, math.min(x._2,0.999999999999999) )).map(x=>(x._1+"|"+x._2)),outputPath)
}}

