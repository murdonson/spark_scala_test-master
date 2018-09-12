package learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class RepartitionDemo {

    public static void main(String[] args) {

        String inputPath="input.txt";
        String outputPaht="output";

        SparkConf conf=new SparkConf().setAppName("dj").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath);


        JavaPairRDD<Tuple2<String,Integer>, Integer> pairRDD = input.mapToPair((String line) -> {
            String[] tokens = line.split(" ");
            Integer value = Integer.parseInt(tokens[1]);

            Tuple2<String, Integer> key = new Tuple2<>(tokens[0], value);
            return new Tuple2<Tuple2<String, Integer>, Integer>(key, value);
        });


        JavaPairRDD<Tuple2<String, Integer>, Integer> sorted = pairRDD.repartitionAndSortWithinPartitions(new MyPartitioner(2), TupleComparatorDescending1.INSTANCE);


        JavaPairRDD<String, Integer> result = sorted.mapToPair(line -> {
            String key = line._1._1;
            Integer value = line._2;
            return new Tuple2<String, Integer>(key, value);
        });


        result.saveAsTextFile(outputPaht);

        sc.close();


    }


}
