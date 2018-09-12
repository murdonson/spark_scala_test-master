package learn;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class Top2NonUnique {

    public static void main(String[] args) throws Exception {

        final int N=2;

        final JavaSparkContext ctx = SparkUtil.createJavaSparkContext("local[2]","secondaryCount");

        // STEP-3: Use ctx to create JavaRDD<String>
        //  input record format: <name><,><time><,><value>
        JavaRDD<String> lines = ctx.textFile("utl.txt");

        Broadcast<Integer> topN = (Broadcast<Integer>) ctx.broadcast(N);

        JavaRDD<String> rdd = lines.coalesce(9);



        JavaPairRDD<String, Integer> pair = rdd.mapToPair((String line) -> {
            String[] tokens = line.split(" ");
            String key = tokens[0];
            Integer value = Integer.parseInt(tokens[1]);
            return new Tuple2<String, Integer>(key, value);
        });


        JavaPairRDD<String, Integer> uniqueKeys = pair.reduceByKey((Integer i1, Integer i2) ->
                i1 + i2
        );


        JavaRDD<TreeMap<Integer, String>> partitions = (JavaRDD<TreeMap<Integer, String>>) uniqueKeys.mapPartitions((Iterator<Tuple2<String, Integer>> iter) -> {
            final int N1 = topN.value();
            TreeMap<Integer, String> localTopN = new TreeMap<>();
            while (iter.hasNext()) {
                Tuple2<String, Integer> tuple = iter.next();
                localTopN.put(tuple._2, tuple._1);
                if (localTopN.size() > N1) {
                    localTopN.remove(localTopN.firstKey());
                }
            }

            return Collections.singletonList(localTopN).iterator();
        });


        TreeMap<Integer, String> finalTopN = new TreeMap<>();
        List<TreeMap<Integer, String>> allTopN = partitions.collect();

        for (TreeMap<Integer, String> localTopN : allTopN) {
            for (Map.Entry<Integer, String> entry : localTopN.entrySet()) {
                finalTopN.put(entry.getKey(),entry.getValue());
                if(finalTopN.size()>N)
                {
                    finalTopN.remove(finalTopN.firstKey());
                }
            }
        }



    }


}
