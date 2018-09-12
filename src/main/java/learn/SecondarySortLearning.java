package learn;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import java.util.SortedMap;
import java.util.TreeMap;

public class SecondarySortLearning {

    public static void main(String[] args) throws Exception {
        // STEP-1: read input parameters and validate them
        if (args.length < 2) {
            System.err.println("Usage: SecondarySortUsingCombineByKey <input> <output>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("inputPath=" + inputPath);
        String outputPath = args[1];
        System.out.println("outputPath=" + outputPath);

        // STEP-2: Connect to the Sark master by creating JavaSparkContext object
        final JavaSparkContext ctx = SparkUtil.createJavaSparkContext("local[2]","secondaryCount");

        // STEP-3: Use ctx to create JavaRDD<String>
        //  input record format: <name><,><time><,><value>
        JavaRDD<String> lines = ctx.textFile(inputPath);


        // name <value,time>
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(s -> {
            String[] tokens = s.split(",");
            Tuple2<Integer, Integer> valueTime = new Tuple2<>(Integer.parseInt(tokens[2]), Integer.parseInt(tokens[1]));
            return new Tuple2<String,Tuple2<Integer, Integer>>(tokens[0], valueTime);

        });


        Function<Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> createCombiner
                = (Tuple2<Integer, Integer> x) -> {
            Integer value = x._1;
            Integer time = x._2;
            SortedMap<Integer, Integer> map = new TreeMap<>();
            map.put(value, time);
            return map;
        };

        Function2<SortedMap<Integer, Integer>, Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> mergedValue =
                (SortedMap<Integer, Integer> map, Tuple2<Integer, Integer> x) -> {
                    Integer value = x._1;
                    Integer time = x._2;
                    map.put(value, time);
                    return map;
                };

        Function2<SortedMap<Integer, Integer>, SortedMap<Integer, Integer>, SortedMap<Integer, Integer>> mergeCombiners =
                (SortedMap<Integer, Integer> map1, SortedMap<Integer, Integer> map2) -> {
                    if (map1.size() < map2.size()) {
                        return mergeMap(map1, map2);
                    } else {
                        return mergeMap(map2, map1);
                    }

                };


        JavaPairRDD<String, SortedMap<Integer, Integer>> combined = pairs.combineByKey(createCombiner,
                mergedValue,
                mergeCombiners);


        combined.saveAsTextFile(outputPath);
        ctx.close();



    }


    private static SortedMap<Integer, Integer> mergeMap
            (SortedMap<Integer, Integer> smaller, SortedMap<Integer, Integer> larger) {
        for (Integer smallerKey : smaller.keySet()) {
            Integer valueFromLargerMap = larger.get(smallerKey);
            if (valueFromLargerMap == null) {
                larger.put(smallerKey, smaller.get(smallerKey));
            } else {
                larger.put(smallerKey, smaller.get(smallerKey) + valueFromLargerMap);
            }
        }
        return larger;

    }





}
