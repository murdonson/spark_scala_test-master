package learn;

import org.apache.spark.Partitioner;
import scala.Tuple2;

public class MyPartitioner extends Partitioner {

    private final int numPartitions;

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {

        if(key==null)
            return 0;
        else if(key instanceof Tuple2)
        {
            Tuple2<String,Integer> tuple2= (Tuple2<String, Integer>) key;
            return Math.abs(tuple2._1.hashCode()%numPartitions);
        }
        else{
            return Math.abs(key.hashCode()%numPartitions);
        }
    }


    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(this==obj)
        {
            return true;
        }
        if(obj==null)
        {
            return false;
        }

        if(!(obj instanceof MyPartitioner))
        {
            return false;
        }

        MyPartitioner other= (MyPartitioner) obj;
        if(numPartitions!=other.numPartitions)
        {
            return false;
        }

        return true;


    }

    public MyPartitioner(int i) {
        assert (i>0);
        this.numPartitions=i;

    }
}
