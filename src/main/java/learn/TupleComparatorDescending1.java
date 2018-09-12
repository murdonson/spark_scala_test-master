package learn;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

//
//

/**
 * TupleComparatorDescending: how to sort in descending order
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 * 
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class TupleComparatorDescending1 implements Serializable, Comparator<Tuple2<String,Integer>> {

    private static final long serialVersionUID = 1287049512718728895L;

    static final TupleComparatorDescending1 INSTANCE = new TupleComparatorDescending1();

    private TupleComparatorDescending1() {
    }


    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {

        if(o1._1.compareTo(o2._1)==0)
        {
            return o1._2.compareTo(o2._2);
        }

        return o1._1.compareTo(o2._1);

    }
}
