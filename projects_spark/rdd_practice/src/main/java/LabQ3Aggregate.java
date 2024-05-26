import scala.Tuple2;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

class AvgCount implements Serializable {
	public int total;
	public int count;

	public AvgCount(int a, int b) {
		total = a;
		count = b;
	}
}

public class LabQ3Aggregate {

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("Practice").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		List<Integer> data = Arrays.asList(1, 2, 3, 3);
		JavaRDD<Integer> rdd = sc.parallelize(data);

		class Add implements Function2<AvgCount,Integer,AvgCount> {
			public AvgCount call(AvgCount x, Integer y) {
				x.total = x.total + y;
				x.count = x.count + 1;
				return x;
			}
		}

		class Combine implements Function2<AvgCount,AvgCount,AvgCount> {
			public AvgCount call(AvgCount x, AvgCount y) {
				x.total = x.total + y.total;
				x.count = x.count + y.count;
				return x;
			}
		}
		AvgCount final_value = 
			rdd.aggregate( new AvgCount(0,0), new Add(), new Combine() );

		System.out.println((double)final_value.total/(double)final_value.count);
		sc.stop();
	}
}
