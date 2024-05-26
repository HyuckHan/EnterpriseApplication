import scala.Tuple2;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class LabQ4 {

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("Practice").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		List<Integer> data = Arrays.asList(1, 2, 3);
		JavaRDD<Integer> rdd = sc.parallelize(data);

		List<Integer> data2 = Arrays.asList(3, 4, 5);
		JavaRDD<Integer> other = sc.parallelize(data2);

		JavaRDD<Integer> unionRdd = rdd.union(other).distinct();
		JavaRDD<Integer> intersectionRdd = rdd.intersection(other);
		JavaRDD<Integer> subtractionRdd = rdd.subtract(other);

		System.out.println( "------------------" );
		unionRdd.foreach(x -> System.out.println(x));
		System.out.println( "------------------" );
		intersectionRdd.foreach(x -> System.out.println(x));
		System.out.println( "------------------" );
		subtractionRdd.foreach(x -> System.out.println(x));
		System.out.println( "------------------" );
		sc.stop();
	}
}
