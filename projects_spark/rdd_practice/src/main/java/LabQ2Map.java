import scala.Tuple2;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class LabQ2Map {

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("Practice").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		List<Integer> data = Arrays.asList(1, 2, 3, 3);
		JavaRDD<Integer> rdd = sc.parallelize(data);

		//rdd -> map( x -> x+1 )
		
		class AddOne implements Function<Integer,Integer> {
			public Integer call(Integer v) {
				return v+1;
			}
		}
		JavaRDD<Integer> returnRdd = rdd.map( new AddOne() );

		returnRdd.foreach(x -> System.out.println(x));
		sc.stop();
	}
}
