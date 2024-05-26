import scala.Tuple2;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class LabQ2Distinct {

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("Practice").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		List<Integer> data = Arrays.asList(1, 2, 3, 3);
		JavaRDD<Integer> rdd = sc.parallelize(data);

		JavaRDD<Integer> returnRdd = rdd.distinct();

		returnRdd.foreach(x -> System.out.println(x));
		sc.stop();
	}
}
