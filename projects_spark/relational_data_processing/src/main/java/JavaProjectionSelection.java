import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaProjectionSelection {

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.exit(1);
		}

		SparkSession spark = SparkSession
			.builder()
			.appName("JavaProjectionSelection")
			.getOrCreate();

		JavaRDD<String> emp = spark.read().textFile(args[0]).javaRDD();

		Function<String,Boolean> selectFunc = new Function<String,Boolean>() {
			public Boolean call(String s) {
				StringTokenizer st = new StringTokenizer(s, "|");
				String id = st.nextToken().trim();
				String dept = st.nextToken().trim();
				String t_salary = st.nextToken().trim();
				String info = st.nextToken().trim();
				int salary = Integer.parseInt( t_salary );
				return salary >= 3500000;
			}
		};
		JavaRDD<String> sEmp = emp.filter(selectFunc);

		Function<String,String> projectFunc = new Function<String,String>() {
			public String call(String s) {
				StringTokenizer st = new StringTokenizer(s, "|");
				String id = st.nextToken().trim();
				String dept = st.nextToken().trim();
				String salary = st.nextToken().trim();
				String info = st.nextToken().trim();
				return id + "|" + salary;
			}
		};
		JavaRDD<String> spEmp = sEmp.map(projectFunc);

		List<String> output = spEmp.collect();
		for (String record : output) {
			System.out.println(record);
		}

		spark.stop();
	}
}
