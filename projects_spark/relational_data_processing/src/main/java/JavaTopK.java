import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

public final class JavaTopK {

	static class EmpComparator implements Comparator<Emp>, Serializable {
		public int compare(Emp o1, Emp o2) {
			if(o2.salary == o1.salary) return 0;
			else if(o2.salary < o1.salary) return -1;
			else return 1;
		}
	}

	static class Emp implements Serializable{
		public String id;
		public int salary;
		public String dept;
		public String info;

		public String toString() {
			return id + "|" + dept + "|" + salary + "|" + info;
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.exit(1);
		}

		int k = 3;

		SparkSession spark = SparkSession
			.builder()
			.appName("JavaTopK")
			.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		Function<String,Emp> mapFunc = new Function<String,Emp>() {
			public Emp call(String s) {
				StringTokenizer st = new StringTokenizer(s, "|");
				Emp emp = new Emp();
				emp.id = st.nextToken().trim();
				emp.dept = st.nextToken().trim();
				emp.salary = Integer.parseInt(st.nextToken().trim());
				emp.info = st.nextToken().trim();
				return emp;
			}
		};
		JavaRDD<Emp> empRDD = lines.map(mapFunc);

		List<Emp> topK = empRDD.takeOrdered(k, new EmpComparator());
		for(int i=0; i<topK.size(); i++) {
			Emp emp = topK.get(i);
			System.out.println(emp);
		}

		spark.stop();
	}
}
