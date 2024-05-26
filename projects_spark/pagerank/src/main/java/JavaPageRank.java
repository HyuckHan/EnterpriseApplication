import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

import com.google.common.collect.Iterables;

public final class JavaPageRank {

	private static class Sum implements Function2<Double, Double, Double> {
		public Double call(Double a, Double b) {
			return a + b;
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: JavaPageRank <in-file> <out-file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession
			.builder()
			.appName("JavaPageRank")
			.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				StringTokenizer st = new StringTokenizer(s);
				String str1 = st.nextToken();
				String str2 = st.nextToken();
				return new Tuple2(str1,str2);
			}
		};
		JavaPairRDD<String, String> linkRDD = lines.mapToPair(pf).distinct();
		JavaPairRDD<String, Iterable<String>> links = linkRDD.groupByKey().cache();

		long numPages = links.count();
		double initPR = (double)1.0/ (double)numPages;
		Function<Iterable<String>,Double> f1 = new Function<Iterable<String>,Double>() {
			public Double call(Iterable<String> s) {
				return initPR;
			}
		};
		JavaPairRDD<String, Double> ranks = links.mapValues(f1);

		List<Tuple2<String, Double>> output = ranks.collect();
		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
		}

		for (int current = 0; current < Integer.parseInt(args[1]); current++) {
			// Calculates URL contributions to the rank of other URLs.
			JavaPairRDD<String, Tuple2<Iterable<String>,Double>> linkRankRDD = links.join(ranks);
			JavaRDD<Tuple2<Iterable<String>,Double>> linkRankValues = linkRankRDD.values();

			PairFlatMapFunction<Tuple2<Iterable<String>,Double>, String, Double> pfmf = 
				new PairFlatMapFunction<Tuple2<Iterable<String>,Double>, String, Double>() {
					public Iterator<Tuple2<String,Double>> call(Tuple2<Iterable<String>,Double> s) {
						int urlCount = Iterables.size(s._1);
						List<Tuple2<String, Double>> results = new ArrayList<>();
						for (String n : s._1) {
							results.add(new Tuple2<>(n, s._2() / urlCount));
						}
						return results.iterator();
					}
				};

			JavaPairRDD<String, Double> contribs = linkRankValues.flatMapToPair(pfmf);

			// Re-calculates URL ranks based on neighbor contributions.
			//ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15/numPages + sum * 0.85);
			ranks = contribs.reduceByKey((a,b) -> a+b).mapValues(sum -> 0.15/numPages + sum * 0.85);
		}

		// Collects all URL ranks and dump them to console.
		//List<Tuple2<String, Double>> output = ranks.collect();
		output = ranks.collect();
		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
		}
		//counts.saveAsTextFile(args[1]);
		spark.stop();
	}
}
