import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.HashMap;

public final class KMeansBroadcast {

	private static <T> scala.reflect.ClassTag<T> classTag(Class<T> clazz) {
		return scala.reflect.ClassManifestFactory.fromClass(clazz);
	}

	static class Point implements java.io.Serializable{
		double x;
		double y;

		public Point() {
			this(0,0);
		}
		public Point(double x, double y) {
			this.x = x;
			this.y = y;
		}

		double distance(Point other) {
			double dist = (x-other.x)* (x-other.x) + (y-other.y)*(y-other.y);
			return Math.sqrt( dist );
		}

		public String toString() {
			return "x="+x+",y="+y;
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: JavaMatrixMul <file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession
			.builder()
			.appName("KMeans")
			.getOrCreate();


		int k = 2;
		Point []points_broadcast = new Point[k];
		points_broadcast[0] = new Point(2,2);
		points_broadcast[1] = new Point(6,6);

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		Function<String, Point> f1 = new Function<String, Point>() {
			public Point call(String s) {
				String[] val = s.split(",");
				Point p = new Point();
				p.x = Double.parseDouble(val[0]);
				p.y = Double.parseDouble(val[1]);
				return p;
			}
		};
		JavaRDD<Point> pointRDD = lines.map(f1); 
		JavaPairRDD<Integer, Point> mean_point = null;

		for(int j = 0 ; j < 2 ; j++ ) {
			Broadcast<Point[]> broadcastVar = spark.sparkContext().broadcast(points_broadcast, scala.reflect.ClassTag$.MODULE$.apply(points_broadcast.getClass()));

			PairFunction<Point, Integer, Tuple2<Integer,Point>> pf1 = new PairFunction<Point, Integer, Tuple2<Integer,Point>>() {
				public Tuple2<Integer, Tuple2<Integer,Point>> call(Point p) {
					Point[] points = broadcastVar.value();
					int index = 0;
					double minDist = p.distance(points[0]);
					for (int i = 1; i < k; i++){
						double dist = p.distance(points[i]);
						if(dist<minDist) {
							index = i;
							minDist = dist;
						}
					}
					return new Tuple2<Integer,Tuple2<Integer,Point>>(index,new Tuple2(1,p));
				}
			};
			JavaPairRDD<Integer, Tuple2<Integer,Point>> clustered = pointRDD.mapToPair(pf1);

			Function2<Tuple2<Integer,Point>, Tuple2<Integer,Point>, Tuple2<Integer,Point>> f2 = 
				new Function2<Tuple2<Integer,Point>, Tuple2<Integer,Point>, Tuple2<Integer,Point>>()  {

					public Tuple2<Integer,Point> call(Tuple2<Integer,Point> p1, Tuple2<Integer,Point> p2) {
						int nr_points = p1._1 + p2._1;
						Point newPoint = new Point();
						newPoint.x = p1._2.x + p2._2.x;
						newPoint.y = p1._2.y + p2._2.y;
						return new Tuple2(nr_points,newPoint);
					}
				};
			JavaPairRDD<Integer, Tuple2<Integer,Point>> agg_values = clustered.reduceByKey(f2);

			Function<Tuple2<Integer,Point>, Point> f3 = new Function<Tuple2<Integer,Point>, Point>() {
				public Point call(Tuple2<Integer,Point> t) {
					Point p = new Point();
					p.x = t._2.x / t._1;
					p.y = t._2.y / t._1;
					return p;
				}
			}; 
			mean_point = agg_values.mapValues(f3);

			List<Tuple2<Integer,Point>> pointList = mean_point.collect();
			for(int i=0; i<pointList.size(); i++) {
				Tuple2<Integer,Point> tuple = pointList.get(i);
				int index = tuple._1;
				points_broadcast[index].x = tuple._2.x;
				points_broadcast[index].y = tuple._2.y;
				System.out.println(tuple);
			}

			broadcastVar.destroy();
		}

		mean_point.saveAsTextFile(args[args.length - 1]);
		spark.stop();
	}
}
