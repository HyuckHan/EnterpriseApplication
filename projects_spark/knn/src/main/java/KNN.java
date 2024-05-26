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
import java.util.Map;
import java.util.Comparator;
import java.util.Collections;


public final class KNN {

	static class TupleComparator implements Comparator<Tuple2<Double,String>>, Serializable {
		public int compare(Tuple2<Double,String> o1, Tuple2<Double,String> o2) {
			if(o2._1 == o1._1) return 0;
			else if(o2._1 > o1._1) return -1;
			else return 1;
		}
	}

	public static ArrayList<Map.Entry<?, Integer>> sortValue(Hashtable<?, Integer> t){
		ArrayList<Map.Entry<?, Integer>> l = new ArrayList(t.entrySet());
		Collections.sort(l, new Comparator<Map.Entry<?, Integer>>(){
			public int compare(Map.Entry<?, Integer> o1, Map.Entry<?, Integer> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}});
		return l;
	}


	static class Point implements java.io.Serializable{
		double mileage;
		double videoGameRate;
		double iceCream;
		String label;

		public Point() {
			this(0,0,0,"");
		}
		public Point(double mileage, double videoGameRate, double iceCream, String label) {
			this.mileage = mileage;
			this.videoGameRate = videoGameRate;
			this.iceCream = iceCream;
			this.label = label;
		}

		double distance(Point other) {
			double dist = (mileage-other.mileage)* (mileage-other.mileage);
			dist = dist + (videoGameRate-other.videoGameRate)*(videoGameRate-other.videoGameRate);
			dist = dist + (iceCream-other.iceCream)*(iceCream-other.iceCream);
			return Math.sqrt( dist );
		}

		public String toString() {
			return "mileage="+mileage+",videoGameRate="+videoGameRate+",iceCream="+iceCream + ",label="+label;
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


		int k = 5;
		Point query = new Point();
		query.mileage = 40900;
		query.videoGameRate = 8.3;
		query.iceCream = 0.9;

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		Function<String, Point> f1 = new Function<String, Point>() {
			public Point call(String s) {
				String[] val = s.split("\t");
				Point p = new Point();
				p.mileage = Double.parseDouble(val[0]);
				p.videoGameRate = Double.parseDouble(val[1]);
				p.iceCream = Double.parseDouble(val[2]);
				p.label = val[3];
				return p;
			}
		};
		JavaRDD<Point> pointRDD = lines.map(f1);

		Function2<Point,Point,Point> f2 = new Function2<Point,Point,Point>() {
			public Point call(Point p1, Point p2) {
				Point p = new Point();
				p.mileage = Math.min( p1.mileage, p2.mileage);
				p.videoGameRate = Math.min( p1.videoGameRate, p2.videoGameRate);
				p.iceCream = Math.min( p1.iceCream, p2.iceCream);
				return p;
			}
		};

		Function2<Point,Point,Point> f3 = new Function2<Point,Point,Point>() {
			public Point call(Point p1, Point p2) {
				Point p = new Point();
				p.mileage = Math.max( p1.mileage, p2.mileage);
				p.videoGameRate = Math.max( p1.videoGameRate, p2.videoGameRate);
				p.iceCream = Math.max( p1.iceCream, p2.iceCream);
				return p;
			}
		};

		Point min = pointRDD.reduce(f2);
		Point max = pointRDD.reduce(f3);

		Broadcast<Point> broadcastMinPoint = spark.sparkContext().broadcast(min, scala.reflect.ClassTag$.MODULE$.apply(min.getClass()));
		Broadcast<Point> broadcastMaxPoint = spark.sparkContext().broadcast(max, scala.reflect.ClassTag$.MODULE$.apply(max.getClass()));

		Function<Point, Point> f4 = new Function<Point, Point>() {
			public Point call(Point p) {
				Point minPoint = broadcastMinPoint.value();
				Point maxPoint = broadcastMaxPoint.value();
				p.mileage = (p.mileage-minPoint.mileage)/(maxPoint.mileage-minPoint.mileage);
				p.videoGameRate = (p.videoGameRate-minPoint.videoGameRate)/(maxPoint.videoGameRate-minPoint.videoGameRate);
				p.iceCream = (p.iceCream-minPoint.iceCream)/(maxPoint.iceCream-minPoint.iceCream);
				return p;
			}
		};
		JavaRDD<Point> normPointRDD = pointRDD.map(f4);

		query.mileage = (query.mileage-min.mileage)/(max.mileage-min.mileage);
		query.videoGameRate = (query.videoGameRate-min.videoGameRate)/(max.videoGameRate-min.videoGameRate);
		query.iceCream = (query.iceCream-min.iceCream)/(max.iceCream-min.iceCream);

		Broadcast<Point> broadcastQueryPoint = spark.sparkContext().broadcast(query, scala.reflect.ClassTag$.MODULE$.apply(query.getClass()));

		PairFunction<Point, Double, String> pf = new PairFunction<Point, Double, String>() {
			public Tuple2<Double, String> call(Point p) {
				Point queryPoint = broadcastQueryPoint.value();
				double dist = p.distance(queryPoint);
				return new Tuple2(dist, p.label);
			}
		};

		JavaPairRDD<Double,String> distanceLabelRDD = normPointRDD.mapToPair(pf);

		List<Tuple2<Double,String>> topK = distanceLabelRDD.takeOrdered(k, new TupleComparator());
		Hashtable<String,Integer> labelTable = new Hashtable<String,Integer>();
		for(int i=0; i<topK.size(); i++) { 
			Tuple2<Double,String> t2 = topK.get(i);
			int val = labelTable.getOrDefault(t2._2,0)+1;
			labelTable.put(t2._2,val);
		}

		ArrayList<Map.Entry<?, Integer>> sortedLableList = sortValue(labelTable);
		for(int i=0; i<sortedLableList.size(); i++) { 
			System.out.println( sortedLableList.get(i).getKey() + " " + sortedLableList.get(i).getValue());
		}

		broadcastMinPoint.destroy();
		broadcastMaxPoint.destroy();
		broadcastQueryPoint.destroy();

		spark.stop();
	}
}
