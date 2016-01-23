package edu.asu.cse512;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.*;

class BaseRectangle implements Serializable {

	private static final long serialVersionUID = 1L;
	private int UniqueId;
	private double CoordinateX1;
	private double CoordinateY1;
	private double CoordinateX2;
	private double CoordinateY2;

	public double GetCoordinateX1() {
		return this.CoordinateX1;
	}

	public double GetCoordinateY1() {
		return CoordinateY1;
	}

	public double GetCoordinateX2() {
		return CoordinateX2;
	}

	public double GetCoordinateY2() {
		return CoordinateY2;
	}

	public BaseRectangle(double x1, double y1, double x2, double y2) {
		this.UniqueId = hashCode();
		this.CoordinateX1 = x1;
		this.CoordinateY1 = y1;
		this.CoordinateX2 = x2;
		this.CoordinateY2 = y2;
	}

	public int GetUniqueId() {
		return this.UniqueId;
	}
}

class DoublePair {
	  final double x;
	  final double y;
	  DoublePair(double x, double y) {
		  this.x=x;
		  this.y=y;
	  }
};
class DoublePairCompare implements Comparator<DoublePair> {
	  public int compare(DoublePair pr1, DoublePair pr2) {
		  int firstResult = (new Double (pr1.x)).compareTo(new Double(pr2.x));
		    if (firstResult == 0) { 
		    	return (new Double (pr1.y)).compareTo(new Double(pr2.y));
		    } else {
		    	return firstResult;
		    }
	  }
}
public class Union {
	static int[] rectangleclass;
	public static int FindClass(int x) {
		int x2 = x;
		while(true) {
			if (rectangleclass[x] == x) {
				break;
			}
			x = rectangleclass[x];
		}
		while(true) {
			if (rectangleclass[x2] == x2) {
				break;
			}
			int next = rectangleclass[x2];
			rectangleclass[x2] = x;
			x2 = next;
		}
		return x;
	}
    public static void RectangleUnionMain( JavaSparkContext sc, String Input, String Output ) {
    	JavaRDD<String> RDDPointString = sc.textFile(Input);
    	JavaRDD<BaseRectangle> rectangles = RDDPointString.map(new Function<String, BaseRectangle>() {
    		private static final long serialVersionUID = 2L;
    		public BaseRectangle call(String line) {
    			double point1x = 0.0, point1y = 0.0, point2x = 0.0, point2y = 0.0;
    			String[] RectangleArray = line.split(",");
    			point1x = Double.parseDouble(RectangleArray[0]);
    			point1y = Double.parseDouble(RectangleArray[1]);
    			point2x = Double.parseDouble(RectangleArray[2]);
    			point2y = Double.parseDouble(RectangleArray[3]);
    			BaseRectangle rectangle = new BaseRectangle(point1x, point1y, point2x, point2y);
    			return rectangle;
    		}
    	});
    	List<BaseRectangle> rectanglelist = rectangles.collect();
    	int NumberOfRectangles = rectanglelist.size();
    	int i = 0;
    	rectangleclass = new int[NumberOfRectangles];
		while (i < NumberOfRectangles) {
			rectangleclass[i] = i;
			i++;
		}
		i = 0;
		while (i < NumberOfRectangles) {
			BaseRectangle outerrectangle = rectanglelist.get(i);
			for (int j = i + 1; j < NumberOfRectangles; j++) {
				BaseRectangle innerrectangle = rectanglelist.get(j);
				double innerrectanglex1 = innerrectangle.GetCoordinateX1();
				double innerrectanglex2 = innerrectangle.GetCoordinateX2();
				double innerrectangley1 = innerrectangle.GetCoordinateY1();
				double innerrectangley2 = innerrectangle.GetCoordinateY2();
				double outerrectanglex1 = outerrectangle.GetCoordinateX1();
				double outerrectanglex2 = outerrectangle.GetCoordinateX2();
				double outerrectangley1 = outerrectangle.GetCoordinateY1();
				double outerrectangley2 = outerrectangle.GetCoordinateY2();
				if (innerrectanglex1 > innerrectanglex2) {
					double temp = innerrectanglex1;
					innerrectanglex1 = innerrectanglex2;
					innerrectanglex2 = temp;
				}
				if (innerrectangley1 > innerrectangley2) {
					double temp = innerrectangley1;
					innerrectangley1 = innerrectangley2;
					innerrectangley2 = temp;
				}
				if (outerrectanglex1 > outerrectanglex2) {
					double temp = outerrectanglex1;
					outerrectanglex1 = outerrectanglex2;
					outerrectanglex2 = temp;
				}
				if (outerrectangley1 > outerrectangley2) {
					double temp = outerrectangley1;
					outerrectangley1 = outerrectangley2;
					outerrectangley2 = temp;
				}
				if (
						((innerrectanglex1 > outerrectanglex1
						&& innerrectanglex1 < outerrectanglex2)
						|| (outerrectanglex1 > innerrectanglex1
						&& outerrectanglex1 < innerrectanglex2)
						|| (innerrectanglex2 > outerrectanglex1
						&& innerrectanglex2 < outerrectanglex2)
						|| (outerrectanglex2 > innerrectanglex1
						&& outerrectanglex2 < innerrectanglex2))
						&&
						((innerrectangley1 > outerrectangley1
						&& innerrectangley1 < outerrectangley2)
						|| (outerrectangley1 > innerrectangley1
						&& outerrectangley1 < innerrectangley2)
						|| (innerrectangley2 > outerrectangley1
						&& innerrectangley2 < outerrectangley2)
						|| (outerrectangley2 > innerrectangley1
						&& outerrectangley2 < innerrectangley2))
				) {
					int pointx = FindClass(i);
					int pointy = FindClass(j);
					rectangleclass[pointx] = pointy;
				}
			}
			i++;
		}
		i = 0;
		HashMap<Integer, Integer> Broadcastmap = new HashMap<Integer, Integer>();
		while (i < NumberOfRectangles) {
			FindClass(i);
			Broadcastmap.put(rectanglelist.get(i).GetUniqueId(), rectangleclass[i]);
			i++;
		}
		JavaRDD<BaseRectangle> distributedlist = sc.parallelize(rectanglelist);
		final Broadcast<HashMap<Integer, Integer>> broadcastHashMap = sc.broadcast(Broadcastmap);
		JavaPairRDD<Integer, BaseRectangle> IntegerRectanglePair = distributedlist.keyBy(new Function<BaseRectangle, Integer>() {
			private static final long serialVersionUID = 2L;
			public Integer call(BaseRectangle v1) throws Exception {
				int va = broadcastHashMap.value().get(v1.GetUniqueId());
				return va;
			}
		});
		JavaPairRDD<Integer, Polygon> tuples2 = IntegerRectanglePair.mapValues(new Function<BaseRectangle, Polygon>() {
			private static final long serialVersionUID = 2L;
			public Polygon call(BaseRectangle r) {
				double Coordinatex1 = r.GetCoordinateX1();
				double Coordinatey1 = r.GetCoordinateY1();
				double Coordinatex2 = r.GetCoordinateX2();
				double Coordinatey2 = r.GetCoordinateY2();
				WKTReader reader = new WKTReader();
				Polygon polygon = null;
				try {
					polygon = (Polygon) reader.read(String.format("POLYGON ((%.10f %.10f, %.10f %.10f, %.10f %.10f, %.10f %.10f, %.10f %.10f))", Coordinatex1, Coordinatey1, Coordinatex2, Coordinatey1, Coordinatex2, Coordinatey2, Coordinatex1, Coordinatey2, Coordinatex1, Coordinatey1));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return polygon;
			}
		}).reduceByKey(new Function2<Polygon, Polygon, Polygon>() {
			private static final long serialVersionUID = 2L;
			public Polygon call(Polygon a, Polygon b) throws Exception {
				return (Polygon) a.union(b);
			}
		});
		JavaRDD<Polygon> polys = tuples2.values();
		JavaRDD<String> result = polys.flatMap(new FlatMapFunction<Polygon, String>() {
			private static final long serialVersionUID = 2L;

			public Iterable<String> call(Polygon polygon) throws Exception {
				LineString ls = polygon.getExteriorRing();
				Coordinate[] coords = ls.getCoordinates();
				int i = 0;
				ArrayList<String> LineArray = new ArrayList<String>();
				while (i < coords.length - 1) {
					LineArray.add(coords[i].toString());
					i++;
				}
				return LineArray;
			}
		});
		List<String> results = result.collect();
		List<DoublePair> doubleresult = new ArrayList<DoublePair>();  
		for (i = 0; i<results.size(); i++) {
			String s = results.get(i).replace(", NaN)","");
			String finalstring = s.replace("(","");
			String Pointpair[] = finalstring.split(",");
			DoublePair d = new DoublePair(Double.parseDouble(Pointpair[0]),Double.parseDouble(Pointpair[1]));
			doubleresult.add(d);
		}
		Collections.sort(doubleresult, new DoublePairCompare());
		List<String> stringresult = new ArrayList<String>();
		for (i = 0; i < doubleresult.size(); i++) {
			DoublePair d = doubleresult.get(i);
			stringresult.add(d.x+ ","+d.y);
		}
		JavaRDD<String> RDDresult = sc.parallelize(stringresult);
		RDDresult.saveAsTextFile(Output);	
        sc.close();
    }
    
    public static void main( String[] args ) 
    {
    	SparkConf conf = new SparkConf().setAppName("Group18-RectangleUnion");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	Union.RectangleUnionMain(sc, args[0],args[1]);
    }
}
