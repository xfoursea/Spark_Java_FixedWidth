package org.sparketl.etljobs;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;

/**
 * @author vijith.reddy
 *
 */
public final class SparkEtl {
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err
					.println("Please use: SparkEtl <master> <input file> <output file> <country>");
			System.exit(1);
		}
		//System.out.println("The class path is    "+System.getProperty("java.class.path"));
		
		@SuppressWarnings("resource")
		JavaSparkContext spark = new JavaSparkContext(args[0],
				"SparkEtl", System.getenv("SPARK_HOME"),
				JavaSparkContext.jarOfClass(SparkEtl.class));
		JavaRDD<String> file = spark.textFile(args[1]);
		final String filterByCt=(String)args[3];
		FlatMapFunction<String, String> jsonLine = jsonFile -> {
			return Arrays.asList(jsonFile.toLowerCase().split("\\r?\\n"));
		};

		JavaRDD<String> eachLine = file.flatMap(jsonLine);

		PairFunction<String, String, String> mapCountry = eachItem -> {
			JSONParser parser = new JSONParser();
			String country = "";
			String lines="";
			try {
				Object obj = parser.parse(eachItem);
				JSONObject jsonObj = (JSONObject) obj;
				country = (String) jsonObj.get("country");
				String name = (String) jsonObj.get("name");
				
				lines=name+"\t"+country;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			return new Tuple2<String, String>(lines, country);
		};

		JavaPairRDD<String, String> pairs = eachLine.mapToPair(mapCountry);
		
		Function<Tuple2<String, String>, Boolean> func=(map)->{
			 return (map._2.equals(filterByCt));
		
		};
		

		JavaPairRDD<String,String> reduced=pairs.filter(func);
		reduced.saveAsTextFile(args[2]);
		System.exit(0);

	}

}
