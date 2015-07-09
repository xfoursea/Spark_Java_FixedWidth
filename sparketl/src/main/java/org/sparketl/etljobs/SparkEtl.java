package org.sparketl.etljobs;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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
		if (args.length < 3) {
			System.err
					.println("Please use: SparkEtl <master> <input file> <output file>");
			System.exit(1);
		}
		//System.out.println("The class path is    "+System.getProperty("java.class.path"));
		
		@SuppressWarnings("resource")
		JavaSparkContext spark = new JavaSparkContext(args[0],
				"Java Wordcount", System.getenv("SPARK_HOME"),
				JavaSparkContext.jarOfClass(SparkEtl.class));
		JavaRDD<String> file = spark.textFile(args[1]);

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

		pairs.sortByKey(true).saveAsTextFile(args[2]);
		System.exit(0);

	}

}
