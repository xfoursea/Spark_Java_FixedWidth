package org.sparketl.etljobs;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;

/**
 * @author vijith.reddy
 *
 */
public final class SparkEtl {
	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.err
					.println("Please use: SparkEtl <master> <input file> <output file for key value> <output file for values only> <country>");
			System.exit(1);
		}
		// System.out.println("The class path is    "+System.getProperty("java.class.path"));

		@SuppressWarnings("resource")
		JavaSparkContext spark = new JavaSparkContext(args[0], "SparkEtl",
				System.getenv("SPARK_HOME"),
				JavaSparkContext.jarOfClass(SparkEtl.class));
		JavaRDD<String> file = spark.textFile(args[1]);

		// Should be a final variable for variable scope in Java8
		final String filterByCt = (String) args[4];

		// As per JSON file each line item is a different json
		FlatMapFunction<String, String> jsonLine = jsonFile -> {
			return Arrays.asList(jsonFile.split("\\r?\\n"));
		};

		JavaRDD<String> eachLine = file.flatMap(jsonLine);

		// This can be customized per JSON Schema
		PairFunction<String, String, String> mapCountry = eachItem -> {
			JSONParser parser = new JSONParser();
			String country = "";
			String lines = "";
			try {
				Object obj = parser.parse(eachItem);
				JSONObject jsonObj = (JSONObject) obj;
				country = (String) jsonObj.get("country");
				String name = (String) jsonObj.get("name");

				lines = name + "\t" + country;

			} catch (Exception e) {
				e.printStackTrace();
			}
			return new Tuple2<String, String>(lines, country);
		};

		JavaPairRDD<String, String> pairs = eachLine.mapToPair(mapCountry);

		// Filter function filters the results with the given country
		Function<Tuple2<String, String>, Boolean> func = (map) -> {
			return (map._2.equals(filterByCt));
		};

		// Reduce the result set as per the filter criteria and save it
		JavaPairRDD<String, String> reduced = pairs.filter(func);
		// Caching intermediate RDD for reuse
		reduced.persist(StorageLevel.MEMORY_AND_DISK());
		// Action to save the file to disk
		reduced.saveAsTextFile(args[2]);
		// Retrieve values leaving the keys behind
		JavaRDD<String> maps = reduced.keys();
		// Save the values to the disk
		maps.saveAsTextFile(args[3]);
		System.exit(0);

	}

}
