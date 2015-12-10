package com.hortonworks.rxu;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.hive.HiveContext;
//import org.apache.spark.sql.hive.api.java.HiveContext;


/**
 * @author Richard Xu
 *
 */
public final class SparkEtl {
	//dummy mask 
	 static class Mask implements Function<String, String> {

		    @Override
		    public String call(String line) {
		    	StringBuilder newLine = new StringBuilder(line);
		    	newLine.replace(54, 69, "xxxxxxxxxxxxxxx");
		      return newLine.toString();
		    }
		  }
	
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err
					.println("Please use: SparkEtl <input file> <output file> <file for validEntries> <file for invalid>");
			System.exit(1);
		}

		//@SuppressWarnings("resource")
		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkEtl");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		//JavaSparkContext sc = new JavaSparkContext(args[0], "SparkEtl",
		//		System.getenv("SPARK_HOME"),
		//		JavaSparkContext.jarOfClass(SparkEtl.class));
		
		JavaRDD<String> people = sc.textFile(args[0]);

		JavaRDD<String> masked = people.map(new Mask());
		
		JavaRDD<String> inValidEmps = masked.filter(
			      new Function<String, Boolean>(){ public Boolean call(String line) {
			    	  // validation rule 1: one cannot be his own boss:
			          if (line.substring(0, 9).trim().equals(line.substring(70, 79).trim())) {
			            return true;
			          }
			          // rule 2: empty firstname or lastname  
			          else if(line.substring(10, 29).trim().length()<1 || line.substring(30, 49).trim().length()<1){
			        	  return true;
			          }else {
			            return false;
			          }
			          
			        }
			      });
		
		JavaRDD<String> validEmps = masked.subtract(inValidEmps);

		masked.saveAsTextFile(args[1]);
		validEmps.saveAsTextFile(args[2]);
		inValidEmps.saveAsTextFile(args[3]);

		
		
		HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());				

		// The schema is encoded in a string
		String schemaString = "emp_id first_name last_name job_title mgr_emp_id";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
		  fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		
		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = masked.map(
		  new Function<String, Row>() {
		    public Row call(String record) throws Exception {
		      	return RowFactory.create(record.substring(0, 9), record.substring(10, 29), record.substring(30, 49), 
		    		  record.substring(50, 69), record.substring(70, 78));
		    }
		  });
		
		// Apply the schema to the RDD.
		//DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);
		DataFrame peopleDataFrame = hiveContext.createDataFrame(rowRDD, schema);
		
		// Register the DataFrame as a table.
		peopleDataFrame.registerTempTable("people");
		// SQL can be run over RDDs that have been registered as tables.
		DataFrame results = hiveContext.sql("SELECT emp_id FROM people");
		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> empids = results.javaRDD().map(new Function<Row, String>() {
		  public String call(Row row) {
		    return "emp_id: " + row.getString(0);
		  }
		}).collect();
		
		System.out.println(empids.toString());

		hiveContext.sql("CREATE TABLE IF NOT EXISTS masked_t1 (emp_id string, first_name string, last_name string, job_title string, mgr_emp_id string)");

		peopleDataFrame.insertInto("masked_t1");
		
		//peopleDataFrame.saveAsTable("default.people_t1");
		
		sc.stop();

	}

}
