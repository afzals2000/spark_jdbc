package openlink.sparkie;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Loads a DataFrame from a relational database table over JDBC,
 * manipulates the data, and saves the results back to a table.
 */
public final class JUsingJDBC {

	public static void main(String[] args) throws Exception {
		SparkSession spark = SparkSession.builder().appName("JUsingJDBC").getOrCreate();

		// Load properties from file
		Properties dbProperties = new Properties();
		dbProperties.load(new FileInputStream(new File("db-properties.flat")));
		String jdbcUrl = dbProperties.getProperty("jdbcUrl");

		System.out.println("A DataFrame loaded from the entire contents of a table over JDBC.");
		String where = "sparkie.people";
		Dataset<Row> entireDF = spark.read().jdbc(jdbcUrl, where, dbProperties);
		entireDF.printSchema();
		entireDF.show();

		System.out.println("Filtering the table to just show the males.");
		entireDF.filter("is_male = 1").show();

		System.out.println("Alternately, pre-filter the table for males before loading over JDBC.");
		where = "(select * from sparkie.people where is_male = 1) as subset";
		Dataset<Row> malesDF = spark.read().jdbc(jdbcUrl, where, dbProperties);
		malesDF.show();

		System.out.println("Update weights by 2 pounds (results in a new DataFrame with same column names)");
		Dataset<Row> heavyDF = entireDF.withColumn("updated_weight_lb", entireDF.col("weight_lb").plus(2));
		Dataset<Row> updatedDF = heavyDF.select("id", "name", "is_male", "height_in", "updated_weight_lb")
			.withColumnRenamed("updated_weight_lb", "weight_lb");
		updatedDF.show();

		System.out.println("Save the updated data to a new table with JDBC");
		where = "sparkie.updated_people";
		updatedDF.write().mode("error").jdbc(jdbcUrl, where, dbProperties);

		System.out.println("Load the new table into a new DataFrame to confirm that it was saved successfully.");
		Dataset<Row> retrievedDF = spark.read().jdbc(jdbcUrl, where, dbProperties);
		retrievedDF.show();

		spark.stop();
	}
}
