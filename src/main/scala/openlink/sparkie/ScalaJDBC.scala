package openlink.sparkie

import org.apache.spark.sql.SparkSession

import java.io.{File, FileInputStream}
import java.util.Properties

/**
 * Loads a DataFrame from a relational database table over JDBC,
 * manipulates the data, and saves the results back to a table.
 */
object ScalaJDBC {
	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("SUsingJDBC").getOrCreate()
		
		// Load properties from file
		val dbProperties = new Properties
		dbProperties.load(new FileInputStream(new File("db-properties.flat")))
		val jdbcUrl = dbProperties.getProperty("jdbcUrl")
		
		println("A DataFrame loaded from the entire contents of a table over JDBC.")
		var where = "sparkie.people"
		val entireDF = spark.read.jdbc(jdbcUrl, where, dbProperties)
		entireDF.printSchema()
		entireDF.show()
		
		println("Filtering the table to just show the males.")
		entireDF.filter("is_male = 1").show()
		
		println("Alternately, pre-filter the table for males before loading over JDBC.")
		where = "(select * from sparkie.people where is_male = 1) as subset"
		val malesDF = spark.read.jdbc(jdbcUrl, where, dbProperties)
		malesDF.show()
		
		println("Update weights by 2 pounds (results in a new DataFrame with same column names)")
		val heavyDF = entireDF.withColumn("updated_weight_lb", entireDF("weight_lb") + 2)
		val updatedDF = heavyDF.select("id", "name", "is_male", "height_in", "updated_weight_lb")
			.withColumnRenamed("updated_weight_lb", "weight_lb")
		updatedDF.show()
		
		println("Save the updated data to a new table with JDBC")
		where = "sparkie.updated_people"
		updatedDF.write.mode("error").jdbc(jdbcUrl, where, dbProperties)
		
		println("Load the new table into a new DataFrame to confirm that it was saved successfully.")
		val retrievedDF = spark.read.jdbc(jdbcUrl, where, dbProperties)
		retrievedDF.show()

		spark.stop()
	}
}
