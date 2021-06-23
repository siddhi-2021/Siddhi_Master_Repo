
package sparkPack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sparkObj {


	def main(args:Array[String]):Unit={


			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")

					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._  

					val rawdf= spark.read.format("json").option("multiLine","true").load("file:///C:/data/array1_new.json")

					println
					print("=================raw df=======================")
					println
					rawdf.show()
					rawdf.printSchema()

					val flatten_df = rawdf.select(
							col("Students").alias("Students"),
							col("address.Permanent_address").alias("Permanent_address"),
							col("address.temporary_address").alias("temporary_address"),
							col("first_name").alias("first_name"),
							col("second_name").alias("second_name")
							)
					println
					print("================flatten_df data=================")
					println
					flatten_df.show()
					flatten_df.printSchema()


					val explode1_df = flatten_df
					.withColumn("Students",explode(col("Students")))


					println
					print("================explode1_df data=================")
					println

					explode1_df.show()
					explode1_df.printSchema()		

					val explode2_df = explode1_df
					.withColumn("components",explode(col("Students.user.components")))


					println
					print("================explode2df data=================")
					println

					explode2_df.show()
					explode2_df.printSchema()		

					println
					print("================final data=================")
					println

					val final_df = explode2_df.select(

							col("Students.user.address.Permanent_address").alias("C_Permanent_address"),
							col("Students.user.address.temporary_address").alias("C_temporary_address"),
							col("Students.user.gender").alias("gender"),
							col("Students.user.name.first").alias("first"),
							col("Students.user.name.last").alias("last"),
							col("Students.user.name.title").alias("title"),
							col("Permanent_address").alias("Permanent_address"),
							col("temporary_address").alias("temporary_address"),
							col("first_name").alias("first_name"),
							col("second_name").alias("second_name"),
							col("components").alias("components")

							)







					final_df.show()
					final_df.printSchema()








	}

}