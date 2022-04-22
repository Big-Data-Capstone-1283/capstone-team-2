package detector

import org.apache.spark.sql.{ SparkSession, SaveMode, Row, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._  // { StringType, StructField, StructType, DataFrame }
import org.apache.commons.io.FileUtils
import scala.collection.mutable.ArrayBuffer
import java.io.File
import util.Try

object PatternDetector {
	val marginOfError = 0.1  // 10% margin of error for pattern detection
	val testMode = true // Determines if testing data is displayed

	/**
	  * Gets a list of filenames in the given directory, filtered by optional matching file extensions.
	  *
	  * @param dir			Directory to search.
	  * @param extensions	Optional list of file extensions to find.
	  * @return				List of filenames with paths.
	  */
	def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    	dir.listFiles.filter(_.isFile).toList.filter { file => extensions.exists(file.getName.endsWith(_)) }
	}
	/**
	  * Moves/renames a file.
	  *
	  * @param oldName	Old filename and path.
	  * @param newName	New filename.
	  * @return			Success or failure.
	  */
	def mv(oldName: String, newName: String) = {
		Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
	}
	/**
	  * Writes out a dataframe as a CSV file in the local directory and returns the path to that file.
	  * This will overwrite any existing file with the same name.
	  *
	  * @param df		The dataframe to output.
	  * @param filename	The filename for the CSV file.
	  * @return			The full path to the CSV file.
	  */
	def saveDataFrameAsCSV(df: DataFrame, filename: String): String = {
		df.coalesce(1).write.options(Map("header"->"true", "delimiter"->",")).mode(SaveMode.Overwrite).format("csv").save("tempCSVDir")
		val curDir = System.getProperty("user.dir")
		val srcDir = new File(curDir + "/tempCSVDir")
		val files = getListOfFiles(srcDir, List("csv"))
		var srcFilename = files(0).toString()
		val destFilename = curDir + "/" + filename
		FileUtils.deleteQuietly(new File(destFilename))  // Clear out potential old copies
		mv(srcFilename, destFilename)  // Move and rename file
		FileUtils.deleteQuietly(srcDir)  // Delete temp directory
		destFilename
	}

	/**
	  * Calclulates the normalized deviation beyond a margin or error from randomized data for a single-factor dataframe.
	  *
	  * @param df	A dataframe consisting of a label and a count for each label.
	  * @return		A normalized deviation from random data.
	  */
	def deviation1F(df: DataFrame): Double = {
		var newData = df.collect()
		var entryNames = newData.map(_.getString(0))
		var totalEntries = entryNames.length
		var entryCounts = newData.map(_.getLong(1).toInt)
		var totalRows = 0
		for (i <- 0 to totalEntries - 1)
			totalRows += entryCounts(i)
		var avgCount = totalRows / totalEntries  // Compute what the count should be for each entry if they were all selected totally randomly
		var totalDeviation = 0.0
		var currentDeviation = 0.0
		var margin = avgCount * marginOfError  // Ignore anything below the margin of error
		if (PatternDetector.testMode)  // Show info if we're in test mode
			println(s"Total data points: $totalRows\nDistinct values: $totalEntries\nExpected rate: $avgCount  +/-$margin")
		for (i <- 0 to totalEntries - 1) {
			currentDeviation = (entryCounts(i).toDouble - avgCount.toDouble) / margin
			totalDeviation += currentDeviation.abs
			if (PatternDetector.testMode)  // Show info if we're in test mode
				if (currentDeviation.abs < 1.0)
					println(s"${entryNames(i)}: ${entryCounts(i)} (within expected rate; deviation = $currentDeviation)")
				else
					println(s"${entryNames(i)}: ${entryCounts(i)} (does not match expected rate; deviations = $currentDeviation)")
		}
		val normalizedDeviation = totalDeviation / totalEntries.toDouble
		if (PatternDetector.testMode)  // Show info if we're in test mode
			println(s"Total deviation: $totalDeviation\nNormalized deviation: $normalizedDeviation\n")
		normalizedDeviation
	}

	/**
	  * This method takes a dataframe and tests it for various patterns in the data.
	  *
	  * @param data	A dataframe with data using the schema given for this project.
	  */
	def Go(data: DataFrame): Unit = {
		var funcArr = ArrayBuffer.empty[Function[DataFrame, Option[String]]]  // Array of pattern testing functions
		var descArr = ArrayBuffer.empty[String]  // Description/title of each function
		var result: Option[String] = None

		// Add new pattern detection functions below
		descArr += "Country pattern"
		funcArr += CountryPattern.Go
		descArr += "Payment Type pattern"
		funcArr += PaymentTypePattern.Go

		// Run all of the pattern tests
		for (i <- 0 to funcArr.length - 1) {
			result = funcArr(i)(data)  // Test for pattern
			if (result == None)
				println(s"${descArr(i)}: None")
			else
				println(s"${descArr(i)}: ${result.get}")
			if (PatternDetector.testMode && (i < funcArr.length - 1))  // Show info if we're in test mode
				println("\n=====================================\n")
		}
		if (PatternDetector.testMode)
			println("\nAll tests complete.")
	}
}
