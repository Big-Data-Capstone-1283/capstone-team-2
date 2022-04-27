package detector

import org.apache.spark.sql.{ SparkSession, SaveMode, Row, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._  // { StringType, StructField, StructType, DataFrame }
import org.apache.commons.io.FileUtils
import scala.collection.mutable.ArrayBuffer
import org.joda.time.DateTime
import java.io.File
import util.Try

object PatternDetector {
	// Constants:
	val marginOfError = 0.05  // 5% margin of error for pattern detection
	val testMode = true // Determines if testing data is displayed

	// Day of week constants and variables:
	val daymap = Map("Monday" -> 1, "Tuesday" -> 2, "Wednesday" -> 3, "Thursday" -> 4, "Friday" -> 5, "Saturday" -> 6, "Sunday" -> 7)  // Used to map the "day of week" names to a number
	val daymapCol = typedlit(daymap)  // Used to map days of week into a sortable column
	val dayToName = Map(1 -> "Monday", 2 -> "Tuesday", 3 -> "Wednesday", 4 -> "Thursday", 5 -> "Friday", 6 -> "Saturday", 7 -> "Sunday")  // Used to convert "day of week" numbers into a string name
	var minDaysSeq = Seq.empty[String]
	var minCount = 0
	var maxCount = 0

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
					println(s"${entryNames(i)}: ${entryCounts(i)} (does NOT match expected rate; deviations = $currentDeviation)")
		}
		val normalizedDeviation = totalDeviation / totalEntries.toDouble
		if (PatternDetector.testMode)  // Show info if we're in test mode
			println(s"Total deviation: $totalDeviation\nNormalized deviation: $normalizedDeviation\n")
		normalizedDeviation
	}

	/**
	  * Calclulates the normalized deviation beyond a margin or error from randomized data for a two-factor dataframe.
	  *
	  * @param df	A dataframe consisting of a label and a count for each label.
	  * @return		A normalized deviation from random data.
	  */
	def deviation2F(df: DataFrame): Double = {
		var newData = df.collect()
		var entryNames = newData.map(x => x.getString(0) + "+" + x.getString(1))
		var totalEntries = entryNames.length
		var entryCounts = newData.map(_.getLong(2).toInt)
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
				if (currentDeviation.abs <= 1.0)
					println(s"${entryNames(i)}: ${entryCounts(i)} (within expected rate; deviation = $currentDeviation)")
				else
					println(s"${entryNames(i)}: ${entryCounts(i)} (does NOT match expected rate; deviations = $currentDeviation)")
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

		// Generate the data from the dataframe that's needed for the "day of week" pattern tests
		var dateRangeDf = data  // Get the range of dates that the data covers
			.select("datetime")
			.withColumn("data", lit("date range"))  // Make a dummy column to group by
			.groupBy("data")
			.agg(min("datetime").as("min_date"), max("datetime").as("max_date"))  // Find the start and end of the date range
			.drop("data")
		if (PatternDetector.testMode)  // If we're in test mode...
			dateRangeDf.show(false)  // ...show the date range we're working with.
		val dateRangeData = dateRangeDf.head()  // Copy the data into Row object
		val dateStart = new DateTime(dateRangeData(0)).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)  // Earliest date (stripped of time)
		val dateEnd = new DateTime(dateRangeData(1)).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)  // Latest date (stripped of time)
		if (PatternDetector.testMode)  // If we're in test mode...
			println(s"Start date: $dateStart\nEnd date: $dateEnd")  // ...verify that the date range was converted properly
		var dayCount = Array.ofDim[Int](8)  // Array for determining count of days; Mon = 1 to Sun = 7 (no day for 0)
		var curDate = dateStart.plusDays(-1)
		while (curDate != dateEnd) {  // Count up the numbers of each day of the week for the data's whole time range
			curDate = curDate.plusDays(1)
			dayCount(curDate.getDayOfWeek()) += 1
		}
		minCount = dayCount(1)  // Lowest count of day of week
		maxCount = dayCount(1)  // Highest count of day of week
		for (i <- 1 to 7) {  // Find the minimum and maximum counts for the days of the week to use for data normalization
			if (minCount > dayCount(i))
				minCount = dayCount(i)
			if (maxCount < dayCount(i))
				maxCount = dayCount(i)
			if (PatternDetector.testMode)  // If we're in test mode...
				println(dayToName(i) + ": " + dayCount(i))  // ...show count number of each day of week for normalization
		}
		minDaysSeq = Seq.empty[String]
		for (i <- 1 to 7)  // Make a list of days which have the minimum counts to use for data normalization
			if (dayCount(i) == minCount)
				minDaysSeq = minDaysSeq :+ dayToName(i)

		// Add new pattern detection functions below
		descArr += "Country pattern"
		funcArr += CountryPattern.Go
		descArr += "Payment Type pattern"
		funcArr += PaymentTypePattern.Go
		descArr += "Transaction Success pattern"
		funcArr += TxnSuccessRatePattern.Go
		descArr += "Ecommerce Website pattern"
		funcArr += WebsitePattern.Go
		descArr += "Product Category pattern"
		funcArr += ProductCategoryPattern.Go
		descArr += "Product Category + Country pattern"
		funcArr += ProdCat_CountryPattern.Go
		descArr += "Day of Week pattern"
		funcArr += DayOfWeekPattern.Go

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
