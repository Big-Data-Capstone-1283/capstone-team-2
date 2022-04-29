package detector

import org.apache.spark.sql.{ SparkSession, SaveMode, Row, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.commons.io.FileUtils
import scala.collection.mutable.ArrayBuffer
import org.joda.time.format.{ DateTimeFormat, DateTimeFormatter }
import org.joda.time.DateTime
import java.io.File
import util.Try
import org.joda.time.Days

object PatternDetector {
	// Constants:
	val marginOfError = 0.05  // 5% margin of error for pattern detection
	val testMode = true // Determines if testing data is displayed
	val forceCSV = true // Determines if CSV files are written out even when there's no pattern

	// Constants and variables for date and time operations:
	var dateStart = new DateTime()  // Start date of the timeframe that the dataframe covers (without time data)
	var dateEnd = new DateTime()  // End date of the timeframe that the dataframe covers (without time data)
	var numberOfDays = 0  // The total number of days that the dataframe covers
	val daymap = Map("Monday" -> 1, "Tuesday" -> 2, "Wednesday" -> 3, "Thursday" -> 4, "Friday" -> 5, "Saturday" -> 6, "Sunday" -> 7)  // Used to map the "day of week" names to a number
	val daymapCol = typedlit(daymap)  // Used to map day of week to day number in a Spark column
	val dayToName = Map(1 -> "Monday", 2 -> "Tuesday", 3 -> "Wednesday", 4 -> "Thursday", 5 -> "Friday", 6 -> "Saturday", 7 -> "Sunday")  // Used to convert "day of week" numbers into a string name
	var daysPerMonth = Map.empty[String, Int]  // A map of "year-month" ("yyyy-MM" format) to "number of days" for each month in the dataframe
	var daysPerMonthCol = typedlit(Map.empty[String, Int])  // The "year-month to number of days" map for use in Spark columns
	var minCount = 0  // Lowest count for a day of week
	var maxCount = 0  // Highest count for a day of week
	var minDaysSeq = Seq.empty[String]  // A list of days of week which were of the lowest count

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
	  * Calclulates the normalized deviation beyond a margin of error (set by this object's `marginOfError` variable) from randomized data on a dataframe column of type DoubleType.
	  * If the `testMode` variable on the PatternDetector object is set to `true`, then it shows the results using the `entryColNums` column values as the row labels.
	  *
	  * @param df			A dataframe consisting of one or more StringType label columns and a DoubleType column to test.
	  * @param dataColNum	The column number of DoubleType to test on (starting from column `0`).
	  * @param labelColNums A sequence of StringType columns to use as labels for the data when showing them if "testMode" is `true`.
	  * @return				Deviation from random data.  (< 1 means probably no pattern, >= 1 the larger the value the more likely it found a pattern)
	  */
	def getDeviationDouble(df: DataFrame, dataColNum: Int, labelColNums: Seq[Int]): Double = {
		var newData = df.collect()
		var label = ""
		var groupLabels = newData.map(x => {  // Generate a label for each group of data
			for (i <- 0 to labelColNums.length - 1)
				if (i == 0)
					label = x.getString(labelColNums(0))
				else
					label += "+" + x.getString(labelColNums(i))
			label
		})
		var groupCount = groupLabels.length  // Number of distinct groups of data
		var entryCounts = newData.map(_.getDouble(dataColNum))  // Array of values for each group
		var groupTotal = entryCounts.sum  // Sum of all group values
		var avgCount = groupTotal / groupCount  // Compute what the count should be for each entry if they were all selected totally randomly
		var totalDeviation = 0.0  // Sum of all groups' deviations
		var currentDeviation = 0.0
		var margin = avgCount * marginOfError  // Ignore anything below the margin of error
		if (PatternDetector.testMode) { // Show info if we're in test mode
			println(s"Column tested: ${df.schema.fields(dataColNum).name}\nDistinct groups: $groupCount\nSum of group values: $groupTotal\nExpected rate: $avgCount  +/-$margin\nGroups:")
		}
		for (i <- 0 to groupCount - 1) {
			currentDeviation = (entryCounts(i).toDouble - avgCount.toDouble) / margin
			totalDeviation += currentDeviation.abs
			if (PatternDetector.testMode)  // Show info if we're in test mode
				if (currentDeviation.abs <= 1.0)
					println(s"  ${groupLabels(i)}: ${entryCounts(i)} (within expected rate; deviation = $currentDeviation)")
				else
					println(s"  ${groupLabels(i)}: ${entryCounts(i)} (does NOT match expected rate; deviations = $currentDeviation)")
		}
		val normalizedDeviation = totalDeviation / groupCount.toDouble  // Resulting deviation (< 1 means probably no pattern, >= 1 the larger the value the more likely it found a pattern)
		if (PatternDetector.testMode)  // Show info if we're in test mode
			println(s"Total deviation: $totalDeviation\nNormalized deviation: $normalizedDeviation\n")
		normalizedDeviation
	}
	/**
	  * Calclulates the normalized deviation beyond a margin of error (set by this object's `marginOfError` variable) from randomized data on a dataframe column of type LongType.
	  * If the `testMode` variable on the PatternDetector object is set to `true`, then it shows the results using the `entryColNums` column values as the row labels.
	  *
	  * @param df			A dataframe consisting of one or more StringType label columns and a LongType column to test.
	  * @param dataColNum	The column number of LongType to test on (starting from column `0`).
	  * @param labelColNums A sequence of StringType columns to use as labels for the data when showing them if "testMode" is `true`.
	  * @return				Deviation from random data.  (< 1 means probably no pattern, >= 1 the larger the value the more likely it found a pattern)
	  */
	def getDeviationLong(df: DataFrame, dataColNum: Int, labelColNums: Seq[Int]): Double = {
		var newData = df.collect()
		var label = ""
		var groupLabels = newData.map(x => {  // Generate a label for each group of data
			for (i <- 0 to labelColNums.length - 1)
				if (i == 0)
					label = x.getString(labelColNums(0))
				else
					label += "+" + x.getString(labelColNums(i))
			label
		})
		var groupCount = groupLabels.length  // Number of distinct groups of data
		var entryCounts = newData.map(_.getLong(dataColNum))  // Array of values for each group
		var groupTotal = entryCounts.sum  // Sum of all group values
		var avgCount = groupTotal / groupCount  // Compute what the count should be for each entry if they were all selected totally randomly
		var totalDeviation = 0.0  // Sum of all groups' deviations
		var currentDeviation = 0.0
		var margin = avgCount * marginOfError  // Ignore anything below the margin of error
		if (PatternDetector.testMode) { // Show info if we're in test mode
			println(s"Column tested: ${df.schema.fields(dataColNum).name}\nDistinct groups: $groupCount\nSum of group values: $groupTotal\nExpected rate: $avgCount  +/-$margin\nGroups:")
		}
		for (i <- 0 to groupCount - 1) {
			currentDeviation = (entryCounts(i).toDouble - avgCount.toDouble) / margin
			totalDeviation += currentDeviation.abs
			if (PatternDetector.testMode)  // Show info if we're in test mode
				if (currentDeviation.abs <= 1.0)
					println(s"  ${groupLabels(i)}: ${entryCounts(i)} (within expected rate; deviation = $currentDeviation)")
				else
					println(s"  ${groupLabels(i)}: ${entryCounts(i)} (does NOT match expected rate; deviations = $currentDeviation)")
		}
		val normalizedDeviation = totalDeviation / groupCount.toDouble  // Resulting deviation (< 1 means probably no pattern, >= 1 the larger the value the more likely it found a pattern)
		if (PatternDetector.testMode)  // Show info if we're in test mode
			println(s"Total deviation: $totalDeviation\nNormalized deviation: $normalizedDeviation\n")
		normalizedDeviation
	}
	/**
	  * Calclulates the normalized deviation beyond a margin of error from randomized data for a single-factor dataframe.
	  *
	  * @param df	A dataframe consisting of a label (column 0) and a count (column 1) for each label.
	  * @return				Deviation from random data.  (< 1 means probably no pattern, >= 1 the larger the value the more likely it found a pattern)
	  */
	def deviation1F(df: DataFrame): Double = {
		getDeviationLong(df, 1, Seq(0))
	}
	/**
	  * Calclulates the normalized deviation beyond a margin of error from randomized data for a two-factor dataframe.
	  *
	  * @param df	A dataframe consisting of two labels (columns 0 & 1) and a count (column 2) for each label.
	  * @return				Deviation from random data.  (< 1 means probably no pattern, >= 1 the larger the value the more likely it found a pattern)
	  */
	def deviation2F(df: DataFrame): Double = {
		getDeviationLong(df, 2, Seq(0, 1))
	}

	/**
	  * Generate the data from the dataframe that's needed for pattern tests on date and time.
	  *
	  * @param df	The dataframe to examine with dates in a "datetime" colum.
	  */
	private def getDateTimeInfo(df: DataFrame): Unit = {
		// Get the range of dates that the data covers
		var dateRangeDf = df
			.select("datetime")
			.withColumn("data", lit("date range"))  // Make a dummy column to group by
			.groupBy("data")
			.agg(min("datetime").as("min_date"), max("datetime").as("max_date"))  // Find the start and end of the date range
			.drop("data")
		if (PatternDetector.testMode)  // If we're in test mode...
			dateRangeDf.show(false)  // ...show the date range we're working with.
		val dateRangeData = dateRangeDf.head()  // Copy the data into Row object
		dateStart = new DateTime(dateRangeData(0)).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)  // Earliest date (stripped of time)
		dateEnd = new DateTime(dateRangeData(1)).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)  // Latest date (stripped of time)
		numberOfDays = Days.daysBetween(dateStart, dateEnd).getDays() + 1  // Total number of days covered by date range
		if (PatternDetector.testMode)  // If we're in test mode...
			println(s"Start date: $dateStart\nEnd date: $dateEnd\nNumber of days: $numberOfDays\n")  // ...verify that the date range was converted properly

		// Create a sequence in minDaysSeq noting which days of week are of the shorter length
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
				println(dayToName(i) + ": " + dayCount(i) + " count")  // ...show count number of each day of week for normalization
		}
		minDaysSeq = Seq.empty[String]
		for (i <- 1 to 7)  // Make a list of days which have the minimum counts to use for data normalization
			if (dayCount(i) == minCount)
				minDaysSeq = minDaysSeq :+ dayToName(i)

		// Create a map in daysPerMonth & daysPerMonthCol for year/month -> days in the month
		val fmt = DateTimeFormat.forPattern("yyyy-MM")
		curDate = dateStart
		daysPerMonth = Map(fmt.print(curDate) -> (curDate.dayOfMonth().getMaximumValue() - curDate.dayOfMonth().get() + 1))  // Get number of days for the first month
		curDate = curDate.plusMonths(1)
		while (curDate.getYear() != dateEnd.getYear() || curDate.getMonthOfYear() != dateEnd.getMonthOfYear()) {  // Get number of days for each month in data
			daysPerMonth = daysPerMonth + (fmt.print(curDate) -> curDate.dayOfMonth().getMaximumValue())
			curDate = curDate.plusMonths(1)
		}
		daysPerMonth = daysPerMonth + (fmt.print(curDate) -> dateEnd.getDayOfMonth())  // Get number of days for the last month
		daysPerMonthCol = typedlit(daysPerMonth)  // Create a version of daysPerMonth to use in Spark columns
		if (PatternDetector.testMode) {  // If we're in test mode...
			println("")
			for (x <- daysPerMonth)
				println(x._1 + " = " + x._2 + " days")  // ...show the year/month count of days
		}
		if (PatternDetector.testMode)  // Show if we're in test mode
			println("\n=====================================\n")
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

		val fname = saveDataFrameAsCSV(data, "Full_Cleaned_Dataset.csv")  // Write out the data we receive
		if (PatternDetector.testMode)  // If we're in test mode...
			println(s"Original dataset saved as: $fname\n\n=====================================\n")  // ...show where the data was saved

		getDateTimeInfo(data)  // Generates the data from the dataframe that's needed for pattern tests on date and time

		// ** Add new pattern detection functions below **
		/* Resulting output columns:
				first one or two columns = group label(s)
				count = number of rows in the data with each group  (usually an integer)
				total = sum of "qty" for each group  (usually an integer)
				total_successful = sum of "qty" for each group where "payment_txn_success" = "Y"  (usually an integer)
			Some other columns are used for certain patterns, see each *Pattern.scala file for details.
		*/
		// 1-factor patterns
		descArr += "Country pattern"
		funcArr += CountryPattern.Go
		descArr += "Failure Reason pattern"
		funcArr += FailureReasonPattern.Go
		descArr += "Day of Week pattern"
		funcArr += OrdersByDayOfWeekPattern.Go
		descArr += "Hour of Day pattern"
		funcArr += OrdersByHourOfDayPattern.Go
		descArr += "Months pattern"
		funcArr += OrdersByMonthPattern.Go
		descArr += "Weeks pattern"
		funcArr += OrdersByWeekPattern.Go
		descArr += "Payment Type pattern"
		funcArr += PaymentTypePattern.Go
		descArr += "Product Category pattern"
		funcArr += ProductCategoryPattern.Go
		descArr += "Quantity pattern"
		funcArr += QuantityPattern.Go
		descArr += "Transaction Success pattern"
		funcArr += TxnSuccessPattern.Go
		descArr += "Website pattern"
		funcArr += WebsitePattern.Go

		// 2-factor patterns
		descArr += "Income by Product Category pattern"
		funcArr += IncomeByProdCatPattern.Go
		descArr += "Payment Type + City pattern"
		funcArr += PmtType_CityPattern.Go
		descArr += "Payment Type + Country pattern"
		funcArr += PmtType_CountryPattern.Go
		descArr += "Product Category + City pattern"
		funcArr += ProdCat_CityPattern.Go
		descArr += "Product Category + Country pattern"
		funcArr += ProdCat_CountryPattern.Go
		descArr += "Product Category + Payment Type"
		funcArr += ProdCat_PmtTypePattern.Go
		descArr += "Product Category + Website"
		funcArr += ProdCat_WebsitePattern.Go
		descArr += "Website + Country pattern"
		funcArr += Website_CountryPattern.Go

		// Run all of the pattern tests
		for (i <- 0 to funcArr.length - 1) {
			if (PatternDetector.testMode)
				println(s"Results for '${descArr(i)}':\n")
			result = funcArr(i)(data)  // Test for pattern
			if (result == None)
				println(s"${descArr(i)}: None")
			else
				println(s"${descArr(i)}: ${result.get}")
			if (PatternDetector.testMode && (i < funcArr.length - 1))  // Show if we're in test mode
				println("\n=====================================\n")
		}
		if (PatternDetector.testMode)
			println("\nAll tests complete.")
	}
}
