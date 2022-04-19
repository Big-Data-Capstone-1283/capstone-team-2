package producer

object WeightedRandomizer_Test {
	/**
	  * Returns a random key from a Map containing `"item name" -> weight` pairs, where the
	  * weighted value indicates its frequency relative to the total value of all weights.
	  * e.g. `WeightedRandomizer(Map("X" -> 60, "Y" -> 30, "Z" -> 10)` would return "X"
	  * 60% of the time, "Y" 30% of the time, and "Z" 10% of the time.
	  *
	  * @param items	A Map of `String -> Int` pairs, representing item name and weight.
	  * @return			The semi-randomly chosen key from that map.
	  */
	def WeightedRandomizer(items: Map[String, Int]): String = {
		var weightSum = 0
		for (i <- items)  // Find the total value of weights for all keys
			weightSum += i._2
		val rnd = scala.util.Random.nextInt(weightSum)  // Pick a random number within the weight range
		val keys = items.keySet.toArray  // Get the key names from the map
		var n = 0
		var weightTotal = 0
		while (weightTotal + items(keys(n)) < rnd) {  // Find the item for the random value's weight group
			weightTotal += items(keys(n))
			n += 1
		}
		keys(n)  // Return the item name
	}

	def test(): Unit = {
		val paymentRates = Map("Card" -> 60, "Internet Banking" -> 10, "UPI" -> 5, "Wallet" -> 25)
		for(i <- 1 to 10)
			println(WeightedRandomizer(paymentRates))
	}
}