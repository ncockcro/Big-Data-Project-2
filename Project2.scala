// Written by: Nicholas Cockcroft
// Date: July 29, 2018

// Assignment: Project 2

// Description: Takes in the input of ip addresses, time stamps, source packet time, destination packet time and first maps the timestamp to 
// those values and finds the highest source or destination times. Then, map the ip address from the time stamps to a constant of 1 and counts how
// many times an ip address appears. Then take the max of the counts and output the ip address that appeared the most times.

val file = sc.textFile("/home/nick/input/UNSW-NB15_1.txt") // Open the file and convert to rrd
val token = file.map(x=>x.split(",")) // Split on comma
val keyval = token.map(x=>(x(28), (x(30).toDouble, x(31).toDouble, x(0)))) // Key-timestamp   Values-Source packet time, destination time, ip address
val maxTime = keyval.reduceByKey{case(a,b)=> if(a._1 > b._1 || a._2 > b._2) {a} else {b}} // Reduces the time stamps to having the lowest 


val token2 = maxTime.map(x=>x.toString.split("[(,)]")) // Going through each time stamp and splitting on parenthesis and commas
val keyval2 = token2.map(x=>(x(5), 1)) // Mapping the ip address as the key and a constant 1 as the value
val counts = keyval2.reduceByKey{case(a,b)=>a+b} // reducing the keys and incrementing the count each time the same ip is found

val maxIp = counts.values.max // Finding the ip that had the highest count

val ddos = counts.filter{case(a,b)=> b == maxIp} // Finding the ip address with the same count as maxIp

ddos.collect.foreach(println) // Outputting the ddoser's ip and count
