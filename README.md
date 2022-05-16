# taxitrip app to solve the moody nyc taxitrip data analysis problem

to run this program:

java -DtaxiTrip.gridSize.latLngGridSize=2 -DtaxiTrip.gridSize.dateTimeGridSize=2 -DtaxiTrip.inputFile.tripData=/Users/upu/moody/green_tripdata_2022-02.csv -DtaxiTrip.inputFile.zoneData=/Users/upu/moody/taxi_zones.csv -jar target/scala-2.13/taxitrip-assembly-0.1.0-SNAPSHOT.jar com.ypu.taxitrip.TaxiTrip::main

Using the green data, and 10x10X10 grid, I got the answer below:

