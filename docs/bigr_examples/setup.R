# Load library
library(bigr)

# Connect to remote cluster
bigr.connect("bigr1.fyre.ibm.com","bigr","bigr",ssl=T)

# Persist airline demo dataset
airfile <- system.file("extdata", "airline.zip", package="bigr")
airfile <- unzip(airfile, exdir = tempdir())
airR <- read.csv(airfile, stringsAsFactors=F)
airline <- as.bigr.frame(airR)
bigr.persist(airline, dataSource="DEL",
             dataPath="/user/bigr/tutorial/airline_demo.csv", header=T, 
             delimiter=",", useMapReduce=F)

# configure server to run 4 reduce tasks
bigr.set.server.option("mapred.reduce.tasks", 4)

# Filter out the column that has many missing values
airline <- airline[, -23]

# Find out which columns have missing values
missing <- bigr.which.na.cols(airline)

# Apply required transformations for Machine Learning
airlineMatrix <- bigr.transform(airline, outData="/user/bigr/examples/airline.mtx",
                                recodeAttrs=c("UniqueCarrier", "TailNum", "Origin", "Dest"),
                                missingAttrs=missing, imputationMethod="global_mode",
                                omit.na=c("UniqueCarrier", "TailNum", "Origin", "Dest"),
                                transformPath="/user/bigr/examples/airline.transform")

# Disconnect
bigr.disconnect()

# exit
quit("no")