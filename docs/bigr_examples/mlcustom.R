# Load library
library(bigr)

# Connect to remote
bigr.connect("bigr1.fyre.ibm.com","bigr","bigr",ssl=T)

# load dataset to a bigr.frame
airline <- bigr.frame(dataSource="DEL", 
                      dataPath="/user/bigr/tutorial/airline_demo.csv", delimiter=",", 
                      coltypes=ifelse(1:29 %in% c(9,11,17,18,23), "character", "integer"),
                      header=T)

# filter out the column that has many missing values
airline <- airline[, -23]

# Remove files from previous executions (if any)
invisible(bigr.rmfs("/user/bigr/examples/svm/*"))

# Apply required transformations for Machine Learning
airlineMatrix <- bigr.transform(airline, outData="/user/bigr/examples/svm/airline.mtx",
                                applyTransformPath="/user/bigr/examples/airline.transform",
                                transformPath="/user/bigr/examples/svm/airline.transform")

# Write your own custom dml script and put it in the HDFS or the Big R master node's algorithm path
# assuming it is utils/head.dml

# Run the dml
bigr.execute("utils/head.dml", scriptOnDFS=F, x=airlineMatrix@dataPath, n=5, o="/user/bigr/examples/head.mtx")

# Load the matrix
headBM <- bigr.matrix("/user/bigr/examples/head.mtx", useMapReduce=F)
headBM
