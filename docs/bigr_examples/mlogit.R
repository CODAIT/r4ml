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
invisible(bigr.rmfs("/user/bigr/examples/mlogit/*"))

# Apply required transformations for Machine Learning
airlineMatrix <- bigr.transform(airline, outData="/user/bigr/examples/mlogit/airline.mtx",
                                applyTransformPath="/user/bigr/examples/airline.transform",
                                transformPath="/user/bigr/examples/mlogit/airline.transform")

# split the data to train and test sets
samples <- bigr.sample(airlineMatrix, perc=c(0.7, 0.3))
train <- samples[[1]]
test <- samples[[2]]

# Create a linear regression model
mlogit <- bigr.mlogit(UniqueCarrier ~ ., data=train, directory="/user/bigr/examples/mlogit/model",
                      intercept=T, shiftAndRescale=T)

# Get the coefficients of the regression
mlogit

# Calculate predictions for the testing set
pred <- predict(mlogit, test, "/user/bigr/examples/mlogit/preds")

pred