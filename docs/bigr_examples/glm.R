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
invisible(bigr.rmfs("/user/bigr/examples/glm/*"))

# Apply required transformations for Machine Learning
airlineMatrix <- bigr.transform(airline, outData="/user/bigr/examples/glm/airline.mtx",
                                applyTransformPath="/user/bigr/examples/airline.transform",
                                transformPath="/user/bigr/examples/glm/airline.transform")

# split the data to train and test sets
samples <- bigr.sample(airlineMatrix, perc=c(0.7, 0.3))
train <- samples[[1]]
test <- samples[[2]]

# Create a generalized linear model
glm <- bigr.glm(DepTime ~ ., data=train, directory="/user/bigr/examples/glm/model",
                family=gaussian(identity))

# Get the coefficients of the regression
glm

# Calculate predictions for the testing set
pred <- predict(glm, test, "/user/bigr/examples/glm/preds")

pred