#
# (C) Copyright IBM Corp. 2017
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# load library R4ML
library(R4ML)
r4ml.session()

# example paths for custom dataset
# path = "/user/data-scientist/airline/1987.csv"
#path = "/user/data-scientist/airline/*1987.csv"
# df <- R4ML:::r4ml.read.csv(path, inferSchema=TRUE, header=TRUE)

# we would like to limit the dataset to a size so that we can run test faster
# df_max_size <- 100
df_max_size <- 100000

# set the predictors and response variables
predictors <- c("Month", "DayofMonth", "DayOfWeek", "Year")
response <- c("Cancelled")
D_names <- append(predictors, response)

# read in data
airline_hf = as.r4ml.frame(airline)

# limit the number of rows so that we can control the size
airline_hf <- limit(airline_hf, df_max_size)
ignore <- cache(airline_hf)

#convert to r4ml frame
airline_hf <- SparkR::select(airline_hf, D_names)
airline_hf <- as.r4ml.frame(airline_hf)
ignore <- cache(airline_hf)

# preprocessing
airline_transform <- r4ml.ml.preprocess(
  airline_hf,
  transformPath = "/tmp",
  recodeAttrs=D_names,
  #dummycodeAttrs = c("Month"),
  binningAttrs = c(), numBins=4,
  #missingAttrs = D_names,
  omit.na = D_names
)

# sample dataset into train and test
sampled_data <- r4ml.sample(airline_transform$data, perc=c(0.7, 0.3))
# collect metadata
metadata <- airline_transform$metadata
train <- as.r4ml.matrix(sampled_data[[1]])
ignore <- cache(train)

# create logistic regression model
ml.coltypes(train) <- c(rep("scale", length(names(train))-1), "nominal")
logistic_regression = r4ml.mlogit(Cancelled ~ ., data=train)

# compute predictions on new data
test <- as.r4ml.matrix(sampled_data[[2]])
ignore <- cache(test)
pred <- predict(logistic_regression, test)
# To print all outputs, just call pred
pred$probabilities[1:10,]
pred$statistics


# exit R/R4ML
r4ml.session.stop()
quit("no")
