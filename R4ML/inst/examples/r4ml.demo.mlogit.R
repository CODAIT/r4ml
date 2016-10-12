#
# (C) Copyright IBM Corp. 2015, 2016
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

# load library HydraR
library(HydraR)

# example paths for custom dataset
# path = "/user/data-scientist/airline/1987.csv"
#path = "/user/data-scientist/airline/*1987.csv"
# df <- HydraR:::hydrar.read.csv(path, inferSchema=TRUE, header=TRUE)

# we would like to limit the dataset to a size so that we can run test faster
# df_max_size <- 1000
df_max_size <- 100000

# set the predictors and response variables
predictors <- c("Month", "DayofMonth", "DayOfWeek", "Year")
response <- c("Cancelled")
D_names <- append(predictors, response)

# read in data
airline_hf = as.hydrar.frame(airline)

# limit the number of rows so that we can control the size
airline_hf <- limit(airline_hf, df_max_size)
cache(airline_hf)

#convert to hydrar frame
airline_hf <- SparkR::select(airline_hf, D_names)
airline_hf <- as.hydrar.frame(airline_hf)
cache(airline_hf)

# preprocessing
airline_transform <- hydrar.ml.preprocess(
  airline_hf,
  transformPath = "/tmp",
  recodeAttrs=D_names,
  #dummycodeAttrs = c("Month"),
  binningAttrs = c(), numBins=4,
  #missingAttrs = D_names,
  omit.na = D_names
)

# sample dataset into train and test
sampled_data <- hydrar.sample(airline_transform$data, perc=c(0.7, 0.3))
# collect metadata
metadata <- airline_transform$metadata
train <- as.hydrar.matrix(sampled_data[[1]])
cache(train)

# create logistic regression model
ml.coltypes(train) <- c(rep("scale", length(names(train))-1), "nominal")
logistic_regression = hydrar.mlogit(Cancelled ~ ., data=train)

# compute predictions on new data
test <- as.hydrar.matrix(sampled_data[[2]])
cache(test)
pred <- predict(logistic_regression, test)
pred

# exit R/HydraR
quit("no")
