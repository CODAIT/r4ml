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
#df_max_size <- 1000
df_max_size <- 100000

# read in data
airline_hf <- as.hydrar.frame(airline)

# remove CancellationCode from dataset
names <- colnames(airline_hf)
to_remove <- c("CancellationCode", "FlightNum", "TailNum", "Cancelled", "Diverted", "DepTime", "CRSDepTime", "CarrierDelay",
               "CRSArrTime", "ArrDelay", "DepDelay", "SecurityDelay", "LateAircraftDelay", "NASDelay", "WeatherDelay",
              "LateAircraftDelay", "WeatherDelay", "Origin", "Dest", "ArrTime")

selected_columns <- names[!(names %in% to_remove)]
airline_hf <- as.hydrar.frame(select(airline_hf, selected_columns))

# limit the number of rows so that we can control the size
airline_hf <- limit(airline_hf, df_max_size)
airline_hf <- cache(airline_hf)

#convert to hydrar frame
airline_hf <- as.hydrar.frame(airline_hf)

# do the preprocessing of the data set
airline_transform <- hydrar.ml.preprocess(
  airline_hf,
  transformPath = "/tmp",
  recodeAttrs = c("Month", "DayOfWeek"),
  omit.na = c("ActualElapsedTime"),
  dummycodeAttrs = c("UniqueCarrier") # one hot encoding
  )

# sample dataset into train and test

# actual data
sampled_data <- hydrar.sample(airline_transform$data, perc = c(0.7, 0.3))
# metadata to look at the decode value and other attributes
metadata <- airline_transform$metadata

train <- as.hydrar.matrix(sampled_data[[1]])
test <- as.hydrar.matrix(sampled_data[[2]])
cache(train)
cache(test)

# change coltypes to scale
ml.coltypes(train) <- rep("scale", ncol(train))

# train the step.lm model
step_lm <- hydrar.step.lm(formula = ActualElapsedTime ~ .,
                          data = train,
                          intercept = FALSE,
                          shiftAndRescale = FALSE,
                          threshold = 0.001,
                          directory = "~/"
                          )

coef(step_lm)

# run the prediction
test <- as.hydrar.matrix(test)
pred <- predict(step_lm, test)
# To print all outputs, just call pred
head(pred$predictions)
pred$statistics

# exit R
quit("no")
