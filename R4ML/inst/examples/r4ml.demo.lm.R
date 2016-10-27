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

# read in data
airline_hf <- as.hydrar.frame(airline)

# remove CancellationCode from dataset
names <- colnames(airline_hf)
to_remove = c("CancellationCode")
selected_columns <- names[(names!=to_remove)]
airline_hf <- as.hydrar.frame(select(airline_hf, selected_columns))

# limit the number of rows so that we can control the size
airline_hf <- limit(airline_hf, df_max_size)
airline_hf <- cache(airline_hf)

#convert to hydrar frame
airline_hf <- as.hydrar.frame(airline_hf)

# do the preprocessing of the data set
airline_transform <- hydrar.ml.preprocess(
  airline_hf, transformPath="/tmp",
  recodeAttrs=c("UniqueCarrier", "TailNum", "Origin", "Dest", "Month", "DayOfWeek"),
  omit.na=c("UniqueCarrier", "TailNum", "Origin", "Dest"),
  dummycodeAttrs = c("Month")
  # dummycodeAttrs=c("UniqueCarrier", "TailNum", "Origin", "Dest", "Month", "DayOfWeek")
  )

# sample dataset into train and test

# actual data
sampled_data <- hydrar.sample(airline_transform$data, perc=c(0.7, 0.3))
# metadata to look at the decode value and other attributes
metadata <- airline_transform$metadata

train <- as.hydrar.matrix(sampled_data[[1]])
test <- as.hydrar.matrix(sampled_data[[2]])
ignore <- cache(train)
ignore <- cache(test)

# train the lm model
lm <- hydrar.lm(DepTime ~ ., train)

# run the prediction
test <- as.hydrar.matrix(test)
pred <- predict(lm, test)
# To print all outputs, just call pred
head(pred$predictions)
pred$statistics

# exit R/HydraR
quit("no")


