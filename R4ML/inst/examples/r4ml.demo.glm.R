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

#load the hydrar
library(HydraR)
hydrar.session()

# these are the different dataset we will analysis
# path <- "/user/data-scientist/airline/1987.csv"
# df <- read.df(path, source="csv", inferSchema="true", header="true")
# df <- HydraR:::hydrar.read.csv(path, inferSchema=TRUE, header=TRUE)

# this example use the airline dataset shipped with HydraR
df <- as.hydrar.frame(airline)

# Temporary as a workaround: remove CancellationCode from dataset
# https://ibm-jira.col.eslabs.ibm.com:8443/browse/REQS-177
names <- colnames(df)
to_remove = c("CancellationCode")
selected_columns <- names[(names!=to_remove)]
df <- as.hydrar.frame(select(df, selected_columns))

# we would like to limit the dataset to a size so that we can run test faster
#df_max_size <- 1000
df_max_size <- 100000

# limit the number of rows so that we can control the size
df <- limit(df, df_max_size) # results in a SparkDataFrame
ignore <- cache(df)  # very important step otherwise the partition gets screw up

# convert to hydrar.frame again
df <- as.hydrar.frame(df)

# do the preprocess of the data set
df_trans <- hydrar.ml.preprocess(
  df, transformPath="/tmp",
  recodeAttrs=c("UniqueCarrier", "TailNum", "Origin", "Dest"),
  omit.na=c("UniqueCarrier", "TailNum", "Origin", "Dest"))

# sample the dataset into the train and test
samples <- hydrar.sample(df_trans$data, perc=c(0.7, 0.3))
train <- samples[[1]]
ignore <- cache(train)
test <- samples[[2]]
ignore <- cache(test)

# train the glm model by default it is the binomial
train_m <- as.hydrar.matrix(train)
glm <- hydrar.glm(DepTime ~ ., train_m)
glm

# run the prediction
test_m <- as.hydrar.matrix(test)
pred <- predict(glm, test_m)
# To print all outputs, just call pred
head(pred$predictions)
pred$statistics

# exit
hydrar.session.stop()
quit("no")
