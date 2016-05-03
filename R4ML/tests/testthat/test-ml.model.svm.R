#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#library (HydraR)

context("Testing hydrar.svm\n")

# @NOTE: this will eventually be the function on SparkR
# for now this is the helper function
# we will also have the transform functionality later
recode <- function (x) {
  if (typeof(x) == "list") {
    x <- x[[1]]
  } 
  v <- as.vector(x)
  f <- as.factor(v)
  levels(f) <- 1:length(levels(f))
  r <- as.numeric(f)
  r
}
# test hydra linear model
# currently only the test for running is done but
# accuracy test can be done later
# this is multi class svm
test_that("hydrar.svm direct", {
  require(SparkR)
  require(HydraR)
  data("iris")
  iris_inp <- as.data.frame(iris)
  iris_inp$Species <- recode(iris_inp$Species)
  iris_inp_df <- as.hydrar.frame(iris_inp)
  iris_inp_mat <- as.hydrar.matrix(iris_inp_df)
  #@TODO to change to derive the nominal automatically
  rsplit <- hydrar.sample(iris_inp_mat, c(0.7, 0.3))
  #browser()
  train_iris_inp_mat <- rsplit[[1]]
  test_iris_inp_mat <- rsplit[[2]]
  ml.coltypes(train_iris_inp_mat) <- c("scale", "scale", "scale", "scale", "nominal")
  iris_svm <- hydrar.svm(Species ~ . , data = train_iris_inp_mat)
  
  preds <- predict(iris_svm, test_iris_inp_mat)
  
  # Get the scores                 
  preds$scores
  
  # Get the confusion matrix
  preds$ctable
              
  # Get the overall accuracy
  preds$accuracy
                   
  cat("testing svm done")
})

# test hydra linear model
# currently only the test for running is done but
# accuracy test can be done later
# this is binary class svm
test_that("hydrar.svm direct", {
  require(SparkR)
  require(HydraR)
  data("iris")
  iris_inp <- as.data.frame(iris)
  iris_inp$Species <- recode(iris_inp$Species)
  iris_inp$Species[iris_inp$Species == 3] <- 2 # convert 3 classes to 2 classes
  iris_inp_df <- as.hydrar.frame(iris_inp)
  iris_inp_mat <- as.hydrar.matrix(iris_inp_df)
  #@TODO to change to derive the nominal automatically
  rsplit <- hydrar.sample(iris_inp_mat, c(0.7, 0.3))
  
  train_iris_inp_mat <- rsplit[[1]]
  test_iris_inp_mat <- rsplit[[2]]
  ml.coltypes(train_iris_inp_mat) <- c("scale", "scale", "scale", "scale", "nominal")
  iris_svm <- hydrar.svm(Species ~ . , data = train_iris_inp_mat, is.binary.class = TRUE)
  
  preds <- predict(iris_svm, test_iris_inp_mat)
  
  # Get the scores                 
  preds$scores
  
  # Get the confusion matrix
  preds$ctable
  
  # Get the overall accuracy
  preds$accuracy
  
  cat("testing svm done")
  
})
