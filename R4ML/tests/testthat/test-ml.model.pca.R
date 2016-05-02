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

context("Testing hydrar.pca\n")



# test PCA feature extractor

test_that("hydrar.pca projDataDefault ", {
  require(SparkR)
  require(HydraR)
  data("iris")
  irisData <- (iris[, -5])
  iris_dataframe <- as.data.frame(irisData)
  iris_hframe <- as.hydrar.frame(iris_dataframe)
  iris_hmat <- as.hydrar.matrix(iris_hframe)
  train <- iris_hmat
  
  # Test with default projData
  k = 2
  iris_pca <- hydrar.pca(train, center=T, scale=T, k=k)
  stopifnot(nrow(iris_pca$projData) > 0, ncol(iris_pca$projData) == k)
  show(iris_pca$model)
  
})

test_that("hydrar.pca profDataFalse", {
  require(SparkR)
  require(HydraR)
  data("iris")
  irisData <- (iris[, -5])
  iris_dataframe <- as.data.frame(irisData)
  iris_hframe <- as.hydrar.frame(iris_dataframe)
  iris_hmat <- as.hydrar.matrix(iris_hframe)
  train <- iris_hmat
  
  # Test with projData set to False
  iris_pca <- hydrar.pca(train, center=T, scale=T, k=2, projData=F)
  show(iris_pca)
  #stopifnot(nrow(iris_pca@newA) == 0, ncol(iris_pca@newA) == 0)
})

test_that("hydrar.pca profDataTrue", {
  require(SparkR)
  require(HydraR)
  data("iris")
  irisData <- (iris[, -5])
  iris_dataframe <- as.data.frame(irisData)
  iris_hframe <- as.hydrar.frame(iris_dataframe)
  iris_hmat <- as.hydrar.matrix(iris_hframe)
  train <- iris_hmat
  
  # Test with projData set to True
  k=2
  iris_pca <- hydrar.pca(train, center=T, scale=T, k=k, projData=T)
  show(iris_pca$model)
  stopifnot(nrow(iris_pca$projData) > 0, ncol(iris_pca$projData) == k)
  
})

test_that("hydrar.pca k_lessThan_features", {
  require(SparkR)
  require(HydraR)
  data("iris")
  irisData <- (iris[, -5])
  iris_dataframe <- as.data.frame(irisData)
  iris_hframe <- as.hydrar.frame(iris_dataframe)
  iris_hmat <- as.hydrar.matrix(iris_hframe)
  train <- iris_hmat
  
  # Test with k set to more than number of features
  k=5
  expect_that(hydrar.pca(train, center=T, scale=T, k=k, projData=T), throws_error("k must be less"))
  
})



