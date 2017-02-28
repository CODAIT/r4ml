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
#library (HydraR)

context("Testing hydrar.pca\n")



# test PCA feature extractor

test_that("hydrar.pca projDataDefault ", {
  data("iris")
  irisData <- (iris[, -5])
  iris_dataframe <- as.data.frame(irisData)
  iris_hframe <- as.hydrar.frame(iris_dataframe)
  iris_hmat <- as.hydrar.matrix(iris_hframe)
  train <- iris_hmat
  
  # Test with default projData
  k = 2
  iris_pca <- hydrar.pca(train, center=T, scale=T, k=k)
  expect_true(nrow(iris_pca$projData) > 0)
  expect_equal(ncol(iris_pca$projData), k)
  show(iris_pca$model)
  
})

test_that("hydrar.pca profDataFalse", {
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
  expect_true(nrow(iris_pca$projData) > 0)
  expect_equal(ncol(iris_pca$projData), k)
  
})

test_that("hydrar.pca k_lessThan_features", {
  data("iris")
  irisData <- (iris[, -5])
  iris_dataframe <- as.data.frame(irisData)
  iris_hframe <- as.hydrar.frame(iris_dataframe, repartition = FALSE)
  iris_hmat <- as.hydrar.matrix(iris_hframe)
  train <- iris_hmat
  
  # Test with k set to more than number of features
  k=5
  expect_that(hydrar.pca(train, center=T, scale=T, k=k, projData=T), throws_error("k must be less"))
  
})

test_that("hydrar.pca accuracy", {
  
  #Load Iris dataset to HDFS
  train <- as.hydrar.matrix(iris[, -5]) 
  
  #Create a hydrar.pca model on Iris
  h_pca <- hydrar.pca(train, center = TRUE, scale = TRUE, projData = FALSE)
  
  #Get the eigen vectors from hydrar pca model
  h_pca_ev <- h_pca@eigen.vectors
  
  #Create Pca model using prcomp
  train <- data.matrix(iris[, -5])
  r_pca <- prcomp(train, center = TRUE, scale = TRUE)
  
  #Get the eigen vectors from r pca model
  r_pca_ev <- data.frame(r_pca$rotation)
  
  #Comparing eigen vectors
  cols <- SparkR::ncol(h_pca_ev)
  
  for(cnt in 1:cols){
    temp_hydra_vector <- as.vector(h_pca_ev[, cnt])
    temp_r_vector <- as.vector(r_pca_ev[, cnt])
    
    #Since the Eigen Vectors obtained by different algorithms maybe a mirror  
    #images of each other, we are ensuring that they are parallel to each other 
    expect_true(isTRUE(all.equal(temp_hydra_vector,temp_r_vector)) || 
                  isTRUE(all.equal(temp_hydra_vector,-temp_r_vector)))
  }
  
  #Comparing Standard deviations
  h_pca_sd <- h_pca@std.deviations
  r_pca_sd <- r_pca$sdev
  expect_true(isTRUE(all.equal(as.vector(h_pca_sd[, 1]), r_pca_sd)))
  
})

