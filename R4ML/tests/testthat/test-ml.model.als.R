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

context("Testing hydrar.als\n")

test_that("hydrar.als", {
  require(SparkR)
  require(HydraR)

  # Create a sample dataset: 4 items, 2 users
  df <- data.frame(X1=c(1, 0), X2=c(5, 2), "X3"=c(0, 1), "X4"=c(1, 0))
  bm <- as.hydrar.matrix(as.hydrar.frame(df))
  
  # Create a hydrar.als model
  als <- hydrar.als(data=bm, rank=2, reg='L2', lambda=.01, iter.max=50, tolerance=0.0001)
  
  # Predict ratings for a given input list of pairs (user-id, item-id)
  dfTest <- data.frame(userIndex=c(1, 1, 2, 2), itemIndex=c(4, 1, 2, 3))
  bmTest <- as.hydrar.matrix(as.hydrar.frame(dfTest))
  pred <- predict(als, bmTest)
  stopifnot(SparkR::collect(pred)[[1]][1] - 0.999999 < .01)
})

test_that("hydrar.als_regularization", {
  require(SparkR)
  require(HydraR)
  
  # Create a sample dataset: 4 items, 2 users
  df <- data.frame(X1=c(1, 0), X2=c(5, 2), "X3"=c(0, 1), "X4"=c(1, 0))
  bm <- as.hydrar.matrix(as.hydrar.frame(df))
  
  # Create a hydrar.als model
  als <- hydrar.als(data=bm, rank=2, reg.type="L2", lambda = .1)
  
  # Predict ratings for a given input list of pairs (user-id, item-id)
  dfTest <- data.frame(userIndex=c(1, 1, 2, 2), itemIndex=c(4, 1, 2, 3))
  bmTest <- as.hydrar.matrix(as.hydrar.frame(dfTest))
  pred <- predict(als, bmTest)
  stopifnot(SparkR::collect(pred)[[1]][1] - 0.9746597 < .01)
})

test_that("hydrar.als_weighted_regularization", {
  require(SparkR)
  require(HydraR)
  
  # Create a sample dataset: 4 items, 2 users
  df <- data.frame(X1=c(1, 0), X2=c(5, 2), "X3"=c(0, 1), "X4"=c(1, 0))
  bm <- as.hydrar.matrix(as.hydrar.frame(df))
  
  # Create a hydrar.als model
  als <- hydrar.als(data=bm, rank=2, reg.type="wL2", lambda = .1)
  
  # Predict ratings for a given input list of pairs (user-id, item-id)
  dfTest <- data.frame(userIndex=c(1, 1, 2, 2), itemIndex=c(4, 1, 2, 3))
  bmTest <- as.hydrar.matrix(as.hydrar.frame(dfTest))
  pred <- predict(als, bmTest)
  stopifnot(SparkR::collect(pred)[[1]][1] - 0.9747276 < .01)
})

test_that("hydrar.als_altered_rank", {
  require(SparkR)
  require(HydraR)
  
  # Create a sample dataset: 4 items, 2 users
  df <- data.frame(X1=c(1, 0), X2=c(5, 2), "X3"=c(0, 1), "X4"=c(1, 0))
  bm <- as.hydrar.matrix(as.hydrar.frame(df))
  
  # Create a hydrar.als model
  als <- hydrar.als(data=bm, rank=1)
  
  # Predict ratings for a given input list of pairs (user-id, item-id)
  dfTest <- data.frame(userIndex=c(1, 1, 2, 2), itemIndex=c(4, 1, 2, 3))
  bmTest <- as.hydrar.matrix(as.hydrar.frame(dfTest))
  pred <- predict(als, bmTest)
  stopifnot(SparkR::collect(pred)[[1]][1] - 1 < .01)
})

test_that("hydrar.als_all_parameters", {
  require(SparkR)
  require(HydraR)
  
  # Create a sample dataset: 4 items, 2 users
  df <- data.frame(X1=c(1, 0), X2=c(5, 2), "X3"=c(0, 1), "X4"=c(1, 0))
  bm <- as.hydrar.matrix(as.hydrar.frame(df))
  
  # Create a hydrar.als model
  als <- hydrar.als(data=bm, rank=2, reg='L2', lambda=.01, iter.max=50, tolerance=0.0001)
  
  # Predict ratings for a given input list of pairs (user-id, item-id)
  dfTest <- data.frame(userIndex=c(1, 1, 2, 2), itemIndex=c(4, 1, 2, 3))
  bmTest <- as.hydrar.matrix(as.hydrar.frame(dfTest))
  pred <- predict(als, bmTest)
  stopifnot(SparkR::collect(pred)[[1]][1] - 0.999999 < .01)
})
