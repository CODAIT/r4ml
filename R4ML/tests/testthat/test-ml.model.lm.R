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

context("Testing hydrar.lm\n")

# test hydra linear model
# currently only the test for running is done but
# accuracy test can be done later
test_that("hydrar.lm direct", {
  require(SparkR)
  require(HydraR)
  data("airquality")
  aq_ozone <- airquality[c("Ozone", "Temp", "Wind")]
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_rdf <- as.data.frame(aq_ozone)
  aq_ozone_df <- as.hydrar.frame(aq_ozone_rdf)
  aq_ozone_mat <- as.hydrar.matrix(aq_ozone_df)
  train <- aq_ozone_mat
  aq_lm <- hydrar.lm(Ozone ~ . , data = train, method ="iterative")
  aq_lm_ds <- hydrar.lm(Ozone ~ . , data = train, method ="direct-solve")
  
  show(aq_lm)
  coef(aq_lm)
  stats(aq_lm)
})

# test hydra linear model
# currently only the test for running is done but
# accuracy test can be done later
test_that("hydrar.lm iterative", {
  require(SparkR)
  require(HydraR)
  data("airquality")
  aq_ozone <- airquality[c("Ozone", "Temp", "Wind")]
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_rdf <- as.data.frame(aq_ozone)
  aq_ozone_df <- as.hydrar.frame(aq_ozone_rdf)
  aq_ozone_mat <- as.hydrar.matrix(aq_ozone_df)
  train <- aq_ozone_mat
  aq_lm <- hydrar.lm(Ozone ~ . , data = train, method ="iterative")

  show(aq_lm)
  coef(aq_lm)
  stats(aq_lm)
})

test_that("hydrar.lm predict", {
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  s <- hydrar.sample(iris_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  y_test = as.hydrar.matrix(as.hydrar.frame(test[,1]))
  y_test = SparkR:::as.data.frame(y_test)
  test = as.hydrar.matrix(as.hydrar.frame(test[,c(2:5)]))
  iris_lm <- hydrar.lm(Sepal_Length ~ . , data = train, method ="iterative")
  output <- predict(iris_lm, test)
  expect_lt(mean(sapply(SparkR::as.data.frame(output[[1]])-y_test, abs)), 5)
})

test_that("hydrar.lm predict_scoring", {
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  s <- hydrar.sample(iris_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  y_test = as.hydrar.matrix(as.hydrar.frame(test[,1]))
  y_test = SparkR:::as.data.frame(y_test)
  test = as.hydrar.matrix(as.hydrar.frame(test[,c(2:5)]))
  iris_lm <- hydrar.lm(Sepal_Length ~ . , data = train, method ="iterative")
  output <- predict(iris_lm, test)
})
