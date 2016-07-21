#
# (C) Copyright IBM Corp. 2015, 2016
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

context("Testing hydrar.step.lm\n")

test_that("hydrar.step.lm direct", {
  require(SparkR)
  require(HydraR)
  data("iris")

  hydra_iris <- as.hydrar.matrix(as.hydrar.frame(iris[,-5]))
  step_lm <- hydrar.step.lm(Sepal_Length ~ ., data = hydra_iris)
  coef(step_lm)
  stats(step_lm)
})

test_that("hydrar.step.lm predict", {
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
  y_test <- as.hydrar.matrix(as.hydrar.frame(test[,1]))
  y_test <- SparkR:::as.data.frame(y_test)
  test <- as.hydrar.matrix(as.hydrar.frame(test[,c(2:5)]))
  iris_step_lm <- hydrar.step.lm(Sepal_Length~ . , data = train)
  output <- predict(iris_step_lm, test)
  expect_true( base::mean(sapply(SparkR::as.data.frame(output[[1]])-y_test, abs)) < 5)
})

test_that("hydrar.step.lm predict_scoring", {
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
  y_test <- as.hydrar.matrix(as.hydrar.frame(test[,1]))
  y_test <- SparkR:::as.data.frame(y_test)
  test <- as.hydrar.matrix(as.hydrar.frame(test[,c(2:5)]))
  iris_step_lm <- hydrar.step.lm(Sepal_Length ~ ., data=train)
  output <- predict(iris_step_lm, test)
  expect_true( mean(sapply(SparkR::as.data.frame(output[[1]])-y_test, abs)) < 1)
})
