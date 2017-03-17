#
# (C) Copyright IBM Corp. 2017
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

context("Testing r4ml.step.lm\n")

test_that("r4ml.step.lm direct", {
  data("iris")

  r4ml_iris <- as.r4ml.matrix(iris[, -5])
  step_lm <- r4ml.step.lm(Sepal_Length ~ ., data = r4ml_iris)
  coef(step_lm)
  stats(step_lm)
})

test_that("r4ml.step.lm predict", {
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.r4ml.frame(df)
  iris_mat <- as.r4ml.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal")
  s <- r4ml.sample(iris_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  y_test <- as.r4ml.matrix(test[, 1])
  y_test <- SparkR:::as.data.frame(y_test)
  test <- as.r4ml.matrix(test[, c(2:5)])
  iris_step_lm <- r4ml.step.lm(Sepal_Length~ . , data = train)
  output <- predict(iris_step_lm, test)
  expect_true( base::mean(sapply(SparkR::as.data.frame(output[[1]])-y_test, abs)) < 5)
})

test_that("r4ml.step.lm predict_scoring", {
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.r4ml.frame(df)
  iris_mat <- as.r4ml.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal")
  s <- r4ml.sample(iris_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  y_test <- as.r4ml.matrix(test[,1])
  y_test <- SparkR:::as.data.frame(y_test)
  test <- as.r4ml.matrix(test[, c(2:5)])
  iris_step_lm <- r4ml.step.lm(Sepal_Length ~ ., data=train)
  output <- predict(iris_step_lm, test)
  expect_true( mean(sapply(SparkR::as.data.frame(output[[1]])-y_test, abs)) < 5)
})
