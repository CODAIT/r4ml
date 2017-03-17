#
# (C) Copyright IBM Corp. 2017
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

context("Testing r4ml.lm\n")

# test r4ml linear model
# currently only the test for running is done but
# accuracy test can be done later
test_that("r4ml.lm direct", {
  data("airquality")
  aq_ozone <- airquality[c("Ozone", "Temp", "Wind")]
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_rdf <- as.data.frame(aq_ozone)
  aq_ozone_df <- as.r4ml.frame(aq_ozone_rdf)
  aq_ozone_mat <- as.r4ml.matrix(aq_ozone_df)
  train <- aq_ozone_mat
  aq_lm <- r4ml.lm(Ozone ~ . , data = train, method ="iterative")
  aq_lm_ds <- r4ml.lm(Ozone ~ . , data = train, method ="direct-solve")
  
  show(aq_lm)
  coef(aq_lm)
  stats(aq_lm)
})

# test r4ml linear model
# currently only the test for running is done but
# accuracy test can be done later
test_that("r4ml.lm iterative", {
  data("airquality")
  aq_ozone <- airquality[c("Ozone", "Temp", "Wind")]
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_rdf <- as.data.frame(aq_ozone)
  aq_ozone_df <- as.r4ml.frame(aq_ozone_rdf)
  aq_ozone_mat <- as.r4ml.matrix(aq_ozone_df)
  train <- aq_ozone_mat
  aq_lm <- r4ml.lm(Ozone ~ . , data = train, method ="iterative")

  show(aq_lm)
  coef(aq_lm)
  stats(aq_lm)
})

test_that("r4ml.lm predict", {
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.r4ml.frame(df)
  iris_mat <- as.r4ml.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  s <- r4ml.sample(iris_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  y_test <- as.r4ml.matrix(test[, 1])
  y_test = SparkR:::as.data.frame(y_test)
  test <- as.r4ml.matrix(test[, c(2:5)])
  iris_lm <- r4ml.lm(Sepal_Length ~ . , data = train, method ="iterative")
  output <- predict(iris_lm, test)
})

test_that("r4ml.lm predict_scoring", {
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.r4ml.frame(df, repartition = FALSE)
  iris_mat <- as.r4ml.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  s <- r4ml.sample(iris_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  y_test <- as.r4ml.matrix(test[,1])
  y_test = SparkR:::as.data.frame(y_test)
  test <- as.r4ml.matrix(test[, c(2:5)])
  iris_lm <- r4ml.lm(Sepal_Length ~ . , data = train, method ="iterative")
  output <- predict(iris_lm, test)
  #stopifnot(mean(sapply(SparkR::as.data.frame(output[[1]])-y_test, abs)) - 5 < .001)		
  expect_true(mean(sapply(SparkR::as.data.frame(output[[1]])-y_test, abs)) - 5 < .001)		
})

test_that("r4ml.lm accuracy", {

  hf <- as.r4ml.frame(datasets::cars)
  hf_transform <- r4ml.ml.preprocess(hf, transformPath = tempdir())

  hf_transform$data <- as.r4ml.matrix(hf_transform$data)

  r4ml_lm <- r4ml.lm(dist ~ ., hf_transform$data, intercept = FALSE)
  r4ml_lm_icpt <- r4ml.lm(dist ~ ., hf_transform$data, intercept = TRUE)

  expect_false(r4ml_lm@intercept)
  expect_true(r4ml_lm_icpt@intercept)

  r_lm <- lm(dist ~ speed - 1, cars)
  r_lm_icpt <- lm(dist ~ speed, cars)

  expect_equal(coef(r4ml_lm)[[1]], coefficients(r_lm)[[1]], tolerance = .01)

  expect_equal(coef(r4ml_lm_icpt)["speed", "beta_out"],
               coef(r_lm_icpt)["speed"][[1]], tolerance = .01)

  expect_equal(coef(r4ml_lm_icpt)[r4ml.env$INTERCEPT, "beta_out"],
               coef(r_lm_icpt)[r4ml.env$INTERCEPT][[1]], tolerance = .01)

  r4ml_pred <- predict.r4ml.lm(r4ml_lm, hf_transform$data)
  r4ml_pred <- SparkR::as.data.frame(r4ml_pred$predictions)

  r4ml_pred_icpt <- predict.r4ml.lm(r4ml_lm_icpt, hf_transform$data)
  r4ml_pred_icpt <- SparkR::as.data.frame(r4ml_pred_icpt$predictions)

  r_pred <- predict.lm(r_lm, datasets::cars)
  r_pred <- as.data.frame(r_pred)

  r_pred_icpt <- predict.lm(r_lm_icpt, datasets::cars)
  r_pred_icpt <- as.data.frame(r_pred_icpt)

  expect_equal(r4ml_pred$preds, r_pred$r_pred)
  expect_equal(r4ml_pred_icpt$preds, r_pred_icpt$r_pred)

})
