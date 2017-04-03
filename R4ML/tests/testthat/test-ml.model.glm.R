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
#library (R4ML)

context("Testing r4ml.glm\n")

# test r4ml GLM model
test_that("r4ml.glm", {
  data("airquality")
  aq_ozone <- airquality[c("Ozone", "Temp", "Wind")]
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_rdf <- as.data.frame(aq_ozone)
  aq_ozone_df <- as.r4ml.frame(aq_ozone_rdf, repartition = FALSE)
  aq_ozone_mat <- as.r4ml.matrix(aq_ozone_df)
  train <- aq_ozone_mat
  aq_glm <- r4ml.glm(Ozone ~ . , data = train)
  coef(aq_glm)
  stats(aq_glm)
  
  aq_glm <- r4ml.glm(Ozone ~ . , data = train, intercept = T)
  coef(aq_glm)
  stats(aq_glm)
  
  aq_glm <- r4ml.glm(Ozone ~ ., data=train, family=poisson(log))
  coef(aq_glm)
  stats(aq_glm)
  
  aq_glm <- r4ml.glm(Ozone ~ ., data=train, family=poisson(log), shiftAndRescale = T, intercept = T)
  coef(aq_glm)
  stats(aq_glm)
})

test_that("predict.r4ml.glm", {
  data("airquality")
  aq_ozone <- airquality[c("Ozone", "Temp", "Wind")]
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_rdf <- as.data.frame(aq_ozone)
  aq_ozone_df <- as.r4ml.frame(aq_ozone_rdf, repartition = FALSE)
  aq_ozone_mat <- as.r4ml.matrix(aq_ozone_df)
  s <- r4ml.sample(aq_ozone_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  test1 <- test[,c(2,3)]
  test2 <- as.r4ml.matrix(test1)
  
  aq_glm <- r4ml.glm(Ozone ~ . , data = train, lambda=1.1)
  predict(aq_glm, test)
  
  a <- predict(aq_glm, test2)
  collect(a$predictions)

  aq_glm <- r4ml.glm(Ozone ~ . , data = train, intercept = T)
  predict(aq_glm, test)
  
  a <- predict(aq_glm, test2)
  collect(a$predictions)

  aq_glm <- r4ml.glm(Ozone ~ ., data=train, family=poisson(log))
  predict(aq_glm, test)
  
  a <- predict(aq_glm, test2)
  collect(a$predictions)
  
  aq_glm <- r4ml.glm(Ozone ~ ., data=train, family=poisson(log), shiftAndRescale = T, intercept = T)
  predict(aq_glm, test)
  
  a <- predict(aq_glm, test2)
  collect(a$predictions)
})

test_that("r4ml.glm accuracy", {
  hf <- as.r4ml.frame(datasets::cars)
  hf_transform <- r4ml.ml.preprocess(hf, transformPath = tempdir())
  hf_transform$data <- as.r4ml.matrix(hf_transform$data)

  h_glm <- r4ml.glm(formula = dist ~ ., data = hf_transform$data,
                      intercept = FALSE)
  r_glm <- glm(formula = dist ~ . - 1, data = datasets::cars)

  expect_equal(coef(h_glm)[[1]], coef(r_glm)[[1]], tol = .01)

  h_glm_icpt <- r4ml.glm(formula = dist ~ ., data = hf_transform$data,
                           intercept = TRUE)
  r_glm_icpt <- glm(formula = dist ~ ., data = datasets::cars)

  expect_equal(h_glm_icpt@coefficients$`(Intercept)`,
               r_glm_icpt$coefficients[["(Intercept)"]], tol = .01)

  expect_equal(h_glm_icpt@coefficients$speed,
               r_glm_icpt$coefficients[["speed"]], tol = .01)

  h_predict <- predict.r4ml.glm(h_glm, hf_transform$data)
  r_predict <- predict.glm(r_glm, datasets::cars)

  expect_equal(SparkR::as.data.frame(h_predict$predictions)$means,
               as.data.frame(r_predict)$r_predict, tol = .01)

  h_predict_icpt <- predict.r4ml.glm(h_glm_icpt, hf_transform$data)
  r_predict_icpt <- predict.glm(r_glm_icpt, datasets::cars)

  expect_equal(SparkR::as.data.frame(h_predict_icpt$predictions)$means,
               as.data.frame(r_predict_icpt)$r_predict, tol = .01)

})

test_that("r4ml.glm sampling check", {
  # this is a regression test for R4ML-195
  df <- airline[1:500, c("Month", "DayofMonth", "DayOfWeek", "CRSDepTime",
                         "Distance", "ArrDelay")]

  df <- as.r4ml.frame(df)

  df <- r4ml.ml.preprocess(
    df,
    transformPath = tempdir(),
    recodeAttrs = c("DayOfWeek"),
    omit.na = c("Distance", "ArrDelay"),
    dummycodeAttrs = c("DayOfWeek")
    )

  df <- as.r4ml.matrix(df$data)

  samples <- r4ml.sample(df, perc = c(0.7, 0.3))
  train <- samples[[1]]

  glm <- r4ml.glm(ArrDelay ~ ., data = train)
})
