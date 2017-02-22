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

context("Testing hydrar.glm\n")

# test hydra GLM model
test_that("hydrar.glm", {
  data("airquality")
  aq_ozone <- airquality[c("Ozone", "Temp", "Wind")]
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_rdf <- as.data.frame(aq_ozone)
  aq_ozone_df <- as.hydrar.frame(aq_ozone_rdf, repartition = FALSE)
  aq_ozone_mat <- as.hydrar.matrix(aq_ozone_df)
  train <- aq_ozone_mat
  aq_glm <- hydrar.glm(Ozone ~ . , data = train)
  coef(aq_glm)
  stats(aq_glm)
  
  aq_glm <- hydrar.glm(Ozone ~ . , data = train, intercept = T)
  coef(aq_glm)
  stats(aq_glm)
  
  aq_glm <- hydrar.glm(Ozone ~ ., data=train, family=poisson(log))
  coef(aq_glm)
  stats(aq_glm)
  
  aq_glm <- hydrar.glm(Ozone ~ ., data=train, family=poisson(log), shiftAndRescale = T, intercept = T)
  coef(aq_glm)
  stats(aq_glm)
})

test_that("predict.hydrar.glm", {
  data("airquality")
  aq_ozone <- airquality[c("Ozone", "Temp", "Wind")]
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_rdf <- as.data.frame(aq_ozone)
  aq_ozone_df <- as.hydrar.frame(aq_ozone_rdf, repartition = FALSE)
  aq_ozone_mat <- as.hydrar.matrix(aq_ozone_df)
  s <- hydrar.sample(aq_ozone_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  test1 <- test[,c(2,3)]
  test2 <- as.hydrar.matrix(test1)
  
  aq_glm <- hydrar.glm(Ozone ~ . , data = train, lambda=1.1)
  predict(aq_glm, test)
  
  a <- predict(aq_glm, test2)
  collect(a$predictions)

  aq_glm <- hydrar.glm(Ozone ~ . , data = train, intercept = T)
  predict(aq_glm, test)
  
  a <- predict(aq_glm, test2)
  collect(a$predictions)

  aq_glm <- hydrar.glm(Ozone ~ ., data=train, family=poisson(log))
  predict(aq_glm, test)
  
  a <- predict(aq_glm, test2)
  collect(a$predictions)
  
  aq_glm <- hydrar.glm(Ozone ~ ., data=train, family=poisson(log), shiftAndRescale = T, intercept = T)
  predict(aq_glm, test)
  
  a <- predict(aq_glm, test2)
  collect(a$predictions)
})

test_that("hydrar.glm accuracy", {
  hf <- as.hydrar.frame(datasets::cars)
  hf_transform <- hydrar.ml.preprocess(hf, transformPath = tempdir())
  hf_transform$data <- as.hydrar.matrix(hf_transform$data)

  h_glm <- hydrar.glm(formula = dist ~ ., data = hf_transform$data,
                      intercept = FALSE)
  r_glm <- glm(formula = dist ~ . - 1, data = datasets::cars)

  expect_equal(coef(h_glm)[[1]], coef(r_glm)[[1]], tol = .01)

  h_glm_icpt <- hydrar.glm(formula = dist ~ ., data = hf_transform$data,
                           intercept = TRUE)
  r_glm_icpt <- glm(formula = dist ~ ., data = datasets::cars)

  expect_equal(h_glm_icpt@coefficients$`(Intercept)`,
               r_glm_icpt$coefficients[["(Intercept)"]], tol = .01)

  expect_equal(h_glm_icpt@coefficients$speed,
               r_glm_icpt$coefficients[["speed"]], tol = .01)

  h_predict <- predict.hydrar.glm(h_glm, hf_transform$data)
  r_predict <- predict.glm(r_glm, datasets::cars)

  expect_equal(SparkR::as.data.frame(h_predict$predictions)$means,
               as.data.frame(r_predict)$r_predict, tol = .01)

  h_predict_icpt <- predict.hydrar.glm(h_glm_icpt, hf_transform$data)
  r_predict_icpt <- predict.glm(r_glm_icpt, datasets::cars)

  expect_equal(SparkR::as.data.frame(h_predict_icpt$predictions)$means,
               as.data.frame(r_predict_icpt)$r_predict, tol = .01)

})
