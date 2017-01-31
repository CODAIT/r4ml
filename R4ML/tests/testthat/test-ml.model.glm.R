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
# currently only the test for running is done but
#@TODO create accuracy test
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
