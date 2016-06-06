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

context("Testing hydrar.mlogit\n")

# test hydra logistic regression

test_that("hydrar.mlogit", {
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==4, ncol(coef(iris_log_reg))==2)
})
  
test_that("hydrar.mlogit_labelnames", {   
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  labels = c("setosa", "versicolor", "virginica")
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, labelNames=labels)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==4, ncol(coef(iris_log_reg))==2)
})
  
test_that("hydrar.mlogit_intercept", { 
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, intercept=T)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==5, ncol(coef(iris_log_reg))==2)
  # As the coefficients are calculated in an n-dimensional bowl, we 
  # check a few of the extreme points to ensure we're at the minimum
  expect_equal(coef(iris_log_reg)[[1]][1], 12.999295, tolerance=1e-2) 
  expect_equal(coef(iris_log_reg)[[2]][3], -9.429609, tolerance=1e-2) 
  expect_equal(coef(iris_log_reg)[[1]][2], 15.9683, tolerance=1e-2) 
})
  
test_that("hydrar.mlogit_shiftAndRescale", {  
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, shiftAndRescale=T, intercept=T)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==5, ncol(coef(iris_log_reg))==2)
})
  
test_that("hydrar.mlogit_inner.iter", {   
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, inner.iter.max=2)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==4, ncol(coef(iris_log_reg))==2)
})
  
test_that("hydrar.mlogit_outer.iter", {
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, outer.iter.max=2)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==4, ncol(coef(iris_log_reg))==2)
})
  
test_that("hydrar.mlogit_inner+outer", {   
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, inner.iter.max=2, outer.iter.max=2)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==4, ncol(coef(iris_log_reg))==2)
})
  
test_that("hydrar.mlogit_lambda2.5", {   
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, lambda=2.5)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==4, ncol(coef(iris_log_reg))==2)
})
  
test_that("hydrar.mlogit_lambda0", {   
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, lambda=0)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==4, ncol(coef(iris_log_reg))==2)
})

test_that("hydrar.mlogit_lambda500", {   
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, lambda=500)
  show(iris_log_reg)
  coef(iris_log_reg)
  expect_lt(abs(coef(iris_log_reg)[[1]][1]), 1)
  stopifnot(nrow(coef(iris_log_reg))==4, ncol(coef(iris_log_reg))==2)
})

test_that("hydrar.mlogit_tolerance", { 
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat, tolerance=10)
  show(iris_log_reg)
  coef(iris_log_reg)
  stopifnot(nrow(coef(iris_log_reg))==4, ncol(coef(iris_log_reg))==2)
})

test_that("scoring", {   
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  s <- hydrar.sample(iris_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  ml.coltypes(train) <- c("scale", "scale", "scale", "scale", "nominal") 
  ml.coltypes(test) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = train, labelNames=c("Setosa","Versicolor","Virginica")) 
  output <- predict.hydrar.mlogit(iris_log_reg, test)
})
  
test_that("testing/prediction", {   
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  s <- hydrar.sample(iris_mat, perc=c(0.2,0.8))
  test <- s[[1]]
  train <- s[[2]]
  ml.coltypes(train) <- c("scale", "scale", "scale", "scale", "nominal") 
  ml.coltypes(test) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = train, labelNames=c("Setosa","Versicolor","Virginica")) 
  test = as.hydrar.matrix(as.hydrar.frame(test[,c(1:4)]))
  output <- predict.hydrar.mlogit(iris_log_reg, test)
})
