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


context("Testing hydrar.sampling\n")

# test for getting the subset of the data 
test_that("hydrar.sampling for subset\n", {
  cat("testing sampling subset\n")
  iris_hf <- as.hydrar.frame(iris)
  
  # get 10% sample
  iris_sample1 <- hydrar.sample(iris_hf, 0.1)
  sample_ratio <- nrow(iris_sample1[[1]]) / nrow(iris_hf)
  cat("sample ratio => {expected=0.1, got=" %++% + sample_ratio %++% "}\n")
  cat("\n")
  expect_equal(sample_ratio, 0.1, tolerance = 0.2)
})

# test for getting number of samples
test_that("hydrar.sampling for number of samples\n", {
  cat("testing sampling count\n")
  iris_hf <- as.hydrar.frame(iris)
  
  # get 50 samples 
  iris_sample <- hydrar.sample(iris_hf, 50/nrow(iris_hf))
  cat("sample size => {expected=50, got=" %++% nrow(iris_sample[[1]]) %++% "}\n")
  cat("\n")
  expect_equal(nrow(iris_sample[[1]]), 50, tolerance = 30)
})

# test that we can randomly partition the data into two split
test_that("hydrar.sampling 2 split\n", {
  cat("testing sampling 2 split\n")
  iris_hf <- as.hydrar.frame(iris)
  # Randomly split the data into training (70%) and test (30%) sets
  iris_sample_arr <- hydrar.sample(iris_hf, c(0.7, 0.3))
  train_ratio <- nrow(iris_sample_arr[[1]]) / nrow(iris_hf)
  test_ratio <- nrow(iris_sample_arr[[2]]) / nrow(iris_hf)
  cat("train_ratio => {expected=0.7, got=" %++% train_ratio %++% "}\n")
  expect_equal(train_ratio, 0.7, tolerance = 0.2)
  cat("test_ratio => {expected=0.3, got=" %++% test_ratio %++% "}\n")
  expect_equal(test_ratio, 0.3, tolerance = 0.2)
  cat("\n")
})

# test that we can randomly partition the data into k splits
test_that("hydrar.sampling k split\n", {
  cat("testing sampling k split\n")
  
  iris_hf <- as.hydrar.frame(iris)
  
  # Randomly split the data into training (70%) and test (30%) sets
  weights <- c(0.1, 0.3, 0.2, 0.4)
  iris_sample_arr <- hydrar.sample(iris_hf, weights)
  lapply(Map(c, weights, iris_sample_arr), function(args) {
    w <- args[[1]]
    folded_iris_hf <- args[[2]]
    w_ratio <- nrow(folded_iris_hf) / nrow(iris_hf)
    cat("weight_ratio => {expected=" %++% w %++% "," %++% "got=" %++% w_ratio %++% "}\n")
    expect_equal(w_ratio, w, tolerance = 0.3)
  })
  cat("\n")
})
