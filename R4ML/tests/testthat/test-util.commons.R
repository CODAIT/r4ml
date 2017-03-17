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

library(testthat)
context("Testing util.commons\n")

#@TODO implement test for the other functions in util.commons.R

test_that("r4ml.which.na.cols", {
  
  hf <- iris
  hf$Sepal.Length[5] <- NA
  hf <- as.r4ml.frame(hf)
  
  output <- r4ml.which.na.cols(hf)
  
  expect_equal(output, "Sepal_Length")
  
  hf <- iris
  hf$Sepal.Length[5] <- NA
  hf$Sepal.Width[7] <- NA
  hf <- as.r4ml.frame(hf)
  
  output <- r4ml.which.na.cols(hf)
  
  expect_equal(output, c("Sepal_Length", "Sepal_Width"))

  
})

