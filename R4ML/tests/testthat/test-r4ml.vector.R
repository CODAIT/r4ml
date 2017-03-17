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

context("Testing r4ml.vector\n")

irisDF <- suppressWarnings(createDataFrame(iris))
irisHDF <- as.r4ml.frame(irisDF)

# @TODO after fixes in r4ml.vector re-enable these test cases

# Collect
test_that("Collect", {
  skip("r4ml.vector::collect is not being enabled now")
  x <- irisHDF$Sepal_Length
  expect_equal(all(collect(x) == iris$Sepal.Length), T)
})

# Show
test_that("Show", {
  skip("r4ml.vector::show is not being enabled now")
  x <- irisHDF$Sepal_Length
  baseline <- capture.output(head(iris$Sepal.Length, 20))
  expect_equal(capture.output(show(x))[[1]], baseline[[1]])
})

# Head
test_that("Head", {
  skip("r4ml.vector::head is not being enabled now")
  x <- irisHDF$Sepal_Length
  expect_equal(head(x), head(iris$Sepal.Length))
})

test_that("Arithmetic functions", {
  skip("r4ml.vector::Arithmethic func is not being enabled now")
  x <- irisHDF$Sepal_Length
  # Sin
  expect_equal(all(collect(round(sin(x) * 100)) == round(sin(iris$Sepal.Length) * 100)), T)
  
  # Nested functions and operators
  expect_equal(all(collect(sin(x) / cos(x) - tan(x) < 1.0e-5) == TRUE), TRUE)
  
  # Test sin^2(x) + cos^2(x) == 1
  y <- sin(x)
  z <- y ^ 2 + (cos(x) ^ 2)
  expect_equal(all(collect(round(z * lit('100'))) == 100), T)
  expect_equal(all(collect(sin(x)) < 1), T)
  
  expect_equal(class(rand()) == "Column", T)
})

hf <- as.r4ml.frame(iris)

test_that("corr", {
  skip("r4ml.vector::corr is not being enabled now")
  expect_equal(round(collect(corr(hf$Sepal_Length, hf$Sepal_Width)) * 100000),
               round(cor(iris$Sepal.Length, iris$Sepal.Width) * 100000))
})

test_that("ifelse", {
  skip("r4ml.vector::ifelse is not being enabled now")
  expect_equal(all(collect(ifelse(hf$Species == "Setosa", hf$Sepal_Length, hf$Sepal_Width)) ==
                         ifelse(iris$Species == "Setosa", iris$Sepal.Length, iris$Sepal.Width)), TRUE)
})

test_that("countDistinct", {
  skip("r4ml.vector::countDistinct is not being enabled now")
  expect_equal(collect(countDistinct(hf$Species)), 3)
})

test_that("str", {
  skip("r4ml.vector::countDistinct is not being enabled now")
  hf <- as.r4ml.frame(iris)
  out <- capture.output(str(hf$Species))
  expect_equal(out[1], "'r4ml.vector'")
  expect_equal(out[2], ' $ Species: chr "setosa" "setosa" "setosa" "setosa" "setosa" "setosa"')
})
