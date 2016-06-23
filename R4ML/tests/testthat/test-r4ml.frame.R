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
library (HydraR)
library(testthat)
context("Testing hydrar.frame\n")

# test hydra c'tor
#
test_that("hydrar.frame", {
  warning("test construction of hydrar.frame is not implemented yet")
  # test creation from SparkR dataframe

})

# test is.hydrar.numeric
test_that("is.hydrar.numeric", {
  warning("test is.hydrar.numeric is not implemented yet")
  # test that it return true for numeric hydrar.frame

  # test that it returns false for non numeric hydrar.frame

})

# test as.hydrar.frame for R dataframe
test_that("as.hydrar.frame", {
  warning("test as.hydrar.frame from R data.frame is not implemented yet")
  # test that we can convert from data frame from R data.frame
})


# test as.hydrar.frame for SparkR DataFrame
test_that("as.hydrar.frame", {
  warning("test as.hydrar.frame from SparkR DataFrame is not implemented yet")
  # test that we can convert from SparkR DataFrame
})

test_that("show", {
  irisHDF <- as.hydrar.frame(iris)
  expect_equal(all(capture.output(show(head(iris, 20)))[2:20] == capture.output(show(irisHDF))[2:20]), TRUE)
    # test that we can convert from SparkR DataFrame
})



#begin hydrar.recode testing
test_that("hydrar.recode iris data with one recode", {
  skip("skip for now")
  require(SparkR)
  require(HydraR)
  data("iris")
  hf <- as.hydrar.frame(as.data.frame(iris))
  hf_rec = hydrar.recode(hf, c("Species"))

  # make sure that recoded value is right
  rhf_rec <- SparkR:::as.data.frame(hf_rec$data)
  expect_equal(unique(rhf_rec$Species), c(1,2,3))

  # make sure that meta data is mapped correctly
  md <- hf_rec$metadata
  expect_equal(md$Species$setosa, 1)
  expect_equal(md$Species$versicolor, 2)
  expect_equal(md$Species$virginica, 3)

  # note test various cases
  # 1) more than one recode
  # 2) nothing passed, so all the coloumns are recoded
  # 3) null value passed
  # 4) cases where bad combination is passed
})

test_that("hydrar.recode all columns recoded", {
  require(SparkR)
  require(HydraR)
  idata <- data.frame(c1=c("b", "a", "c", "a"),
                      c2=c("C", "B", "A", "B"),
                      c3=c("a1", "a2", "a3", "a3"))

  idata <- data.frame(c1=c("b", "a", "c", "a"),
                      c2=c("C", "B", "A", "B"),
                      c3=c("a1", "a2", "a3", "a3"))
  exp_rec_data <- data.frame(c1=c(2,1,3,1),
                             c2=c(3,2,1,2),
                             c3=c(1,2,3,3))

  exp_meta_data <- list(
    c1 = list(a = 1, b = 2, c = 3),
    c2 = list(A = 1, B = 2, C = 3),
    c3 = list(a1 = 1, a2 = 2, a3 = 3)
  )

  hf <- as.hydrar.frame(as.data.frame(idata))
  hf_rec = hydrar.recode(hf)

    # make sure that recoded value is right
  rhf_rec <- SparkR:::as.data.frame(hf_rec$data)

  expect_true(all.equal(rhf_rec, exp_rec_data))

  # make sure that meta data is mapped correctly
  md <- hf_rec$metadata
  emd <- exp_meta_data
  for (name in names(emd)) {
    colmd <- emd[[name]]
    for (vname in names(colmd)) {
      #write("DEBUG " %++% name %++% " " %++% vname, stderr())
      #write("DEBUG " %++% md[[name]][[vname]] %++% " " %++% emd[[name]][[vname]], stderr())
      exp <- emd[[name]][[vname]]
      act <- md[[name]][[vname]]
      cat(exp %++% " " %++% act)
      expect_equal(act, exp)
    }
  }
})
#end hydrar.recode testing


