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

test_that("%++%", {
  expect_equal("foo" %++% "bar", "foobar")
})

test_that("r4ml.emptySymbol", {
  r4ml.emptySymbol()
})

test_that(".r4ml.isNullOrEmpty", {
  expect_true(.r4ml.isNullOrEmpty(""))
  expect_false(.r4ml.isNullOrEmpty(iris))
})

test_that(".r4ml.hasNullOrEmpty", {
  expect_true(.r4ml.hasNullOrEmpty(c(1, 2, 3, NA, 5)))
  expect_false(.r4ml.hasNullOrEmpty(c(1, 2, 3, 4, 5)))
})

test_that("r4ml.checkParameter", {
  expect_error(.r4ml.checkParameter("logsrc", parm = 1,
                                    expectedClasses = "character"))
  expect_true(.r4ml.checkParameter("logsrc", parm = 1,
                                    expectedClasses = "numeric"))

  expect_error(.r4ml.checkParameter("logsrc", parm = NA, isNAOK = FALSE))
  expect_true(.r4ml.checkParameter("logsrc", parm = NA, isNAOK = TRUE))

  expect_error(.r4ml.checkParameter("logsrc", parm = NULL, isNullOK = FALSE))
  expect_true(.r4ml.checkParameter("logsrc", parm = NULL, isNullOK = TRUE))

  expect_error(.r4ml.checkParameter("logsrc", parm = 1,
                                    expectedValues = c(2, 3)))
  expect_true(.r4ml.checkParameter("logsrc", parm = 1,
                                   expectedValues = c(1, 2)))
})

test_that(".r4ml.contains", {
  expect_true(.r4ml.contains("abcdefg", "efg"))
  expect_false(.r4ml.contains("abcdefg", "efgh"))
})

test_that(".r4ml.is.integer", {
  expect_true(.r4ml.is.integer(1))
  expect_false(.r4ml.is.integer(1.1))

})

test_that(".r4ml.separateXAndY", {
  df <- datasets::beaver1
  r4f <- as.r4ml.frame(df)
  output <- .r4ml.separateXAndY(r4f, "temp")

  expect_equal(dim(output$X), c(114, 3))
  expect_equal(dim(output$Y), c(114, 1))

})

test_that(".r4ml.binary.splitXY", {
  df <- datasets::beaver1
  r4f <- as.r4ml.frame(df)
  output <- .r4ml.binary.splitXY(r4f, "temp")

  expect_equal(dim(output$X), c(114, 3))
  expect_equal(dim(output$Y), c(114, 1))
})

test_that("r4ml.ml.checkModelFeaturesMatchData", {
  r4f <- as.r4ml.frame(datasets::cars)
  r4f_transform <- r4ml.ml.preprocess(r4f, transformPath = tempdir())

  r4f_transform$data <- as.r4ml.matrix(r4f_transform$data)

  object <- r4ml.lm(dist ~ ., r4f_transform$data)

  expect_true(r4ml.ml.checkModelFeaturesMatchData(coef(object),
                                                  r4f_transform$data,
                                                  object@intercept,
                                                  object@labelColumnName,
                                                  object@yColId))

  expect_error(r4ml.ml.checkModelFeaturesMatchData(coef(object),
                                                  r4f_transform$data[, -1],
                                                  object@intercept,
                                                  object@labelColumnName,
                                                  object@yColId))
})

test_that(".r4ml.tree.traversal", {
  # this tested when we test r4ml.extractColsFromFormulaTree
})

test_that(".r4ml.validColname", {
  expect_true(.r4ml.validColname("valid_name"))
  expect_false(.r4ml.validColname("invalid,name"))
  expect_false(.r4ml.validColname("invalid;name"))
  expect_false(.r4ml.validColname("invalid+name"))
  expect_false(.r4ml.validColname("invalid$name"))
})

test_that("r4ml.extractColsFromFormulaTree", {
  df <- survival::heart
  df$transplant <- as.numeric(df$transplant)
  r4f <- as.r4ml.matrix(df)

  output <- r4ml.extractColsFromFormulaTree(c("age", "year", "stop"), r4f)

  expect_equal(length(output), 3) # 3 baseline vars
  expect_equal(output[[1]], 4) # age is the 4th column
  expect_equal(output[[2]], 5) # year is the 5th column
  expect_equal(output[[3]], 2) # stop is the 2nd column

  expect_error(r4ml.extractColsFromFormulaTree(c("age", "invalid_column"), r4f))
})

test_that("r4ml.parseSurvivalArgsFromFormulaTree", {
  df <- survival::lung
  r4f <- as.r4ml.matrix(df)

  form <- Surv(time, status) ~ age + ph_karno + pat_karno + wt_loss

  surv_list <- r4ml.parseSurvivalArgsFromFormulaTree(form, r4f, tempdir())

  expect_equal(surv_list[[1]][1], 2) # time = col 2
  expect_equal(surv_list[[1]][2], 3) # status = col 3
  expect_equal(surv_list[[2]][1], 4) # age = col 4
  expect_equal(surv_list[[2]][2], 7) # ph_karno = col 7
  expect_equal(surv_list[[2]][3], 8) # pat_karno = col 8
  expect_equal(surv_list[[2]][4], 10) # wt_loss = col 10

  form_invalid <- Surv(time, status) ~ age + ph_karno + invalid_col

  expect_error(r4ml.parseSurvivalArgsFromFormulaTree(form_invalid, r4f,
                                                     tempdir()))
})

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
