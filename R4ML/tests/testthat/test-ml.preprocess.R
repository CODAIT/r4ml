#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#library (R4ML)

context("Testing r4ml.pre.processing\n")


test_that("r4ml.impute", {
  data("airquality")
  airq_hf <- as.r4ml.frame(as.data.frame(airquality))
  airq_hm <- as.r4ml.matrix(airq_hf)
  airq_hm_n <- r4ml.impute(airq_hm, list("Ozone"="mean"))
})

# begin r4ml.ml.preprocess
test_that("r4ml.ml.preprocess", {
  data("iris")
  iris_hf <- as.r4ml.frame(as.data.frame(iris))
  
  iris_transform <- r4ml.ml.preprocess(
    iris_hf, transformPath = tempdir(),
    dummycodeAttrs = "Species",
    binningAttrs = c("Sepal_Length", "Sepal_Width"),
    numBins=4,
    missingAttrs = c("Petal_Length", "Sepal_Width"),
    imputationMethod = c("global_mean", "constant"),
    imputationValues = list("Sepal_Width" = 40),
    omit.na="Petal_Width",
    recodeAttrs=c("Species"),
    scalingAttrs=c("Petal_Length")
  )
  
  showDF(iris_transform$data, n = 154)
})

test_that("r4ml.ml.preprocess omit.na", {
  iris_hf <- iris
  iris_hf$Petal.Width[5] <- NA
  iris_hf <- as.r4ml.frame(iris_hf)
  
  # with omit.na col specified
  iris_transform <- r4ml.ml.preprocess(
    data = iris_hf,
    transformPath = tempdir(),
    omit.na = c("Petal_Width")
  )
  expect_equal(nrow(iris_transform$data), 149)
  
  # with no cols specified (should default to all columns)
  iris_transform <- r4ml.ml.preprocess(
    data = iris_hf,
    transformPath = tempdir()
  )
  expect_equal(nrow(iris_transform$data), 149)
  
  # with no cols spefied
  iris_transform <- r4ml.ml.preprocess(
    data = iris_hf,
    transformPath = tempdir(),
    omit.na = c()
  )
  expect_equal(nrow(iris_transform$data), 150)
  
})

#Execute ml.preprocess without transformPath parameter
test_that("r4ml.ml.preprocess excludetransformPath", {
  data("iris")
  iris_hf <- as.r4ml.frame(as.data.frame(iris))
  
  iris_transform <- r4ml.ml.preprocess(
    iris_hf, dummycodeAttrs = "Species",
    binningAttrs = c("Sepal_Length", "Sepal_Width"),
    numBins=4,
    missingAttrs = c("Petal_Length", "Sepal_Width"),
    imputationMethod = c("global_mean", "constant"),
    imputationValues = list("Sepal_Width" = 40),
    omit.na="Petal_Width",
    recodeAttrs=c("Species"),
    scalingAttrs=c("Petal_Length")
    )
  
  showDF(iris_transform$data, n = 154) 
})

test_that("r4ml.sysml.transform", {
  data("iris")
  iris_hf <- as.r4ml.frame(as.data.frame(iris))
  
  iris_transform <- r4ml.sysml.transform(
    iris_hf,
    dummycodeAttrs = "Species",
    binningAttrs = c("Sepal_Length", "Sepal_Width"),
    numBins=4,
    missingAttrs = c("Petal_Length", "Sepal_Width"),
    imputationMethod = c("global_mean", "constant"),
    imputationValues = list("Sepal_Width" = 40),
    omit.na="Petal_Width",
    recodeAttrs=c("Species"),
    scalingAttrs=c("Petal_Length")
  )
  
  expect_true(class(iris_transform$data) == "r4ml.matrix")
})

test_that("r4ml.sysml.transform run all na dataframe", {
  result <- tryCatch({
    na_df <- data.frame(c(NA,NA,NA), c(10,20,30))
    names(na_df) <- c("c1", "c2")
    na_hf <- as.r4ml.frame(na_df)
    na_transform <- r4ml.sysml.transform(
      na_hf,
      dummycodeAttrs = "c2",
      omit.na=c("c1"),
      recodeAttrs=c("c2")
    )
    return(FALSE) # this should fail
  }, warning = function(war) {
    print(paste("Got Warning as expected :  ",war))
    ok=grep("After na omission, there are no records left", war)
    if (length(ok) > 0) {
      return(TRUE) # we should get warning first before anything
    }else {
      return(FALSE) # we didn't get the warning
    }
  }, error = function(err) {
    print(paste("Got Error as expected ",err))
    return(TRUE) # if we got err without warn, that's not right
  }, finally = {
    print("done running all na dataframe")
  }) # END tryCatch

  expect_true(result)

})
