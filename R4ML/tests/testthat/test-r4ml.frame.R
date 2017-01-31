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

library(testthat)

if (!hydrar.env$HYDRAR_SESSION_EXISTS) {
  hydrar.session()
}

context("Testing hydrar.frame\n")

# test hydra c'tor
#
test_that("hydrar.frame", {

  sdf <- SparkR::as.DataFrame(beaver1)
  hdf <- as.hydrar.frame(sdf)

})

# test is.hydrar.numeric
test_that("is.hydrar.numeric", {

  non_numeric_hdf <- as.hydrar.frame(iris)
  numeric_hdf <- as.hydrar.frame(beaver1)

  expect_false(is.hydrar.numeric(non_numeric_hdf))
  expect_true(is.hydrar.numeric(numeric_hdf))
  
})

# test as.hydrar.frame for R dataframe
test_that("as.hydrar.frame", {
  
  hf <- as.hydrar.frame(iris)
  
})

test_that("show", {
  hydrar.session()
  irisHDF <- as.hydrar.frame(iris)
  expect_true(length(capture.output(show(irisHDF))) > 5)
})


#begin hydrar.recode testing
test_that("hydrar.recode iris data with one recode", {
  #skip("skip for now")
  hydrar.session()
  data("iris")
  hf <- as.hydrar.frame(as.data.frame(iris))
  hf_rec = hydrar.recode(hf, c("Species"))

  # make sure that recoded value is right
  rhf_rec <- SparkR::as.data.frame(hf_rec$data)
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
  rhf_rec <- SparkR::as.data.frame(hf_rec$data)

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

#begin hydrar.normalize aka hydrar.scale (scale and shift)
test_that("hydrar.normalize all columns recoded", {
  #skip("skip for now")
  idata <- data.frame(c1=c(10, 10, 10, 10, 10),
                      c2=c(1, 2, 3, 4, 5),
                      c3=c(100, 200, 300, 400, 500))
  
  exp_rec_data <- data.frame(c1=c(0,0,0,0,0),
                             c2=c(-0.6324555320336758,-0.3162277660168379,0,0.3162277660168379,0.6324555320336758),
                             c3=c(100, 200,300,400,500))
  
  exp_metadata <- list(
    c1 = list("mean" = 10, "stddev" = 1),
    c2 = list("mean" = 3, "stddev" = 1.581139)
  )
  
  hf <- as.hydrar.frame(as.data.frame(idata))
  #hf_rec = hydrar.normalize(hf, "c1", "c2")
  col2norm <- list("c1", "c2")
   hf_rec = hydrar.normalize(hf, col2norm)
  
  # make sure that normalize value is right
  rhf_rec <- SparkR::as.data.frame(hf_rec$data)
  
  expect_true(all.equal(rhf_rec, exp_rec_data))
  
  # make sure that meta data is mapped correctly
  md <- hf_rec$metadata
  # check that one normalize metadata is right
  norm_md <- as.list(md)
  expect_equal(capture.output(norm_md), capture.output(exp_metadata))
  
})
#end hydrar.normalize aka hydrar.scale (scale and shift)

test_that("hydrar.binning", {
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df, repartition = FALSE)
  num_bins = 4
  col_names = list("Sepal_Width", "Petal_Length")
  binned_df = hydrar.binning(iris_df, col_names, num_bins)
  results = SparkR::collect(binned_df$data)
  expect_equal(results[[2]][1], 3.5, tolerance=1e-2)
  expect_equal(results[[3]][2], 1.7375, tolerance=1e-2)
  expect_equal(results[[2]][3], 2.9, tolerance=1e-2)
  expect_equal(as.numeric(binned_df$metadata[["Sepal_Width"]]["minValue"]), 2, tolerance=1e-2)
  expect_equal(as.numeric(binned_df$metadata[["Sepal_Width"]]["maxValue"]), 4.4, tolerance=1e-2)
  expect_equal(as.numeric(binned_df$metadata[["Sepal_Width"]]["binWidth"]), 0.6, tolerance=1e-2)
  expect_equal(as.numeric(binned_df$metadata[["Sepal_Width"]]["numBins"]), 4, tolerance=1e-2)
  expect_equal(as.numeric(binned_df$metadata[["Petal_Length"]]["minValue"]), 1, tolerance=1e-2)
  expect_equal(as.numeric(binned_df$metadata[["Petal_Length"]]["maxValue"]), 6.9, tolerance=1e-2)
  expect_equal(as.numeric(binned_df$metadata[["Petal_Length"]]["binWidth"]), 1.475, tolerance=1e-2)
  expect_equal(as.numeric(binned_df$metadata[["Petal_Length"]]["numBins"]), 4, tolerance=1e-2)
})

test_that("hydrar.impute all columns imputed", {
  df <- as.hydrar.frame(airquality)
  new_df <- hydrar.impute(df, list("Ozone"=4000, "Solar_R"="mean"))
  result = SparkR::collect(new_df$data)
  expect_equal(result[[1]][5], 4000, tolerance=1e-2)
  expect_equal(result[[2]][5], 185, tolerance=1e-2)
  expect_equal(new_df$metadata[["Ozone"]], 4000, tolerance=1e-2)
  expect_equal(new_df$metadata[["Solar_R"]], 185, tolerance=1e-2)
})
