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

library(HydraR)

context("Testing sysml_bridge\n")

# test the Bridge to SystemML MatrixCharacteristics
test_that("sysml.MatrixCharacteristics", {
  mc = sysml.MatrixCharacteristics$new(1000, 1000, 10, 10)
  expect_equal(class(mc$env$jref), "jobj")
  expect_equal(mc$nrow, 1000)
  expect_equal(mc$ncol, 1000)
  expect_equal(mc$bnrow, 10)
  expect_equal(mc$bncol, 10)
})

# test the bridge to the SystemML RDDConverterUtilsExt
test_that("sysml.RDDConverterUtils", {
  require(SparkR)
  require(HydraR)
  aq_ozone <- airquality$Ozone
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_df <- as.hydrar.matrix(as.hydrar.frame(as.data.frame(aq_ozone)))
  mc <- HydraR:::sysml.MatrixCharacteristics$new(count(aq_ozone_df), 1, 10, 1)
  rdd_utils <- HydraR:::sysml.RDDConverterUtils$new()
  sysml_jrdd <- rdd_utils$dataFrameToBinaryBlock(aq_ozone_df, mc)
})

# test the Bridge to the SystemML MLContext
test_that("sysml.MLContext sample data",{
  cat("testing sysml.MLContext...")
  v = c(1,2,3,4,5)
  as.data.frame(v)
  tdf=as.data.frame(v)
  dv=as.data.frame(v)
  dv_df <- as.hydrar.matrix(as.hydrar.frame(dv))
  
  mc <- HydraR:::sysml.MatrixCharacteristics$new(count(dv_df), 1, 10,1)
  rdd_utils<- HydraR:::sysml.RDDConverterUtils$new()
  mlc = HydraR:::sysml.MLContext$new(sysmlSparkContext)
  mlc$reset()
  sysml_jrdd=rdd_utils$dataFrameToBinaryBlock(dv_df, mc)
  mlc = HydraR:::sysml.MLContext$new(sysmlSparkContext)
  dml = '
  fileX = ""
  fileO = 1
  fileF = 2
  #factor = 0
  X = read($fileX)
  print($factor)
  F = $factor
  O = X*F
  write(O, fileO)
  #write(F, fileF)
  '
  mlc$reset()
  mlc$registerInput("X", sysml_jrdd, mc)
  mlc$registerOutput("O")
  outputs <- mlc$executeScript(dml, c("factor"), c("2"))
  o1 = outputs$getDF("O")
  cat("output dataframe")
  SparkR:::showDF(o1)
  o2=SparkR:::collect(o1)
  expect_equivalent(2*v, o2$O)
})

# test the Bridge to the SystemML MLContext . To be removed eventually
test_that("sysml.MLContext Short data", {
  cat("testing sysml.MLContext...")
  mlc = HydraR:::sysml.MLContext$new(sysmlSparkContext)
  dml = '
    fileX = ""
    fileO = ""
    X = read($fileX)
    O = X*2
    write(O, fileO)
  '
  aq_ozone <- airquality$Ozone
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_df <- as.hydrar.matrix(as.hydrar.frame(as.data.frame(aq_ozone)))
  
  x_cnt = SparkR:::count(aq_ozone_df)
  mc <- HydraR:::sysml.MatrixCharacteristics$new(x_cnt, 1, 10, 1)
  rdd_utils <- HydraR:::sysml.RDDConverterUtils$new()
  sysml_jrdd <- rdd_utils$dataFrameToBinaryBlock(aq_ozone_df, mc)
  mlc$reset()
  mlc$registerInput("X", sysml_jrdd, mc)
  mlc$registerOutput("O")
  outputs <- mlc$executeScript(dml)
  o1 = outputs$getDF("O")
  cat("output dataframe")
  SparkR:::showDF(o1)
})

# test the Bridge to the SystemML MLContext. To be removed eventually
test_that("sysml.MLContext Long", {
  if (exists("TESTTHAT_LONGTEST", hydrar.env) &&
      hydrar.env$TESTTHAT_LONGTEST == TRUE) {
    mlc = HydraR:::sysml.MLContext$new(sysmlSparkContext)
    dml = '
    fileX = ""
    fileO = ""
    X = read($fileX)
    O = X*2
    write(O, fileO)
    '
    airr <- read_airline_data()
    airrt <- airr$Distance
    airrt[is.na(airrt)] <- 0
    airrtd <- as.data.frame(airrt)
    air_dist <- createDataFrame(sysmlSqlContext, airrtd)

    X_cnt <- SparkR:::count(air_dist)
    #X_rdd <- SparkR:::toRDD(air_dist)
    X_mc <- HydraR:::sysml.MatrixCharacteristics$new(X_cnt, 1, 10, 1)
    rdd_utils <- HydraR:::sysml.RDDConverterUtils$new()
    bb_df <- rdd_utils$dataFrameToBinaryBlock(air_dist, X_mc)
    mlc$reset()
    mlc$registerInput("X", bb_df, X_mc)
    mlc$registerOutput("O")
    outputs <- mlc$executeScript(dml)
    o1 = outputs$getDF("O")
  }
})


# test the Bridge to the SystemML MLOutput
test_that("sysml.MLOutput",{
  cat("testing sysml.MLOutput...")
  warning("testing for sysml.MLOutput is not implemented yet")
  #@TODO
})

# test the helper function for executing systemML dml via the hydrar.matrix
test_that("sysml.execute", {
  cat("testing sysml.execute")
  warning("testing for sysml.execute is not implemented yet")
  #@TODO
})
