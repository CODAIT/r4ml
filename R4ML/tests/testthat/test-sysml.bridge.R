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


context("Testing sysml_bridge\n")

# test the Bridge to SystemML MatrixCharacteristics
test_that("sysml.MatrixCharacteristics", {
  mc <- sysml.MatrixCharacteristics$new()
  expect_equal(class(mc$env$jref), "jobj")
})

# test the bridge to the SystemML RDDConverterUtilsExt
test_that("sysml.RDDConverterUtils", {
  aq_ozone <- airquality$Ozone
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_df <- as.r4ml.matrix(as.data.frame(aq_ozone))
  mc <- R4ML:::sysml.MatrixCharacteristics$new()
  rdd_utils <- R4ML:::sysml.RDDConverterUtils$new()
  sysml_jrdd <- rdd_utils$dataFrameToBinaryBlock(aq_ozone_df, mc)
})

# test the Bridge to the SystemML MLContext
test_that("sysml.MLContext sample data",{
  cat("testing sysml.MLContext...")
  v = c(1,2,3,4,5)
  as.data.frame(v)
  tdf=as.data.frame(v)
  dv=as.data.frame(v)
  dv_df <- as.r4ml.matrix(as.r4ml.frame(dv, repartition = FALSE))
  
  mc <- R4ML:::sysml.MatrixCharacteristics$new()
  rdd_utils <- R4ML:::sysml.RDDConverterUtils$new()
  mlc <- R4ML:::sysml.MLContext$new(r4ml.env$sysmlSparkContext)
  mlc$reset()
  sysml_jrdd=rdd_utils$dataFrameToBinaryBlock(dv_df, mc)
  mlc = R4ML:::sysml.MLContext$new(r4ml.env$sysmlSparkContext)
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
  # in some machine this is needed but I think it is fixed and 
  # keep it here so that we get failure if any
  #expect_equivalent(sort(2*v), sort(o2$O))
  expect_equivalent(2*v, o2$O)
})

# test the Bridge to the SystemML MLContext . To be removed eventually
test_that("sysml.MLContext Short data", {
  cat("testing sysml.MLContext...")
  mlc = R4ML:::sysml.MLContext$new(r4ml.env$sysmlSparkContext)
  dml = '
    fileX = ""
    fileO = ""
    X = read($fileX)
    O = X*2
    write(O, fileO)
  '
  aq_ozone <- airquality$Ozone
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_df <- as.r4ml.matrix(as.data.frame(aq_ozone))
  
  mc <- R4ML:::sysml.MatrixCharacteristics$new()
  rdd_utils <- R4ML:::sysml.RDDConverterUtils$new()
  sysml_jrdd <- rdd_utils$dataFrameToBinaryBlock(aq_ozone_df, mc)
  mlc$reset()
  mlc$registerInput("X", sysml_jrdd, mc)
  mlc$registerOutput("O")
  outputs <- mlc$executeScript(dml)
  o1 = outputs$getDF("O")
  cat("output dataframe")
  SparkR:::showDF(o1)
})

# test the Bridge to the SystemML MLContext . To be removed eventually
test_that("sysml.MLContext Exception handling test", {
  cat("testing sysml.MLContext...")
  mlc = R4ML:::sysml.MLContext$new(r4ml.env$sysmlSparkContext)
  dml = '
    fileX = ""
    fileO = ""
    X = read($fileX)
    O = X*2 + $foo ~ M # it must fail
    write(O, fileO)
  '
  aq_ozone <- airquality$Ozone
  aq_ozone[is.na(aq_ozone)] <- 0
  aq_ozone_df <- as.r4ml.matrix(as.r4ml.frame(as.data.frame(aq_ozone),
                                                  repartition = FALSE))

  mc <- R4ML:::sysml.MatrixCharacteristics$new()
  rdd_utils <- R4ML:::sysml.RDDConverterUtils$new()
  sysml_jrdd <- rdd_utils$dataFrameToBinaryBlock(aq_ozone_df, mc)
  mlc$reset()
  mlc$registerInput("X", sysml_jrdd, mc)
  mlc$registerOutput("O")
  options(warning.length = 5000) # set warning length to some number
  expect_error(do.call(mlc$executeScript,list(dml)))
  expect_equal(options()$warning.length, 5000) # test that the number remains the same

})

# test the Bridge to the SystemML MLContext. To be removed eventually
test_that("sysml.MLContext Long", {
  if (r4ml.env$TESTTHAT_LONGTEST() == TRUE) {
    mlc = R4ML:::sysml.MLContext$new(r4ml.env$sysmlSparkContext)
    dml = '
    fileX = ""
    fileO = ""
    X = read($fileX)
    O = X*2
    write(O, fileO)
    '
    airr <- R4ML::airline
    airrt <- airr$Distance
    airrt[is.na(airrt)] <- 0
    airrtd <- as.data.frame(airrt)
    air_dist <- createDataFrame(airrtd)

    #X_rdd <- SparkR:::toRDD(air_dist)
    X_mc <- R4ML:::sysml.MatrixCharacteristics$new()
    rdd_utils <- R4ML:::sysml.RDDConverterUtils$new()
    air_dist <- as.r4ml.matrix(air_dist)
    bb_df <- rdd_utils$dataFrameToBinaryBlock(air_dist, X_mc)
    mlc$reset()
    mlc$registerInput("X", bb_df, X_mc)
    mlc$registerOutput("O")
    outputs <- mlc$executeScript(dml)
    o1 <- outputs$getDF("O")
  }
})


# test the Bridge to the SystemML MLOutput
test_that("sysml.MLOutput",{
  cat("testing sysml.MLOutput...")
  warning("testing for sysml.MLOutput is not implemented yet")
  #@TODO
})

# test the helper function for executing systemML dml via the r4ml.matrix
test_that("sysml.execute", {
  cat("testing sysml.execute")
  warning("testing for sysml.execute is not implemented yet")
  #@TODO
})
