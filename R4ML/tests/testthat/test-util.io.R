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
context("Testing util.io\n")

test_that("hydrar.fs", {
  if(HydraR:::hydrar.fs() == "local") {
    expect_true(HydraR:::hydrar.fs.local())
    expect_false(HydraR:::hydrar.fs.cluster())
  }
  
  if(HydraR:::hydrar.fs() == "cluster") {
    expect_true(HydraR:::hydrar.fs.cluster())
    expect_false(HydraR:::hydrar.fs.local())
  }
  
})

test_that("hydrar.hdfs.exist", {
  if (HydraR:::hydrar.fs.cluster()) { # only need to run this test in cluster mode

    warning("test hydrar.fs.cluster() is not implemented yet")
    #@TODO
  }

})

test_that("hydrar.read.csv", {
  if (HydraR:::hydrar.fs.local()) {
    
    seperators <- c(" ", ",", ":", "|")
  
    for (i in 1:NROW(seperators)) {
      tmp_file <- paste0(tempfile(), ".csv")
      write.table(iris, file = tmp_file, sep = seperators[i], row.names = FALSE)
      df <- hydrar.read.csv(tmp_file, header = TRUE, sep = seperators[i])
      expect_equal(nrow(df), nrow(iris))
      expect_equal(ncol(df), ncol(iris))
      expect_true(all(df == iris))
      }

  }
  
  if (HydraR:::hydrar.fs.cluster()) {
    warning("test hydrar.read.csv() is not implemented in cluster mode yet")
    #@TODO
  }
  
})


test_that("Logging", {
  
  log <- HydraR:::Logging$new();
  
  # default levels
  level <- log$getLevel();
  expect_equal(level, "INFO")
  
  # change the log level
  log$setLevel("FATAL")
  expect_equal("FATAL", log$getLevel())
  
  # java and hydraR level should change
  expect_equal("FATAL", log$getLevel())
  expect_equal("ERROR", log$getLevel(is_java=TRUE))
  
  log$setLevel(level)
})
