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
context("Testing util.io\n")

test_that("hydrar.fs", {
  require(SparkR)
  require(HydraR)
  
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
  
  if(HydraR:::hydrar.fs.cluster()) { # only need to run this test in cluster mode
    require(SparkR)
    require(HydraR)
  
    warning("test hydrar.fs.cluster() is not implemented yet")
  }

})

test_that("hydrar.read.csv", {
  require(SparkR)
  require(HydraR)
  
  if(HydraR:::hydrar.fs.local()) {
    warning("test hydrar.read.csv() is not implemented yet")
    #@TODO
  }
  
  if(HydraR:::hydrar.fs.cluster()) {
    warning("test hydrar.read.csv() is not implemented in cluster mode yet")
    #@TODO
  }
  
})
