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
context("Testing zzz\n")

# TODO add remaining zzz test

test_that("r4ml.session.param.driver.memory", {
  # test if parameters passed to r4ml.session are propagated to sparkR session.
  r4ml.session.stop()

  r4ml.session(sparkConfig = list("spark.driver.memory" = "1G"))
  config <- sparkR.conf()
  expect_match(config$spark.driver.memory, "1G")

  r4ml.session.stop()
  r4ml.session()
})

test_that("r4ml.session.param.enableHiveSupport.false", {
  # test if parameters passed to r4ml.session are propagated to sparkR session.
  r4ml.session.stop()
  
  r4ml.session(enableHiveSupport = FALSE)
  config <- sparkR.conf()
  expect_null(config$spark.sql.catalogImplementation)
  
  r4ml.session.stop()
  r4ml.session()
})

test_that("r4ml.session.param.enableHiveSupport.true", {
  # test if parameters passed to r4ml.session are propagated to sparkR session.
  r4ml.session.stop()

  r4ml.session(enableHiveSupport = TRUE)
  config <- sparkR.conf()
  # this may not work if it is build without hive
  if (is.null(config$spark.sql.catalogImplementation)) {
    warning("spark might not have been build with 'hive'")
  } else {
    expect_match(config$spark.sql.catalogImplementation, "hive")
  }

  r4ml.session.stop()
  r4ml.session()
})

