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
context("Testing util.io\n")

test_that("r4ml.fs", {
  if(R4ML:::r4ml.fs.mode() == "local") {
    expect_true(R4ML:::is.r4ml.fs.local())
    expect_false(R4ML:::is.r4ml.fs.cluster())
  }
  
  if(R4ML:::r4ml.fs.mode() == "cluster") {
    expect_true(R4ML:::is.r4ml.fs.cluster())
    expect_false(R4ML:::is.r4ml.fs.local())
  }
  
})

test_that("r4ml.read.csv", {
  if (R4ML:::is.r4ml.fs.local()) {
    
    seperators <- c(" ", ",", ":", "|")
  
    for (i in 1:NROW(seperators)) {
      tmp_file <- paste0(tempfile(), ".csv")
      write.table(iris, file = tmp_file, sep = seperators[i], row.names = FALSE)
      df <- r4ml.read.csv(tmp_file, header = TRUE, sep = seperators[i])
      df <- SparkR::as.data.frame(df)
      expect_equal(nrow(df), nrow(iris))
      expect_equal(ncol(df), ncol(iris))
      expect_true(all(df == iris))
      }

  }
  
  if (R4ML:::is.r4ml.fs.cluster()) {
    warning("test r4ml.read.csv() is not implemented in cluster mode yet")
    #@TODO
  }
  
})


test_that("Logging", {
  log <- R4ML:::Logging$new();
  
  # default levels
  level <- log$getLevel();
  expect_equal(level, "INFO")
  
  # change the log level
  log$setLevel("FATAL")
  expect_equal("FATAL", log$getLevel())
  
  # java and r4ml level should change
  expect_equal("FATAL", log$getLevel())
  expect_equal("ERROR", log$getLevel(is_java=TRUE))
  
  # change the log level for testing
  log$setLevel("TRACE")
  
  # make sure that all the log level do work
  
  # test message
  expect_message(log$info("I am message", "root"), "INFO")
  expect_message(log$trace("I am message", "root"), "TRACE")
  expect_message(log$debug("I am message", "root"), "DEBUG")

  # test warning
  expect_warning(log$warn("I am warning", "root"), "WARN")
  
  # test error
  expect_error(log$error("I am error", "root"), "ERROR")
  expect_error(log$fatal("I am fatal", "root"), "FATAL")
  
  # test helper msg
  expect_match(log$message("I am message", "root", ""), "I am message")
  
  # change the log level back to original
  log$setLevel(level)
})

test_that("FileSystem", {
  # test the linux file system
  lfs <- R4ML:::FileSystem$new()
  
  # create the unique file name
  uu_name <- lfs$uu_name();
  cat("testing universal unique file name: ", uu_name)
  
  # create the uniq file, we expect it to fail
  expect_error(do.call(lfs$create, list(uu_name)))
})

test_that("LinuxFS", {
  if (!is.r4ml.fs.local()) {
    skip("LinuxFS runs only in the local mode")
    return
  }
  
  # linux file system
  lfs <- R4ML:::LinuxFS$new()
  
  # create the unique file name
  uu_name <- lfs$uu_name();
  cat("testing universal unique file name: ", uu_name)
  
  # create the uniq file
  f_created <- lfs$create(uu_name)
  expect_true(f_created)
  
  # check if the file exists
  f_exists <- lfs$exists(uu_name)
  expect_true(f_exists)
  
  # remove the file
  f_removed <- lfs$remove(uu_name)
  expect_true(f_removed)
  
  # check again, this time the file shouldn't exists
  expect_false(lfs$exists(uu_name))
  
  # the file/dir must be deleted later automatically on sys.exit
  lfs_td <- lfs$tempdir()
  
  # make sure that user_home is properly returned
  user_home <- lfs$user.home()
})

test_that("HadoopFS", {
  # test the hadoop file system
  if (!is.r4ml.fs.cluster()) {
    skip("HadoopFS runs only in the cluster mode")
    return
  }
  
  # hadoop filesystem
  hfs <- R4ML:::HadoopFS$new()
  
  # create the unique file name
  uu_name <- hfs$uu_name();
  cat("testing universal unique file name: ", uu_name)
  
  # create the uniq file
  f_created <- hfs$create(uu_name)
  expect_true(f_created)
  
  # check if the file exists
  f_exists <- hfs$exists(uu_name)
  expect_true(f_exists)
  
  # remove the file
  f_removed <- hfs$remove(uu_name)
  expect_true(f_removed)
  
  # check again, this time the file shouldn't exists
  expect_false(hfs$exists(uu_name))
  
  # the file/dir must be deleted later automatically on sys.exit
  lfs_td <- hfs$tempdir()
  
  # make sure that user_home is properly returned
  user_home <- hfs$user.home()
})

