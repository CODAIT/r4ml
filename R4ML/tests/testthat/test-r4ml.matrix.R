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

context("Testing hydrar.matrix\n")

test_that("is.hydrar.matrix", {
  warning("test is.hydrar.matrix is not implemented yet")
  #@TODO
})

test_that("as.hydrar.matrix", {
  warning("test as.hydrar.matrix is not implemented yet")
  #@TODO
})

# begin one hot testing
test_that("hydrar.onehot generic case 1", {
  rdf2hm <- function(rdf) {
    as.hydrar.matrix(as.DataFrame(sysmlSqlContext, rdf))
  }
  # out data set contains mix columns
  data <- rdf2hm(data.frame( c1=c(2,3,4), c2=c(1,4,3), c3=c(5,5,5), c4=c(3,1,2) ))
  # expected output
  exp_oh <- data.frame(c1=c(2,3,4),
                             c2_1=c(1,0,0),
                             c2_2=c(0,0,0),
                             c2_3=c(0,0,1),
                             c2_4=c(0,1,0),
                             c3=c(5,5,5),
                             c4_1=c(0,1,0),
                             c4_2=c(0,0,1),
                             c4_3=c(1,0,0)
                            )
  # expected metadata
  exp_metadata <- list(
    c4 = c("c4_1", "c4_2", "c4_3"),
    c2 = c("c2_1", "c2_2", "c2_3", "c2_4")
  )
  onehot_cols <- c("c2", "c4")
  hf_oh_db <- hydrar.onehot(data, onehot_cols)
  hf_oh <- hf_oh_db$data
  hf_oh_md <- hf_oh_db$metadata
  showDF(hf_oh)
  
  rhf_oh <- SparkR:::as.data.frame(hf_oh)
  # check the one hot encoding is true
  expect_true(all.equal(rhf_oh, exp_oh))

  # check that one hot encoding metadata is right
  oh_md <- as.list(hf_oh_md)
  expect_equal(capture.output(oh_md), capture.output(exp_metadata))
  
})

# end one hot encoding

test_that("hydrar.impute", {
  df <- as.hydrar.matrix(airquality)
  ml.coltypes(df) <- c("scale", "scale", "scale", "scale", "nominal", "nominal")
  new_df <- hydrar.impute(df, list("Ozone"=4000, "Solar_R"="mean"))
})
