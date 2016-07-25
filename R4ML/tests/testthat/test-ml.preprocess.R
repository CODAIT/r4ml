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
#library (HydraR)

context("Testing hydrar.pre.processing\n")


test_that("hydrar.impute", {
  # skip("skip for now")
  require(SparkR)
  require(HydraR)
  data("airquality")
  airq_hf <- as.hydrar.frame(as.data.frame(airquality))
  airq_hm <- as.hydrar.matrix(airq_hf)
  airq_hm_n <- hydrar.impute(airq_hm, list("Ozone"="mean"))
})

# begin hydrar.ml.preprocess
test_that("hydrar.ml.preprocess",  {
  #skip("skip for now")
  data("iris")
  iris_hf <- as.hydrar.frame(as.data.frame(iris))
  
  iris_transform <- hydrar.ml.preprocess(
    iris_hf, transformPath = "/tmp",
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
# end hydrar.ml.preprocess
