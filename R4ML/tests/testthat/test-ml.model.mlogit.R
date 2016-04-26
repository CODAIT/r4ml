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

context("Testing hydrar.mlogit\n")

# test hydra logistic regression

test_that("hydrar.mlogit", {
  require(SparkR)
  require(HydraR)
  df <- iris
  df$Species <- (as.numeric(df$Species))
  iris_df <- as.hydrar.frame(df)
  iris_mat <- as.hydrar.matrix(iris_df)
  ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
  iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat)
  
  show(iris_log_reg)
  coef(iris_log_reg)
})