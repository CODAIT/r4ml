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

test_that("hydrar.kaplan.meier summary", {
  surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),Race=c(1,0,0,2,6,2,0,0,0),Origin=c(2,0,0,15,0,2,0,0,0),Age=c(50,52,50,52,52,50,20,50,52))
  survFrame <- as.hydrar.frame(surv)
  survMatrix <- as.hydrar.matrix(survFrame)
  ml.coltypes(survMatrix) <- c("scale", "nominal", "nominal", "scale", "nominal") 
  survFormula <- Surv(Timestamp, Censor) ~ Age
  km <- hydrar.kaplan.meier(survFormula, data=survMatrix,
                            test=1, rho="wilcoxon")
  summary = summary.hydrar.kaplan.meier(km)
  test = hydrar.kaplan.meier.test(km)
  expect_equal(sum(summary[grep("20", names(summary))][[1]]), 0)
  expect_equal(sum(summary[grep("50", names(summary))][[1]]), 9)
  expect_equal(sum(summary[grep("52", names(summary))][[1]]), 9)
})

test_that("hydrar.kaplan.meier tests", {
  surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),Race=c(1,0,0,2,6,2,0,0,0),Origin=c(2,0,0,15,0,2,0,0,0),Age=c(50,52,50,52,52,50,20,50,52))
  survFrame <- as.hydrar.frame(surv, repartition = FALSE)
  survMatrix <- as.hydrar.matrix(survFrame)
  ml.coltypes(survMatrix) <- c("scale", "nominal", "nominal", "scale", "nominal") 
  survFormula <- Surv(Timestamp, Censor) ~ Age
  km <- hydrar.kaplan.meier(survFormula, data=survMatrix,
                            test=1, rho="wilcoxon")
  summary = summary.hydrar.kaplan.meier(km)
  test = hydrar.kaplan.meier.test(km)
  
  expect_true(as.numeric(head(test[[1]])[4][[1]][1]) - 0.3333333 < .001)
  expect_true(as.numeric(head(test[[1]])[5][[1]][2]) - 0.04938272 < .001)
})


test_that("hydrar.kaplan.meier none", {
  surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),Race=c(1,0,0,2,6,2,0,0,0),Origin=c(2,0,0,15,0,2,0,0,0),Age=c(50,52,50,52,52,50,20,50,52))
  survFrame <- as.hydrar.frame(surv)
  survMatrix <- as.hydrar.matrix(survFrame)
  ml.coltypes(survMatrix) <- c("scale", "nominal", "nominal", "scale", "nominal") 
  survFormula <- Surv(Timestamp, Censor) ~ Age
  km <- hydrar.kaplan.meier(survFormula, data=survMatrix,
                            test=1, rho="none")
  summary = summary.hydrar.kaplan.meier(km)
  
  expect_equal(sum(summary[grep("50", names(summary))][[1]]), 9)
  
  #@TODO create a more robust test case as the order of the vars in test may change:
  #expect_true(as.numeric(head(test[[1]])[4][[1]][1]) - 0.3333333 < .001)
  #expect_true(as.numeric(head(test[[1]])[5][[1]][2]) - 0.04938272 < .001)
})


test_that("hydrar.kaplan.meier log-rank", {
  surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),Race=c(1,0,0,2,6,2,0,0,0),Origin=c(2,0,0,15,0,2,0,0,0),Age=c(50,52,50,52,52,50,20,50,52))
  survFrame <- as.hydrar.frame(surv)
  survMatrix <- as.hydrar.matrix(survFrame)
  ml.coltypes(survMatrix) <- c("scale", "nominal", "nominal", "scale", "nominal") 
  survFormula <- Surv(Timestamp, Censor) ~ Age
  km <- hydrar.kaplan.meier(survFormula, data=survMatrix,
                            test=1, rho="log-rank")
  summary = summary.hydrar.kaplan.meier(km)
  test = hydrar.kaplan.meier.test(km)
  
  expect_equal(sum(summary[grep("50", names(summary))][[1]]), 9)
  
  expect_true(as.numeric(SparkR::head(test[[1]])[4][[1]][1]) - 0.4777778 < .001)
  expect_true(as.numeric(SparkR::head(test[[1]])[5][[1]][2]) - 0.06188374 < .001)
})
