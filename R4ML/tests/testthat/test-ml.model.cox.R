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
#library (HydraR)

context("Testing hydrar.cox\n")

test_that("hydrar.coxph", {
  require(SparkR)
  require(HydraR)
  
  surv <- data.frame("Other"    = c(1, 2, 3, 4, 5, 6),
                    "Gender"   = c(1, 0, 0, 1, 1, 0),
                    "Race"     = c(1, 0, 0, 2, 6, 2),
                    "Origin"   = c(2, 0, 0, 15, 0, 2),
                    "Censor"   = c(1, 0, 0, 0, 1, 1),
                    "Age"      = c(20, 17, 25, 52, 52, 52),
                    "Timestamp"= c(1, 2, 3, 4, 5, 6))

  surv <- as.hydrar.frame(surv)

  coxsurvdc <- hydrar.ml.preprocess(surv,
                                    transformPath = "/tmp/cox/survcox2.transform",
                                    dummycodeAttrs = c("Origin", "Gender"),
                                    recodeAttrs = c("Origin", "Gender"))
  
  cox_formula <- Surv(Timestamp, Censor) ~ Gender_1 + Gender_2 + Origin_1 + Origin_2 + Origin_3 + Age
  
  cox_obj <- hydrar.coxph(formula = cox_formula,
                          data = coxsurvdc$data,
                          baseline = list("Origin_1", "Origin_2", "Origin_3", "Gender_1", "Gender_2")
                          )

  expect_equal(round(cox_obj@coxModel$coef[1]), -6)
  expect_equal(round(cox_obj@coxModel$coef[6]), 0)  
  
  coxpred <- data.frame("Gender" = c(1, 0, 0, 0), 
                        "Origin" = c(0, 2, 14, 15),
                        "Censor" = c(0, 1, 0, 0), 
                        "Age" = c(65, 56, 45, 90),
                        "Timestamp" = c(5, 6, 8, 9))
  
  coxsurvpredbf <- as.hydrar.frame(coxpred)

  coxsurvpredc <- hydrar.ml.preprocess(coxsurvpredbf,
                                      transformPath = "/tmp/cox/survcox2.transform",
                                      dummycodeAttrs = c("Origin", "Gender"),
                                      recodeAttrs = c("Origin", "Gender"))

  pred <- predict.hydrar.coxph(cox_obj, data = coxsurvpredc$data)
  
  pred <- SparkR::as.data.frame(pred)
  
  expect_equal(round(pred$lp[1]), -397)
  
  })
