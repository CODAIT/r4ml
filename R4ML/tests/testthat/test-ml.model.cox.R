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

context("Testing hydrar.cox\n")

test_that("hydrar.coxph", {
  
  # this test case works by building 2 cox models: 1 using survival::coxph and
  # 1 using HydraR::hydrar.coxph. It then checks to make sure the outputs are
  # the same
  
  
  library("SparkR")
  library("survival")
  
  data("lung")
  colnames(lung) <- c("inst", "time", "status", "age", "sex", "ph_ecog",
                      "ph_karno", "pat_karno", "meal_cal", "wt_loss" )
  
  lung$meal_cal[which(is.na(lung$meal_cal))] <- 930
  lung$wt_loss[which(is.na(lung$wt_loss))] <- 10
  lung$ph_ecog[which(is.na(lung$ph_ecog))] <- 1
  lung$pat_karno[which(is.na(lung$pat_karno))] <- 80
  lung$inst[which(is.na(lung$inst))] <- 11
  lung$ph_karno[which(is.na(lung$ph_karno))] <- 80
  
  
  hydrar_lung <- as.hydrar.frame(lung, repartition = FALSE)
  
  hydrar_lung_pp <- hydrar.ml.preprocess(hydrar_lung,
                                         transformPath = file.path(tempdir(), "cox"),
                                         dummycodeAttrs = c("sex", "ph_ecog"),
                                         recodeAttrs = c("sex", "ph_ecog"))
  lung <- SparkR::as.data.frame(hydrar_lung_pp$data)
  
  surv_formula <- Surv(time, status) ~ age + sex_1 + ph_ecog_1 + ph_ecog_2 + ph_ecog_3
  
  cox_fit <- coxph(surv_formula, lung)
  hydrar_cox_fit <- hydrar.coxph(hydrar_lung_pp$data, surv_formula,
                                 baseline = list("sex_1", "ph_ecog_1", "ph_ecog_2", "ph_ecog_3"))

  expect_true(abs(hydrar_cox_fit@coxModel["age","coef"] - cox_fit$coefficients["age"]) < .1)
  
  #@TODO test predict
  
  #predict(cox_fit, type = "lp")
  #hydrar_pred <- predict.hydrar.coxph(hydrar_cox_fit, hydrar_lung_pp$data)
  #predict(cox_fit, type = "expected")
  #predict(cox_fit, type = "risk", se.fit = TRUE)
  #predict(cox_fit, type = "terms", se.fit = TRUE)
  })

test_that("hydrar.coxph accuracy", {
  df <- survival::lung
  df$inst <- NULL
  df$sex <- NULL
  df <- stats::na.omit(df)
  colnames(df) <- c("time", "status", "age", "ph_ecog", "ph_karno", "pat_karno",
                    "meal_cal", "wt_loss")

  hf <- as.hydrar.matrix(df)

  library("survival")
  cox_formula <- Surv(time, status) ~ age + ph_karno + pat_karno + wt_loss

  h_cox <- hydrar.coxph(hf, cox_formula)
  r_cox <- coxph(formula = cox_formula, data = df)

  h_cm <- h_cox@coxModel
  r_cm <- r_cox$coefficients

  # use this as the R 3.1 version's subpackage testthat 0.9.1 can't handle 
  # expect_equal. And some of our customer might be using 3.1
  expect_true(abs(h_cm["age", "coef"]-r_cm["age"][[1]]) <= 0.01)

  expect_true(abs(h_cm["ph_karno", "coef"]-r_cm["ph_karno"][[1]])<= 0.01)

  expect_true(abs(h_cm["pat_karno", "coef"]-r_cm["pat_karno"][[1]]) <= 0.01)

  expect_true(abs(h_cm["wt_loss", "coef"]-r_cm["wt_loss"][[1]]) <= 0.01)

  h_predict <- predict(h_cox, data = hf)
  r_predict <- predict(r_cox, data = df)

  #@TODO fix cox predict and create accuracy test

})
