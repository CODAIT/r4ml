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

test_that("r4ml.kaplan.meier summary", {
  surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),Race=c(1,0,0,2,6,2,0,0,0),Origin=c(2,0,0,15,0,2,0,0,0),Age=c(50,52,50,52,52,50,20,50,52))
  survFrame <- as.r4ml.frame(surv)
  survMatrix <- as.r4ml.matrix(survFrame)
  ml.coltypes(survMatrix) <- c("scale", "nominal", "nominal", "scale", "nominal") 
  survFormula <- Surv(Timestamp, Censor) ~ Age
  km <- r4ml.kaplan.meier(survFormula, data = survMatrix, test.type = "wilcoxon")
  summary(km)
})

test_that("r4ml.kaplan.meier tests", {
  surv <- data.frame(Timestamp = c(1, 2, 3, 4, 5, 6, 7, 8, 9),
                     Censor = c(1, 0, 0, 1, 1, 0, 0, 1, 0),
                     Race = c(1, 0, 0, 2, 6, 2, 0, 0, 0),
                     Age = c(50, 52, 50, 52, 52, 50, 20, 50, 52))

  survFrame <- as.r4ml.frame(surv, repartition = FALSE)
  survMatrix <- as.r4ml.matrix(survFrame)

  survFormula <- Surv(Timestamp, Censor) ~ Race + Age

  km <- r4ml.kaplan.meier(survFormula, data = survMatrix,
                            test.type = "wilcoxon")
  summary <- summary(km)
  test <- r4ml.kaplan.meier.test(km)

  km_test_full <- SparkR::as.data.frame(test$km_test_full)
  km_test_chsqr <- SparkR::as.data.frame(test$km_test_chsqr)

  expect_equal(dim(km_test_full), c(7, 7))
  expect_equal(dim(km_test_chsqr), c(1, 4))
})


test_that("r4ml.kaplan.meier none", {
  surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),Race=c(1,0,0,2,6,2,0,0,0),Origin=c(2,0,0,15,0,2,0,0,0),Age=c(50,52,50,52,52,50,20,50,52))
  survFrame <- as.r4ml.frame(surv)
  survMatrix <- as.r4ml.matrix(survFrame)
  ml.coltypes(survMatrix) <- c("scale", "nominal", "nominal", "scale", "nominal") 
  survFormula <- Surv(Timestamp, Censor) ~ Age
  km <- r4ml.kaplan.meier(survFormula, data = survMatrix, test.type = "none")
  summary <- summary(km)
  
  expect_error(r4ml.kaplan.meier.test(km))
})

test_that("r4ml.kaplan.meier log-rank", {
  surv <- data.frame(Timestamp = c(1, 2, 3, 4, 5, 6, 7, 8, 9),
                     Censor = c(1, 0, 0, 1, 1, 0, 0, 1, 0),
                     Race = c(1, 0, 0, 2, 6, 2, 0, 0, 0),
                     Age = c(50, 52, 50, 52, 52, 50, 20, 50, 52))

  survFrame <- as.r4ml.frame(surv, repartition = FALSE)
  survMatrix <- as.r4ml.matrix(survFrame)

  survFormula <- Surv(Timestamp, Censor) ~ Race + Age

  formula <- Surv(time, status) ~ age + ph_karno + pat_karno + wt_loss
  km <- r4ml.kaplan.meier(survFormula, data = survMatrix,
                            test.type = "log-rank")
  summary <- summary(km)
  test = r4ml.kaplan.meier.test(km)
  
  km_test_full <- SparkR::as.data.frame(test$km_test_full)
  km_test_chsqr <- SparkR::as.data.frame(test$km_test_chsqr)

  expect_equal(dim(km_test_full), c(7, 7))
  expect_equal(dim(km_test_chsqr), c(1, 4))
})

test_that("r4ml.kaplan.meier accuracy", {
  
  df <- survival::lung
  df$inst <- NULL
  df$sex <- NULL
  df <- stats::na.omit(df)
  colnames(df) <- c("time", "status", "age", "ph_ecog", "ph_karno", "pat_karno",
                    "meal_cal", "wt_loss")

  df$status <- df$status - 1
  
  hf <- as.r4ml.matrix(df)

  library("survival")
  formula <- Surv(time, status) ~ age + ph_karno + pat_karno + wt_loss

  h_km <- r4ml.kaplan.meier(hf, formula)
  r_km <- survival::survfit(formula = formula, data = df, conf.type = "log",
                            conf.int = .95)
  
  h_km_table <- SparkR::as.data.frame(h_km@median)
  
  r_km_summary <- summary(r_km)
  r_km_table <- as.data.frame(r_km_summary$table) 
  
  expect_equal(summary(h_km_table$events), summary(r_km_table$events), tol = .001)
  expect_equal(summary(h_km_table$median), summary(r_km_table$median), tol = .001)

})

test_that("r4ml.kaplan.meier input validation", {
  df <- survival::lung[1:10, ]
  df$inst <- NULL
  df$sex <- NULL
  df <- stats::na.omit(df)
  colnames(df) <- c("time", "status", "age", "ph_ecog", "ph_karno", "pat_karno",
                    "meal_cal", "wt_loss")

  df_bad <- df
  df_good <- df

  df_good$status <- df_good$status - 1

  hf_bad <- as.r4ml.matrix(df_bad)
  hf_good <- as.r4ml.matrix(df_good)

  formula <- Surv(time, status) ~ age + ph_karno + pat_karno + wt_loss

  hkm <- r4ml.kaplan.meier(hf_good, formula)

  expect_error(r4ml.kaplan.meier(hf_bad, formula))
  r4ml.kaplan.meier(hf_good, formula)
})
