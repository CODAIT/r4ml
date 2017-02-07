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
# This class represents Cox proportional hazard (coxph) regression models.
setClass("hydrar.coxph",
         representation(
           tolerance = "numeric",
           iter.max.outer = "numeric",
           iter.max.inner = "numeric",
           conf.int = "numeric",
           baseline = "character",
           testStatistics = "data.frame",
           parameterStatistics = "data.frame",
           coxModel = "data.frame"),
         contains = "hydrar.model"
)

#' @name hydrar.coxph
#' @export
#' @title Cox proportional hazards models
#' @description Fits a Cox proportional hazards regression model with time dependant coefficients or loads an existing model from HDFS.
#' @details The Cox proportional hazard analysis is a semi-parametric statistical estimator. It is used
#' to handle censored time-to-event data. It is semi-parametric because it does not make any assumptions on 
#' the distribution of the timestamps yet it assumes that the hazard ratios remain constant.
#' In this implementation, we use Breslow's approximation to handle ties and the regression parameters are calculated using
#' trust region Newton's method with conjugate gradient.
#' 
#' The input dataset should have the following columns:
#'
#' - \emph{Censor}: binary value indicating whether the patient stayed or left the study at the given timestamp.\cr
#' - \emph{Timestamp}: time of the event\cr
#' - \emph{Grouping column(s)}: (optional)\cr
#' 
#' \strong{Note}: All categorical column must be dummycoded, both timestamp and
#' censor must be scale, and censor column must be binary.
#' 
#' @param data (hydrar.matrix) Input dataset
#' @param formula (character) describes data columns used for survival analysis, e.g., Surv(Timestamp, Censor) ~ Age.
#'              The left side of the formula indicates the timestamp and censor columns, 
#'              while the right side indicates the grouping column(s) if any.
#'              
#' @param baseline (list) A list of baseline column names. The column names should be the names of the columns we are planning to dummy code. 
#'                       For instance
#' @param conf.int (numeric) Regularization parameter
#' @param tolerance (numeric) tolerance value
#' @param iter.max.inner (numeric) Max. number of inner (conjugate gradient) iterations (0 = unlimited)
#' @param iter.max.outer (numeric) Max. number of outer (Newton) iterations
#' @param test (numeric) indicates whether the tests should be calculated (1 = yes, 0 = no)
#' @param directory (character) The HDFS path to save the Cox model if input data is specified.
#'        Otherwise, an HDFS location with a previously trained model to be loaded.
#' @return An S4 object of class \code{hydrar.coxph} which contains the arguments above as well as the following additional fields:
#' 
#'  \tabular{rlll}{
##'  \tab\code{conf.int}   \tab (numeric) \tab The level of two-sided confidence interval. The default value is 0.95\cr
##'  \tab\code{baseline}  \tab (character) \tab A list of baseline columns.\cr
##'  \tab\code{coxModel}  \tab (data.frame) \tab The cox model parameters which include coefficients, standard error, P-value, Z-score lower and upper
##'                                             confidence \cr
##'  \tab\code{testStatistics}    \tab (data.frame) \tab Test statistics include: Wald test, log likelihood ratio and logrank tests.\cr
##'  \tab\code{parameterStatistics}  \tab (data.frame) \tab Parameter statistics data frame includes the number of records, number of events, 
##'  log likelihood, AIC criterion, Rsquare (Cox & Snell) and maximum possible RSquare \cr
##'  \tab\code{featureNames} \tab (character) \tab List of feature names used for analysis\cr
##'  \tab\code{call}         \tab (character) \tab String representation of this method's call, including the parameters and values passed to it.\cr
#'  }
#'
#' @details Cox proportional hazards is a method used in biostatistics to handle time-to-event data.
#' @examples \dontrun{
#' surv <- data.frame("Other"    = c(1, 2, 3, 4, 5, 6),
#'                   "Gender"   = c(1, 0, 0, 1, 1, 0),
#'                   "Race"     = c(1, 0, 0, 2, 6, 2),
#'                   "Origin"   = c(2, 0, 0, 15, 0, 2),
#'                   "Censor"   = c(1, 0, 0, 0, 1, 1),
#'                   "Age"      = c(20, 17, 25, 52, 52, 52),
#'                   "Timestamp"= c(1, 2, 3, 4, 5, 6))
#' 
#' surv <- as.hydrar.frame(surv)
#' 
#' coxsurvdc <- hydrar.ml.preprocess(surv,
#'                                   transformPath = "/tmp",
#'                                   dummycodeAttrs = c("Origin", "Gender"),
#'                                   recodeAttrs = c("Origin", "Gender"))
#' 
#' cox_formula <-Surv(Timestamp, Censor) ~ Gender_1 + Gender_2 + Origin_1 + Origin_2 + Origin_3 + Age
#' 
#' cox_obj <- hydrar.coxph(formula = cox_formula,
#'                          data = coxsurvdc$data,
#'                          baseline = list("Origin_1", "Origin_2", "Origin_3", "Gender_1", "Gender_2")
#'                          )
#' }
#' @seealso \link{summary.hydrar.coxph}
#' @seealso \link{predict.hydrar.coxph}
#' 
hydrar.coxph <- function(data,
                         formula,
                         baseline,
                         tolerance = 0.000001, # DML default
                         conf.int = 0.05, # DML default
                         iter.max.inner = 0, # DML default
                         iter.max.outer = 100, # DML default
                         directory = file.path(tempdir(), "HydraR", "cox")) {
  new("hydrar.coxph",
      modelType = "other",
      data = data,
      directory = directory,
      formula = formula,
      baseline = baseline,
      tolerance = tolerance,
      conf.int = conf.int,
      iter.max.inner = iter.max.inner,
      iter.max.outer = iter.max.outer,
      directory = directory)
}

setMethod("hydrar.model.validateTrainingParameters", signature="hydrar.coxph", def = 
            function(model, args) {
              logSource <- "hydrar.model.validateTrainingParameters"
              with(args, {
                .hydrar.checkParameter(logSource, formula, expectedClasses = "formula")
                .hydrar.checkParameter(logSource, baseline, inheritsFrom = "list", isOptional = TRUE)
                .hydrar.checkParameter(logSource, tolerance, inheritsFrom = c("integer","numeric"), isOptional = TRUE)
                .hydrar.checkParameter(logSource, conf.int, inheritsFrom = c("integer","numeric"), isOptional = TRUE)
                .hydrar.checkParameter(logSource, iter.max.inner, inheritsFrom = c("integer","numeric"), isOptional = TRUE)
                .hydrar.checkParameter(logSource, iter.max.outer, inheritsFrom = c("integer","numeric"), isOptional = TRUE)
                
                if(!missing(tolerance) && tolerance < 0) {
                  hydrar.err(logSource, "Parameter tolerance must be a positive real number")
                  }
                
                if (!missing(conf.int) && (conf.int < 0 || conf.int > 1)) {
                  hydrar.err(logSource, "Parameter conf.int must be a value between 0 and 1")
                }
                
                if (!missing(iter.max.inner) && iter.max.inner < 0) {
                  hydrar.err(logSource, "Parameter iter.max.inner must be a positive interger")
                }
                
                if (!missing(iter.max.outer) && iter.max.outer < 0) {
                  hydrar.err(logSource, "Parameter iter.max.outer must be a positive interger")
                  }
                })
              
              return (model)
              })
      
setMethod("hydrar.model.buildTrainingArgs", signature="hydrar.coxph", def = 
            function(model, args) {
              logSource <- "hydrar.model.buildTrainingArgs.coxph"
              with(args, {
              dmlArgs <- list(
                  X_orig = data,
                  dml = file.path(hydrar.env$SYSML_ALGO_ROOT(), hydrar.env$DML_COX_SCRIPT),
                  fmt = "csv",
                  "M",
                  "RT",
                  "COV",
                  "XO",
                  "S",
                  "T",
                  "MF",
                  "TE_F")
                
                # time_status and feature-id paths
                survTSFList <- hydrar.parseSurvivalArgsFromFormulaTree(formula, data, directory)
                timeAndStatusIds <- survTSFList[[1]]
                timeAndStatusIdsFrame <- as.hydrar.frame(data.frame(timeAndStatusIds), 
                                                         repartition = FALSE)
                survTSMatrix <- as.hydrar.matrix(timeAndStatusIdsFrame)
                dmlArgs <- c(dmlArgs, TE = survTSMatrix)
                
                if (length(survTSFList) > 1) {
                  featureIds <- survTSFList[[2]]
                  
                  survFrame <- as.hydrar.frame(as.data.frame(featureIds),
                                               repartition = FALSE)
                  survFMatrix <- as.hydrar.matrix(survFrame)
                  
                  dmlArgs <- c(dmlArgs, F = survFMatrix)
                  } else {
                    hydrar.err(logSource, "specify at least one feature")
                  }
                
                # adding baseline to the model
                if(!missing(baseline)) {
                  #adding feature-id path to the model
                  model@featureNames <- SparkR::colnames(data)
                  model@baseline <- as.character(baseline)
                  baselineFrame <- hydrar.parseBaselineIds(baseline, data, directory)
                  baselineMatrix <- as.hydrar.matrix(baselineFrame)
                  #@TODO check that only dummy coded columns can be baseline
                  dmlArgs <- c(dmlArgs, R = baselineMatrix)
                  } else {
                    model@featureNames <- data@colnames[featureIds]
                  }
                
                if (!missing(tolerance)) {
                  dmlArgs <- c(dmlArgs, tol = tolerance)
                  model@tolerance <- tolerance
                }
                  
                if (!missing(conf.int)) {
                  dmlArgs <- c(dmlArgs, alpha = conf.int)
                  model@conf.int <- conf.int
                }
                
                if (!missing(iter.max.outer)) {
                  dmlArgs <- c(dmlArgs, moi = iter.max.outer)
                  model@iter.max.outer <- iter.max.outer
                }
                  
                if (!missing(iter.max.inner)) {
                  dmlArgs <- c(dmlArgs, mii = iter.max.inner)
                  model@iter.max.inner <- iter.max.inner
                }
                
                model@dmlArgs <- dmlArgs
                return (model)
              })
              }
          )

# overwrite the base model's post training function so that one can
# post process the final outputs from the dml scripts
setMethod("hydrar.model.postTraining", signature="hydrar.coxph", def =
  function(model) {
    outputs <- model@dmlOuts$sysml.execute

    S <- SparkR::as.data.frame(outputs$S)
    T <- SparkR::as.data.frame(outputs$T)
    M <- SparkR::as.data.frame(outputs$M)
    
    stats <- list(observations = S[1, 1],
                  events = S[2, 1],
                  log_likelihood = S[3, 1],
                  AIC = S[4, 1],
                  Rsquare = S[5, 1],
                  Rsquare_max = S[6, 1],
                  likelihood_ratio_test_stat = T[1, 1],
                  likelihood_ratio_test_dof = T[1, 2],
                  likelihood_ratio_test_p = T[1, 3],
                  wald_test_stat = T[2, 1],
                  wald_test_dof = T[2, 2],
                  wald_test_p = T[2, 3],
                  score_test_stat = T[3, 1],
                  score_test_dof = T[3, 2],
                  score_test_p = T[3, 3]
                  )
    
    model@dmlOuts$stats <- stats
    model@testStatistics <- T
    model@parameterStatistics <- S
    
    colnames(M) <- c("coef",
                    "exp(coef)",
                    "se(coef)",
                    "Z",
                    "P-value",
                    (1-model@conf.int) %++% " lower",
                    (1-model@conf.int) %++% " upper")

    if (!is.na(model@baseline) && !is.null(model@baseline) && length(model@baseline) > 0) {
      modelFeatures <- SparkR::as.data.frame(outputs$MF)
      featureIds <- modelFeatures[, 1]
      featureIds <- featureIds[-1:-2] # remove timestamp & censor
      modelFeatureNames <- model@featureNames[featureIds]
    } else {
      modelFeatureNames <- model@featureNames
    }
     
    rownames(M) <- modelFeatureNames
    model@coxModel <- M
    
    outNames <- names(outputs)
    i <- 1
    for (output in outputs) {
      outName <- outNames[i]
      i <- i + 1
      model@dmlOuts[outName] <- output
    }
    
    return(model)
  }
)

setMethod(f = "show",
          signature = "hydrar.coxph",
          definition = 
            function(object) {
              logSource <- "hydrar.coxph"
              callNextMethod()
              cat("\n\nCox Model\n\n")
              print(object@coxModel)
              cat("\n\n")
              print(object@testStatistics)
              cat("\n\n")
              print(object@parameterStatistics)
              }
)
#' @name summary.hydrar.coxph
#' @title Cox Summary
#' @description Summarizes cox proportional hazard analysis
#' @param object A cox model \code{hydrar.matrix}
#' @return Three matrices: Statistical parameters, tests and model
#' @export
#' @examples \dontrun{
#' surv <- data.frame("Other"    = c(1, 2, 3, 4, 5, 6),
#'                   "Gender"   = c(1, 0, 0, 1, 1, 0),
#'                   "Race"     = c(1, 0, 0, 2, 6, 2),
#'                   "Origin"   = c(2, 0, 0, 15, 0, 2),
#'                   "Censor"   = c(1, 0, 0, 0, 1, 1),
#'                   "Age"      = c(20, 17, 25, 52, 52, 52),
#'                   "Timestamp"= c(1, 2, 3, 4, 5, 6))
#' 
#' surv <- as.hydrar.frame(surv)
#' 
#' coxsurvdc <- hydrar.ml.preprocess(surv,
#'                                   transformPath = "/tmp",
#'                                   dummycodeAttrs = c("Origin", "Gender"),
#'                                   recodeAttrs = c("Origin", "Gender"))
#' 
#' cox_formula <- Surv(Timestamp, Censor) ~ Gender_1 + Gender_2 + Origin_1 + Origin_2 + Origin_3 + Age
#'
#' cox_obj <- hydrar.coxph(formula = cox_formula,
#'                          data = coxsurvdc$data,
#'                          baseline = list("Origin_1", "Origin_2", "Origin_3", "Gender_1", "Gender_2")
#'                          )
#'
#' summary(cox_obj)
#' }
#' @seealso \code{\link{hydrar.coxph}}
#' @seealso \link{predict.hydrar.coxph}
summary.hydrar.coxph <- function(object) {
  logSource <- "hydrar.coxph"
  
  coxSummary <- list(cox_model = object@coxModel,
                     cox_stat_test = object@testStatistics,
                     cox_param_tests = object@parameterStatistics)
  
  return (coxSummary)
}

# Parse a list of baseline and store it in a hydrar.frame
hydrar.parseBaselineIds <- function(baseline, data, directory) {
  baselineIds <- hydrar.extractColsFromFormulaTree(baseline, data, delimiter = ",")
  from <- c()
  to <- c()
  for (i in 1:length(baselineIds)) {
    elems <- baselineIds[[i]]
    from <- c(from, elems[1])
    if (length(elems) > 1) {
      to <- c(to, elems[length(elems)])
    } else {
      to <- c(to, elems[1])
    }
  }
  
  baselineIdsFrame <- as.hydrar.frame(data.frame(from, to), repartition = FALSE)
  return (baselineIdsFrame)
}

#' @name predict.hydrar.coxph
#' @export
#' @title Predict method for Cox proportional hazards models
#' @description This method allows to make linear, risk, and cumulative hazard predictions for each entry in the test data set.
#' @param object (hydrar.coxph) The cox regression model
#' @param data   (hydrar.matrix) The test data. Must be in the same format as the training data
#' @return A hydrar.matrix with predictions for each row.
#' 
#' @examples \dontrun{
#' surv <- data.frame("Other"    = c(1, 2, 3, 4, 5, 6),
#'                   "Gender"   = c(1, 0, 0, 1, 1, 0),
#'                   "Race"     = c(1, 0, 0, 2, 6, 2),
#'                   "Origin"   = c(2, 0, 0, 15, 0, 2),
#'                   "Censor"   = c(1, 0, 0, 0, 1, 1),
#'                   "Age"      = c(20, 17, 25, 52, 52, 52),
#'                   "Timestamp"= c(1, 2, 3, 4, 5, 6))
#' 
#' surv <- as.hydrar.frame(surv)
#' 
#' coxsurvdc <- hydrar.ml.preprocess(surv,
#'                                   transformPath = "/tmp",
#'                                   dummycodeAttrs = c("Origin", "Gender"),
#'                                   recodeAttrs = c("Origin", "Gender"))
#' 
#' cox_formula <- Surv(Timestamp, Censor) ~ Gender_1 + Gender_2 + Origin_1 + Origin_2 + Origin_3 + Age
#'
#' cox_obj <- hydrar.coxph(formula = cox_formula,
#'                          data = coxsurvdc$data,
#'                          baseline = list("Origin_1", "Origin_2", "Origin_3", "Gender_1", "Gender_2")
#'                          )
#' 
#' coxpred <- data.frame("Gender" = c(1, 0, 0, 0), 
#'                       "Origin" = c(0, 2, 14, 15),
#'                       "Censor" = c(0, 1, 0, 0), 
#'                       "Age" = c(65, 56, 45, 90),
#'                       "Timestamp" = c(5, 6, 8, 9))
#' 
#' coxsurvpredbf <- as.hydrar.frame(coxpred)
#' 
#' coxsurvpredc <- hydrar.ml.preprocess(coxsurvpredbf,
#'                                   transformPath = "/tmp",
#'                                   dummycodeAttrs = c("Origin", "Gender"),
#'                                   recodeAttrs = c("Origin", "Gender"))
#' 
#' pred <- predict.hydrar.coxph(cox_obj, data = coxsurvpredc$data)
#'}
#' 
#' @seealso \link{summary.hydrar.coxph}
#' @seealso \link{hydrar.coxph}
#' 
predict.hydrar.coxph <- function(object, data) {
  
  logSource <- "predict.hydrar.coxph"
  
  .hydrar.checkParameter(logSource, data, inheritsFrom = "hydrar.matrix", isOptional = FALSE)
  
  if (is.null(object@dmlOuts$M) || is.null(object@dmlOuts$COV) || is.null(object@dmlOuts$RT) || is.null(object@dmlOuts$XO)) {
    hydrar.err(logSource, "The model is incomplete. Some of the expected files are missing")
    }
  
  args <- list(Y_orig = data,
               dml = file.path(hydrar.env$SYSML_ALGO_ROOT(), hydrar.env$DML_COX_PREDICT_SCRIPT),
               M = object@dmlOuts$M,
               RT_X = object@dmlOuts$RT,
               COV = object@dmlOuts$COV,
               "P",
               X_orig = object@dmlOuts$XO,
               col_ind = object@dmlOuts$MF,
               fmt = "csv")
    
  # execute cox predict dml script
  hydrar.info(logSource, "Running DML")
  hydrar.infoShow(logSource, args)
  
  dmlOuts <- do.call("sysml.execute", args)
  prediction <- dmlOuts$P
  SparkR::colnames(prediction) <- c("lp", "se(lp)", "risk", "se(risk)", "cum.hazard", "se(cum.hazard)")

  prediction <- as.hydrar.matrix(prediction)
  return (prediction)
}
