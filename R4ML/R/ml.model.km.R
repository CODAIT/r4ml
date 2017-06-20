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
# This class represents a Kaplan-Meier model
#' @include ml.model.base.R
#' @include zzz.R
setClass("r4ml.kaplan.meier",
         slots = c(
           conf.int = "numeric",
           summary = "list",
           median = "r4ml.matrix",
           testPath = "character",
           dataColNames = "vector",
           groupNames = "vector",
           strataNames = "vector"
         ), contains = "r4ml.model"
)
#' @name r4ml.kaplan.meier
#' @title Kaplan-Meier Analysis
#' @description Fits a Kaplan-Meier model from an r4ml.matrix
#' @details Kaplan-Meier analysis is a non-parametric statistical estimator which can be applied to survival data.
#' It is often used to estimate the proportion of living/survived objects/patients during certain time period of time.
#' This implementation allows a user to calculate the survival function and the median of survival timestamps, as well as 
#' comparing different groups using statistical tests.
#'
#' The input dataset should have the following columns:
#'
#' - \emph{Censor}: binary value indicating whether the patient stayed or left the study at the given timestamp.\cr
#' - \emph{Timestamp}: time of the event\cr
#' - \emph{Strata column(s)}: (optional)\cr
#' - \emph{Grouping column(s)}: (optional)\cr
#'
#' @param data (r4ml.matrix):  Input dataset
#' @param formula (character):  Describes data columns used for survival analysis, e.g., Surv(Timestamp, Censor) ~ Age.
#'              The left side of the formula indicates the timestamp and censor columns, 
#'              while the right side indicates the grouping column(s) if any.
#' @param strata (list):  A list of stratum columns
#' @param conf.int (numeric):  Regularization parameter
#' @param conf.type (character):  Confidence type: accepted values are 'plain', 'log' (the default), or 'log-log' 
#' @param error.type (character):  Parameter to specify the error type according to "greenwood" (the default) or "peto"
#' @param test.type (character):  If survival data for multiple groups is available
#'            specifies which test to perform for comparing survival data across
#'            multiple groups: "none" (the default), "log-rank", or "wilcoxon"
#' @param directory (character):  The path to save the Kaplan-Meier model if input data is specified. 
#' @param input.check (logical):  If FALSE parameter validation is skipped
#' @return An S4 object of class \code{r4ml.kaplan.meier} which contains the arguments above as well as the following additional fields:
#' 
#'  \tabular{rlll}{
##'  \tab\code{conf.int}   \tab (numeric) \tab The level of two-sided confidence interval. The default value is 0.95.\cr
##'  \tab\code{summary}  \tab (character) \tab kaplan-meier summary data\cr
##'  \tab\code{testPath}    \tab (character) \tab location where Kaplan-Meier tests are stored \cr
##'  \tab\code{groupNames}  \tab (vector) \tab A list of column names which are used in grouping \cr
##'  \tab\code{strataNames} \tab (vector) \tab A list of column names which are used as strata \cr
##'  \tab\code{dataColNames} \tab (vector) \tab The column names of original survival dataset \cr
##'  \tab\code{modelPath}    \tab (character) \tab location where the \code{r4ml.transform()} metadata are stored \cr
##'  \tab\code{call}         \tab (character) \tab String representation of this method's call, including the parameters and values passed to it.\cr
#'  }  
#'
#' @details Kaplan-Meier estimator is a non-parametric method for survival analysis. Several metrics such as greenwood are used
#' to approximate its variance. It is used in combination with different 
#' statistics such as 'log-rank' and 'wilcoxon' to compare different survival curves.
#' @examples \dontrun{
#' 
#'
#' # Load a sample data set
#' surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),
#'               Race=c(1,0,0,2,6,2,0,0,0), Origin=c(2,0,0,15,0,2,0,0,0),
#'               Age=c(50,52,50,52,52,50,20,50,52))
#' 
#' survmat <- as.r4ml.matrix(surv)
#'   
#' # Create survival formula
#' survformula <- Surv(Timestamp, Censor) ~ Age
#'
#' # Create survival model
#' km <- r4ml.kaplan.meier(survformula, data = survmat, test.type = "log-rank")
#' 
#' # Show summary
#' summary(km)
#' 
##' # Show test results
##' r4ml.kaplan.meier.test(km)
#'}
#'
#' @export
#' @seealso \link{summary.r4ml.kaplan.meier}
#' @seealso \link{r4ml.kaplan.meier.test}
#'
r4ml.kaplan.meier <- function(data, formula, strata, conf.int, conf.type,
                                error.type, test.type = "none", directory,
                                input.check = TRUE) {
  new("r4ml.kaplan.meier", modelType="other", data=data, 
      directory = directory, formula = formula, strata = strata,
      conf.int = conf.int, conf.type = conf.type, error.type = error.type, test.type = test.type,
      input.check = input.check)
}

setMethod("r4ml.model.validateTrainingParameters",
          signature = "r4ml.kaplan.meier", definition =  
  function(model, args) {
    logSource <- "r4ml.model.validateTrainingParameters"
    with(args, {
      .r4ml.checkParameter(logSource, formula, expectedClasses="formula")
      .r4ml.checkParameter(logSource, strata, inheritsFrom="list", isOptional=T)
      .r4ml.checkParameter(logSource, conf.int, inheritsFrom=c("integer","numeric"), isOptional=T)
      .r4ml.checkParameter(logSource, conf.type, inheritsFrom="character", isOptional=T)
      .r4ml.checkParameter(logSource, error.type, inheritsFrom = "character",
                             isOptional = TRUE)
      .r4ml.checkParameter(logSource, test.type, inheritsFrom = "character",
                             isOptional = TRUE)
      .r4ml.checkParameter(logSource, input.check, inheritsFrom = "logical",
                             isOptional = TRUE)
      
      if (!missing(conf.type) && !(conf.type %in% c("plain","log","log-log"))) {
        r4ml.err(logSource, "Parameter conf.type must be either plain (the default), log or log-log")
      }
      if (!missing(conf.int) && (conf.int < 0 || conf.int > 1)) {
        r4ml.err(logSource, "Parameter conf.int must be a positive integer between 0 and 1.")
      }
      if (!missing(error.type) && !(error.type %in% c("greenwood","peto"))) {
        r4ml.err(logSource, "Parameter error.type must be either greenwood (the default) or peto")
      }
      if (!missing(test.type) && !(test.type %in% c("none","log-rank","wilcoxon"))) {
        r4ml.err(logSource, "Parameter test.type must be either none (the default), log-rank, or wilcoxon")
      }
     if (!missing(input.check) && input.check) {

       distinct_vals <- SparkR::distinct(data[, 2])

       if (SparkR::nrow(distinct_vals) != 2) {
         r4ml.err(logSource, "Event status must only contain values 0 and 1")
       }

       distinct_vals <- SparkR::as.data.frame(distinct_vals)
       if (all(sort(distinct_vals[, 1]) != c(0, 1))) {
         r4ml.err(logSource, "Event status must contain values 0 and 1")
       }
      }
    })
    return(model)
  }
)

setMethod("r4ml.model.buildTrainingArgs", signature = "r4ml.kaplan.meier",
          definition = function(model, args) {
    logSource <- "r4ml.model.buildTrainingArgs"
    with(args,  {
      # Defining output directories
      if (missing(directory)){
        directory <- r4ml.env$WORKSPACE_ROOT("r4ml.ml.model.kaplan.meier")
      }

      #adding column names to the model
      model@dataColNames <- SparkR::colnames(data)

      dmlPath <- file.path(r4ml.env$SYSML_ALGO_ROOT(), r4ml.env$DML_KM_SCRIPT)
      # @TODO - Find a way to get the timestamps index to the DML script automatically
      dmlArgs <- list(X = data, dml = dmlPath, "M", "KM", "$fmt" = 'CSV') 
      groupIds <- list()
      
      # time_status and groupid paths
      survTSGList <- r4ml.parseSurvivalArgsFromFormulaTree(formula,data,directory)
      timeAndStatusIds <- survTSGList[[1]]
      timeAndStatusIdsFrame <- as.r4ml.frame(data.frame(timeAndStatusIds),
                                               repartition = FALSE)

      survTSMatrix <- as.r4ml.matrix(timeAndStatusIdsFrame)
      dmlArgs <- c(dmlArgs, TE=survTSMatrix)
      if (length(survTSGList) > 1) {                        
        groupIds <- survTSGList[[2]]
        if (length(groupIds) > 0){
          survGFrame <- as.r4ml.frame(data.frame(groupIds),
                                        repartition = FALSE)
          survGMatrix <- as.r4ml.matrix(survGFrame)
          
          dmlArgs <- c(dmlArgs, GI=survGMatrix)
          model@groupNames <- model@dataColNames[groupIds]
        }
      }
      
      # strata-id paths
      if (!missing(strata)) {
        if (length(strata) > 0) {
          strataIds<- r4ml.extractColsFromFormulaTree(strata,data,delimiter=",")
          strataIds <- unlist(strataIds)
          
          # check if there is an intersection beteen groups and stratas
          # if there is an intersection send an error about it
          if (length(groupIds) > 0){
            intersection <- intersect(groupIds, strataIds)
            if (length(intersection) > 0){
              intersectionstr <- paste(intersection, collapse=',')
              r4ml.err(logSource, paste("Grouping columns must not overlap with strata columns. Overlapping column ids are: ", intersectionstr))
            }
          }
          
          survStrataFrame <- as.r4ml.frame(strataIds, repartition = FALSE)
          #TODO see if it is possible to avoid the creation of matrix
          strataIdsMatrix <- as.r4ml.matrix(survStrataFrame)   
          dmlArgs <- c(dmlArgs, SI=strataIdsMatrix)
          #adding strata-id path to the model
          model@strataNames <- model@dataColNames[strataIds]
          for (i in 1:length(model@strataNames)) {
            model@strataNames[i] <- "strata(" %++% model@strataNames[i] %++% ")"
          }
        }
      }
      
      dmlArgs <- c(dmlArgs, "$ttype" = test.type)
      
      if (!missing(test.type) & test.type != "none") {
        dmlArgs <- c(dmlArgs, "TEST", "TEST_GROUPS_OE")
      }
      
      if (!missing(conf.int)) {
        model@conf.int <- conf.int
      } else {
        model@conf.int <- 0.95 # default value
      }
      dmlArgs <- c(dmlArgs, "$alpha" = (1 - model@conf.int))
      
      if (!missing(error.type)) {
        dmlArgs <- c(dmlArgs, "$etype"=error.type)
      }
      if (!missing(conf.type)) {
        dmlArgs <- c(dmlArgs, "$ctype"=conf.type)
      }
      model@dmlArgs <- dmlArgs
      return(model)
    })
  }                  
)

setMethod("r4ml.model.postTraining", signature = "r4ml.kaplan.meier",
          definition = function(model) {
    ### model@median ###
    median_colnames <- c("n", "events", "median",
                         (1 - model@conf.int) %++% "%LCL",
                         (1 - model@conf.int) %++% "%UCL")
    
    if (!is.na(model@groupNames) && length(model@groupNames) > 0) {
      median_colnames <- c(model@groupNames, median_colnames)
    }

    if (!is.na(model@strataNames) && length(model@strataNames) > 0) {
      median_colnames <- c(model@strataNames, median_colnames)
    }
    
    median <- model@dmlOuts$sysml.execute$M
    SparkR::colnames(median) <- median_colnames
    median <- as.r4ml.matrix(median)

    model@median <- median

    ### model@summary ###

    start <- 1
    if (!is.na(model@strataNames) || !is.na(model@groupNames)) {
      temp <- model@median
      values <- SparkR::as.data.frame(temp)
      }
    
    if (!is.na(model@groupNames) && length(model@groupNames) > 0) {
      groupHeaders_n <- length(model@groupNames)
      groupValues <- values[, 1:groupHeaders_n]
      if (groupHeaders_n == 1) {
        groupValues <- SparkR::as.data.frame(groupValues)
        }
      start <- groupHeaders_n + 1
      }
    if (!is.na(model@strataNames) && length(model@strataNames) > 0) {
      strataHeaders_n <- length(model@strataNames)
      end <- start + strataHeaders_n - 1
      strataValues <- values[, start:end]
      if (strataHeaders_n == 1) {
        strataValues <- SparkR::as.data.frame(strataValues)
      }
    }

    km_colnames <- c("time", "n.risk", "n.event", "survival", "std.err",
                     (1 - model@conf.int) %++% "%LCL",
                     (1 - model@conf.int) %++% "%UCL")
    km_full <- SparkR::as.data.frame(model@dmlOuts$sysml.execute$KM)
    i <- 1
    list_ind <- 1
    km_summary_list <- list()
    while (i < ncol(km_full)) {
      j <- i + 6
      km7 <- km_full[, i:j]
      SparkR::colnames(km7) <- km_colnames
      # Adding caption for groups
      start <- 1
      km7_caption <- c()
      if (!is.na(model@groupNames) && length(model@groupNames) > 0) {
        groupValuesRow <- SparkR::as.data.frame(groupValues[list_ind,])
        groupValuesRow_n <- ncol(groupValuesRow)
        for (i2 in start:groupValuesRow_n) {
          grp_colname <- model@groupNames[i2]
          str <- grp_colname %++% "=" %++% as.character(groupValuesRow[1, i2])
          km7_caption <- paste(km7_caption, str)
          }
        
        # updating the starting point for strata
        start <- groupValuesRow_n + 1
      }
      
      #Adding caption for stratum
      
      if (!is.na(model@strataNames) && length(model@strataNames) > 0) {
        strataValuesRow <- SparkR::as.data.frame(strataValues[list_ind, ])
        for (i2 in 1:ncol(strataValuesRow)) {
          str_colname <- model@strataNames[i2]
          str <- str_colname %++% "=" %++% as.character(strataValuesRow[1, i2])
          km7_caption <- paste(km7_caption, str)
        }
        
        start <- 1
        }
      
      if (is.na(model@groupNames) && is.na(model@strataNames)) {
        km_summary_list[list_ind] <-  km7
      } else {
        km_summary_list[km7_caption] <-  km7
      }
      
      i <- i + 7
      list_ind <- list_ind + 1
    }
    
    model@summary <- km_summary_list
    return(model)
    }
)

setMethod(f = "show", signature = "r4ml.kaplan.meier", definition =
  function(object) {
    logSource <- "show.r4ml.kaplan.meier"
    callNextMethod()

    cat("Survival Median\n")

    median <- object@median

    print(SparkR::head(median, num = r4ml.env$DEFAULT_HEAD_ROWS))
  }
)


#' @title Kaplan-Meier Summary
#' @description Computes different survival estimates for a given r4ml.kaplan.meier model. 
#' @param object (r4ml.kaplan.meier):  Survival model from Kaplan Meier analysis.  
#' @return A list of r4ml.matrix objects with seven columns and as many rows as the input dataset. There
#' will be one r4ml.matrix per group/strata combination. Each matrix will have the following structure:
#' timestamp, number at risk, number of events, Kaplan-Meier estimate of survival function, 
#' standard error of survival, as well as 100*(1-alpha)\% upper and lower 
#' confidence interval bounds of the median.
#' 
#' @examples \dontrun{
#' 
#' # Load a sample data set
#' surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),
#'               Race=c(1,0,0,2,6,2,0,0,0), Origin=c(2,0,0,15,0,2,0,0,0),
#'               Age=c(50,52,50,52,52,50,20,50,52))
#' 
#' survmat <- as.r4ml.matrix(surv) 
#'   
#' # Create survival formula
#' survformula <- Surv(Timestamp, Censor) ~ Age
#' 
#' # Create survival model
#' km <- r4ml.kaplan.meier(survformula, data=survmat, test.type="log-rank", 
#'                             directory = "/tmp")
#' 
#' # Show summary
#' summary(km)
#' 
##' # Show test results
##' r4ml.kaplan.meier.test(km)
#'}
#'
#' @export summary.r4ml.kaplan.meier
#' @seealso \link{r4ml.kaplan.meier}
#' @seealso \link{r4ml.kaplan.meier.test}
summary.r4ml.kaplan.meier <- function(object) {
  logSource <- "r4ml.kaplan.meier"

  summary <- object@summary
  median <- object@median
  
  km_summary_list <- SparkR::as.data.frame(median)
  km_summary_list$time <- NA
  
  for (i in 1:nrow(km_summary_list)) {
    km_summary_list$time[i] <- summary[[i]]
  }
  
  return(km_summary_list)
}

#' @title Kaplan-Meier Test
#' @description Runs Kaplan-Meier statistical tests for the given group values.
#' Supported statistical test types are 'log-rank' and 'wilcoxon'. Test type has to be specified as an
#' input parameter of the fitting function: \code{\link{r4ml.kaplan.meier}}.
#' @param object (r4ml.kaplan.meier):  Survival model from Kaplan Meier analysis.  
#' @return Two r4ml.matrix objects: the first one contains contains the p-values of the Chi-square test and
#' the second one has the expected observed values (O-E)^2/E and (O-E)^2/V.
#' 
#' @examples \dontrun{
#' 
#' # Load a sample data set
#' surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),
#'               Race=c(1,0,0,2,6,2,0,0,0), Origin=c(2,0,0,15,0,2,0,0,0),
#'               Age=c(50,52,50,52,52,50,20,50,52))
#' 
#' survmat <- as.r4ml.matrix(surv)
#'   
#' # Create survival formula
#' survformula <- Surv(Timestamp, Censor) ~ Age
#' 
#' # Create survival model
#' km <- r4ml.kaplan.meier(survformula, data=survmat, test.type = "log-rank", 
#'                             directory = "/tmp")
#' 
#' # Show summary
#' summary(km)
#' 
##' # Show test results
##' r4ml.kaplan.meier.test(km)
#'}
#'
#' @export
#' @seealso \link{summary.r4ml.kaplan.meier}
r4ml.kaplan.meier.test <- function(object) {
  logSource <- "r4ml.kaplan.meier.test"

  if (length(object@dmlArgs$`$ttype`) == 0 || object@dmlArgs$`$ttype` == "none") {
    r4ml.err(logSource, "KM object does not incude a test")
  }
  
  # colnames -> N Observed Expected (O-E)^2/E (O-E)^2/V
  km_test_colnames <- c("N", "Observed", "Expected", "(O-E)^2/E", "(O-E)^2/V")
  km_test_chsqr_colnames <- c("n","degree of freedom", "chsqr", "p-value")

  # generating header for tests
  test_colnames <- c()

  if (!is.na(object@groupNames) && length(object@groupNames) > 1) {
    test_colnames <- c(test_colnames, object@groupNames)
  } else {
    r4ml.err(logSource, "Hypothesis testing requires at least two groups")
  }
  if (!is.na(object@strataNames) && length(object@strataNames) > 0) {
    test_colnames <- c(test_colnames, object@strataNames)
  }

  km_test_colnames <- c(test_colnames, km_test_colnames)
  ### end generating header
  km_test_full <- as.r4ml.frame(object@dmlOuts$sysml.execute$TEST)
  SparkR::colnames(km_test_full) <- km_test_colnames

  km_test_chsqr <- as.r4ml.frame(object@dmlOuts$sysml.execute$TEST_GROUPS_OE)
  SparkR::colnames(km_test_chsqr) <- km_test_chsqr_colnames

  #chisqr <- "Chisq=" %++% km_test_chsqr[,3] %++% " on " %++% as.character(km_test_chsqr[,2]) %++% " degrees of freedom, p= " %++% as.character(km_test_chsqr[,4])
  #  Chisq= 4.4  on 5 degrees of freedom, p= 0.499
  output <- c()
  output$km_test_full <- km_test_full
  output$km_test_chsqr <- km_test_chsqr
  return(output)
}
