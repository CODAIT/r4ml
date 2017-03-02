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
# This class represents a Kaplan-Meier model
#' @include ml.model.base.R
#' @include zzz.R
setClass("hydrar.kaplan.meier",
         representation(
           conf.int = "numeric",
           summary = "list",
           median = "hydrar.matrix",
           testPath = "character",
           dataColNames = "vector",
           groupNames = "vector",
           strataNames = "vector"
         ), contains = "hydrar.model"
)
#' @name hydrar.kaplan.meier
#' @title Kaplan-Meier Analysis
#' @export
#' @description Fits a Kaplan-Meier model from a hydrar.matrix
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
#' @param data (hydrar.matrix) Input dataset
#' @param formula (character) describes data columns used for survival analysis, e.g., Surv(Timestamp, Censor) ~ Age.
#'              The left side of the formula indicates the timestamp and censor columns, 
#'              while the right side indicates the grouping column(s) if any.
#' @param strata (list) A list of stratum columns
#' @param conf.int (numeric) Regularization parameter
#' @param conf.type (character) Confidence type: accepted values are 'plain', 'log' (the default), or 'log-log' 
#' @param type (character) Parameter to specify the error type according to "greenwood" (the default) or "peto"
#' @param rho (character) Test type: accepted values are 'none', 'log-rank', or 'wilcoxon'
#' @param test (numeric) Indicates whether the tests should be calculated (test=1) or not (test=0)
#' @param directory (character) The path to save the Kaplan-Meier model if input data is specified. 
#' @return An S4 object of class \code{hydrar.kaplan.meier} which contains the arguments above as well as the following additional fields:
#' 
#'  \tabular{rlll}{
##'  \tab\code{conf.int}   \tab (numeric) \tab The level of two-sided confidence interval. The default value is 0.95.\cr
##'  \tab\code{summary}  \tab (character) \tab kaplan-meier summary data\cr
##'  \tab\code{testPath}    \tab (character) \tab location where Kaplan-Meier tests are stored \cr
##'  \tab\code{groupNames}  \tab (vector) \tab A list of column names which are used in grouping \cr
##'  \tab\code{strataNames} \tab (vector) \tab A list of column names which are used as strata \cr
##'  \tab\code{dataColNames} \tab (vector) \tab The column names of original survival dataset \cr
##'  \tab\code{modelPath}    \tab (character) \tab location where the \code{hydrar.transform()} metadata are stored \cr
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
#' survmat <- as.hydrar.matrix(surv)
#'   
#' # Create survival formula
#' survformula <- Surv(Timestamp, Censor) ~ Age
#'
#' # Create survival model
#' km <- hydrar.kaplan.meier(survformula, data=survmat, test=1, rho="log-rank", 
#'                             directory = "/tmp")
#' 
#' # Show summary
#' summary(km)
#' 
#' # Show test results
#' hydrar.kaplan.meier.test(km)
#'}
#'
#' @seealso \link{summary.hydrar.kaplan.meier}
#' @seealso \link{hydrar.kaplan.meier.test}
#'
hydrar.kaplan.meier <- function(data, formula, strata, conf.int, conf.type, type, rho, test, directory) {
  new("hydrar.kaplan.meier", modelType="other", data=data, 
      directory=directory, formula=formula, strata=strata, conf.int=conf.int, conf.type=conf.type, type=type, rho=rho, test=test)
}

setMethod("hydrar.model.validateTrainingParameters",
          signature = "hydrar.kaplan.meier", definition =  
  function(model, args) {
    logSource <- "hydrar.model.validateTrainingParameters"
    with(args, {
      .hydrar.checkParameter(logSource, formula, expectedClasses="formula")
      .hydrar.checkParameter(logSource, strata, inheritsFrom="list", isOptional=T)
      .hydrar.checkParameter(logSource, conf.int, inheritsFrom=c("integer","numeric"), isOptional=T)
      .hydrar.checkParameter(logSource, conf.type, inheritsFrom="character", isOptional=T)
      .hydrar.checkParameter(logSource, type, inheritsFrom="character", isOptional=T)
      .hydrar.checkParameter(logSource, rho, inheritsFrom="character", isOptional=T)
      .hydrar.checkParameter(logSource, test, inheritsFrom=c("integer","numeric"), isOptional=T)
      
      if (!missing(conf.type) && !(conf.type %in% c("plain","log","log-log"))) {
        hydrar.err(logSource, "Parameter conf.type must be either plain(the default), log or log-log")
      }
      if (!missing(conf.int) && (conf.int < 0 || conf.int > 1)) {
        hydrar.err(logSource, "Parameter conf.int must be a positive integer between 0 and 1.")
      }
      if (!missing(type) && !(type %in% c("greenwood","peto"))) {
        hydrar.err(logSource, "Parameter type must be either greenwood or peto")
      }
      if (!missing(rho) && !(rho %in% c("none","log-rank","wilcoxon"))) {
        hydrar.err(logSource, "Parameter type must be either none (the default), log-rank or wilcoxon")
      }
      if (!missing(test) && test !=0 && test != 1) {
        hydrar.err(logSource, "Parameter test must be either 0 or 1")
      }
    })
    return(model)
  }
)

setMethod("hydrar.model.buildTrainingArgs", signature = "hydrar.kaplan.meier",
          definition = function(model, args) {
    logSource <- "hydrar.model.buildTrainingArgs"
    with(args,  {
      # Defining output directories
      if (missing(directory)){
        directory <- hydrar.env$WORKSPACE_ROOT("hydrar.ml.model.kaplan.meier")
      }

      TESTS <- directory %++% hydrar.env$DML_KM_TESTS
      
      #adding column names to the model
      model@dataColNames <- SparkR::colnames(data)

      dmlPath <- file.path(hydrar.env$SYSML_ALGO_ROOT(), hydrar.env$DML_KM_SCRIPT)
      # @TODO - Find a way to get the timestamps index to the DML script automatically
      dmlArgs <- list(X = data, dml = dmlPath, "M", "KM", fmt = 'CSV') 
      groupIds <- list()
      
      # time_status and groupid paths
      survTSGList <- hydrar.parseSurvivalArgsFromFormulaTree(formula,data,directory)
      timeAndStatusIds <- survTSGList[[1]]
      timeAndStatusIdsFrame <- as.hydrar.frame(data.frame(timeAndStatusIds),
                                               repartition = FALSE)

      survTSMatrix <- as.hydrar.matrix(timeAndStatusIdsFrame)
      dmlArgs <- c(dmlArgs, TE=survTSMatrix)
      if (length(survTSGList) > 1) {                        
        groupIds <- survTSGList[[2]]
        if (length(groupIds) > 0){
          survGFrame <- as.hydrar.frame(data.frame(groupIds),
                                        repartition = FALSE)
          survGMatrix <- as.hydrar.matrix(survGFrame)
          
          dmlArgs <- c(dmlArgs, GI=survGMatrix)
          model@groupNames <- model@dataColNames[groupIds]
        }
      }
      
      # strata-id paths
      if (!missing(strata)) {
        if (length(strata) > 0) {
          strataIds<- hydrar.extractColsFromFormulaTree(strata,data,delimiter=",")
          strataIds <- unlist(strataIds)
          
          # check if there is an intersection beteen groups and stratas
          # if there is an intersection send an error about it
          if (length(groupIds) > 0){
            intersection <- intersect(groupIds, strataIds)
            if (length(intersection) > 0){
              intersectionstr <- paste(intersection, collapse=',')
              hydrar.err(logSource, paste("Grouping columns must not overlap with strata columns. Overlapping column ids are: ", intersectionstr))
            }
          }
          
          survStrataFrame <- as.hydrar.frame(strataIds, repartition = FALSE)
          #TODO see if it is possible to avoid the creation of matrix
          strataIdsMatrix <- as.hydrar.matrix(survStrataFrame)   
          dmlArgs <- c(dmlArgs, SI=strataIdsMatrix)
          #adding strata-id path to the model
          model@strataNames <- model@dataColNames[strataIds]
          for (i in 1:length(model@strataNames)) {
            model@strataNames[i] <- "strata(" %++% model@strataNames[i] %++% ")"
          }
        }
      }
      
      if (!missing(test) && test == 1) {
        dmlArgs <- c(dmlArgs, T=TESTS)
        model@testPath <- TESTS
        if (!missing(rho)) {
          dmlArgs <- c(dmlArgs, ttype=rho)
        }else{
          dmlArgs <- c(dmlArgs, ttype="log-rank")
        }
      }     
      
      if (!missing(conf.int)) {
        model@conf.int <- conf.int
      } else {
        model@conf.int <- 0.95 # default value
      }
      dmlArgs <- c(dmlArgs, alpha = (1 - model@conf.int))
      
      if (!missing(type)) {
        dmlArgs <- c(dmlArgs, etype=type)
      }
      if (!missing(conf.type)) {
        dmlArgs <- c(dmlArgs, ctype=conf.type)
      }
      model@dmlArgs <- dmlArgs
      return(model)
    })
  }                  
)

setMethod("hydrar.model.postTraining", signature = "hydrar.kaplan.meier",
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
    median <- as.hydrar.matrix(median)

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

setMethod(f = "show", signature = "hydrar.kaplan.meier", definition =
  function(object) {
    logSource <- "show.hydrar.kaplan.meier"
    callNextMethod()

    cat("Survival Median\n")

    median <- object@median

    SparkR::head(median, num = hydrar.env$DEFAULT_HEAD_ROWS)
  }
)


#' @title Kaplan-Meier Summary
#' @description Computes different survival estimates for a given hydrar.kaplan.meier model. 
#' @param object An S4 object of class \code{hydrar.kaplan.meier}
#' @return A list of hydrar.matrix objects with seven columns and as many rows as the input dataset. There
#' will be one hydrar.matrix per group/strata combination. Each matrix will have the following structure:
#' timestamp, number at risk, number of events, Kaplan-Meier estimate of survival function, 
#' standard error of survival, as well as 100*(1-alpha)\% upper and lower 
#' confidence interval bounds of the median.
#' 
#' @usage summary(object)
#' @examples \dontrun{
#' 
#' # Load a sample data set
#' surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),
#'               Race=c(1,0,0,2,6,2,0,0,0), Origin=c(2,0,0,15,0,2,0,0,0),
#'               Age=c(50,52,50,52,52,50,20,50,52))
#' 
#' survmat <- as.hydrar.matrix(surv) 
#'   
#' # Create survival formula
#' survformula <- Surv(Timestamp, Censor) ~ Age
#' 
#' # Create survival model
#' km <- hydrar.kaplan.meier(survformula, data=survmat, test=1, rho="log-rank", 
#'                             directory = "/tmp")
#' 
#' # Show summary
#' summary(km)
#' 
#' # Show test results
#' hydrar.kaplan.meier.test(km)
#'}
#'
#' @export summary.hydrar.kaplan.meier
#' @seealso \link{hydrar.kaplan.meier}
#' @seealso \link{hydrar.kaplan.meier.test}
summary.hydrar.kaplan.meier <- function(object) {
  logSource <- "hydrar.kaplan.meier"

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
#ALOK ' @export
#' Supported statistical test types are 'log-rank' and 'wilcoxon'. Test type has to be specified as an
#' input parameter of the fitting function: \code{\link{hydrar.kaplan.meier}}.
#' @param object  An S4 object of class \code{hydrar.kaplan.meier}
#' @return Two hydrar.matrix objects: the first one contains contains the p-values of the Chi-square test and
#' the second one has the expected observed values (O-E)^2/E and (O-E)^2/V.
#' 
#' @examples \dontrun{
#' 
#' # Load a sample data set
#' surv <- data.frame(Timestamp=c(1,2,3,4,5,6,7,8,9), Censor=c(1,0,0,1,1,0,0,1,0),
#'               Race=c(1,0,0,2,6,2,0,0,0), Origin=c(2,0,0,15,0,2,0,0,0),
#'               Age=c(50,52,50,52,52,50,20,50,52))
#' 
#' survmat <- as.hydrar.matrix(surv)
#'   
#' # Create survival formula
#' survformula <- Surv(Timestamp, Censor) ~ Age
#' 
#' # Create survival model
#' km <- hydrar.kaplan.meier(survformula, data=survmat, test=1, rho="log-rank", 
#'                             directory = "/tmp")
#' 
#' # Show summary
#' summary(km)
#' 
#' # Show test results
#' hydrar.kaplan.meier.test(km)
#'}
#'
#' @export
#' @seealso \link{summary.hydrar.kaplan.meier}
#' @seealso \link{hydrar.kaplan.meier.test}
hydrar.kaplan.meier.test <- function(object) {
  logSource <- "hydrar.kaplan.meier.test"

  # colnames -> N Observed Expected (O-E)^2/E (O-E)^2/V
  km_test_colnames <- c("N", "Observed", "Expected", "(O-E)^2/E", "(O-E)^2/V")
  km_test_chsqr_colnames <- c("n","degree of freedom", "chsqr", "p-value")

  # generating header for tests
  test_colnames <- c()
  dataColNames <- object@dataColNames

  if (!is.na(object@groupNames) && length(object@groupNames) > 0) {
    test_colnames <- c(test_colnames, object@groupNames)
  }else{
    hydrar.err(logSource, "Tests should specify at least one group")
  }
  if (!is.na(object@strataNames) && length(object@strataNames) > 0) {
    test_colnames <- c(test_colnames, object@strataNames)
  }

  km_test_colnames <- c(test_colnames, km_test_colnames)
  ### end generating header
  km_test_full <- as.hydrar.frame(hydrar.read.csv(file.path(object@testPath),
                                                  header=FALSE),
                                  repartition = FALSE)
  SparkR::colnames(km_test_full) <- km_test_colnames

  km_test_chsqr_path <- object@testPath %++% hydrar.env$DML_KM_TESTS_GRPS_OE_SUFFIX
  km_test_chsqr <- as.hydrar.frame(hydrar.read.csv(file.path(km_test_chsqr_path),
                                                   header=FALSE),
                                   repartition = FALSE)
  SparkR::colnames(km_test_chsqr) <- km_test_chsqr_colnames

  #chisqr <- "Chisq=" %++% km_test_chsqr[,3] %++% " on " %++% as.character(km_test_chsqr[,2]) %++% " degrees of freedom, p= " %++% as.character(km_test_chsqr[,4])
  #  Chisq= 4.4  on 5 degrees of freedom, p= 0.499
  return (list(km_test_full,km_test_chsqr))
}
