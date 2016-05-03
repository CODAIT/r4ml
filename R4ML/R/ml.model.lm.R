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


setClass("hydrar.lm",
         representation(coefficients = "data.frame",
                        intercept = "logical",
                        shiftAndRescale = "logical",
                        transformPath = "character",
                        method="character"),
         contains = "hydrar.model"
)

#' @name hydrar.lm
#' @title Linear Regression
#' @export
#' @description Fits a linear regression model from a hydrar.matrix or
#'    loads an existing model from HDFS.
#' @details There are two different implementations to estimate the coefficients
#'  of the Linear Regression. Depending on the size and sparsity of the training
#'  set, one or the other method may be more efficient.
#'
#'  The \emph{direct-solve} method performs better when the number of features
#' \tabular{rlll}{
#'  \tab 1 \tab is relatively small (i.e., less than 1000) and \cr
#'  \tab 2 \tab the training set is either \cr
#'    \tab \tab ---- tall (i.e., number of rows >> number of columns) or \cr
#'    \tab \tab ---- fairly dense (i.e., the proportion of zero/missing values is very small). \cr
#'  }
#'  Otherwise,
#'  the \emph{conjugate gradient} method would be preferred. If the number of columns
#'  is above 50,000, \emph{conjugate gradient} should be used.
#' @param formula (formula) A formula in the form Y ~ ., where Y is the response variable.
#'                The response variable must be of type "scale".
#' @param data (hydrar.matrix) A hydrar.matrix to be fitted.
#' @param method (character) Either "direct-solve" or "iterative". Default is "direct-solve".
#' @param intercept (logical) Boolean value indicating if the intercept term should be used for the regression.
#' @param shiftAndRescale (logical) Boolean value indicating if shifting and rescaling X columns to mean = 0, variance = 1 should be performed.
#' @param tolerance (numeric) Epsilon degree of tolerance, used when method is "iterative".
#' @param iter.max (numeric) Number of iterations, used when method is "iterative".
#' @param lambda (numeric) Regularization parameter.
# @TODO remove it
#' @param directory (character) The HDFS path to save the Linear Regression model
#'  if \code{formula} and \code{data} are specified. Otherwise, an HDFS location
#'  with a previously trained model to be loaded.
#' @return An S4 object of class \code{hydrar.lm} which contains the arguments above as well
#' as the following additional fields:
#'  \tabular{rlll}{
##'\tab\code{coefficients}  \tab (numeric)   \tab Coefficients of the regression\cr
##'\tab\code{modelPath}     \tab (character) \tab HDFS location where the model files are stored\cr
##'\tab\code{transformPath} \tab (character) \tab HDFS location where the \code{hydrar.transform()}
##'                                               metadata are stored \cr
##'\tab\code{yIdx}          \tab (numeric)   \tab Column id of the response variable\cr
##'\tab\code{numFeatures}   \tab (numeric)   \tab The number of attributes\cr
##'\tab\code{labelColname}  \tab (character) \tab Column name of the response variable \cr
##'\tab\code{call}          \tab (character) \tab String representation of this method's call,
##'                                               including the parameters and values passed to it.\cr
##'
##' }
#'
#' @examples \dontrun{
#'
#'
#'
#' # Project some relevant columns for modeling / statistical analysis
#' airlineFiltered <- air[, c("Month", "DayofMonth", "DayOfWeek", "CRSDepTime",
#'                                 "Distance", "ArrDelay")]
#'
#' # Apply required transformations for Machine Learning
#' hydrar transform is not implemented yet
#' outData <- "/user/hydrar/examples/lm/airline.mtx"
#' airlineMatrix <- hydrar.transform(airlineFiltered,
#'                    outData=outData,
#'                    dummycodeAttrs=c("DayOfWeek"),
#'                    recodeAttrs=c("DayOfWeek"),
#'                    missingAttrs=c("Distance", "ArrDelay"),
#'                    imputationMethod=c("global_mode"),
#'                    transformPath="/user/hydrar/examples/lm/airline.transform")
#'
#' # Split the data into 70% for training and 30% for testing
#' samples <- hydrar.sample(airlineMatrix, perc=c(0.7, 0.3))
#' train <- samples[[1]]
#' test <- samples[[2]]
#'
#' # Create a linear regression model
#' lm <- hydrar.lm(ArrDelay ~ ., data=train, directory="/user/hydrar/examples/lm/lm.model")
#'
#' # Get the coefficients of the regression
#' coef(lm)
#'
#' # Calculate predictions for the testing set
#' pred <- predict(lm, test, "/user/hydrar/examples/lm/lm.preds")
#' }
#'
#' @seealso {predict.hydrar.lm} #NOTE add link after predict func
hydrar.lm <- function(formula, data, method = "direct-solve", intercept=F, shiftAndRescale=F,
                      tolerance, iter.max, lambda, directory) {
  new("hydrar.lm", modelType="regression",
      formula=formula, data=data, method=method, intercept=intercept, shiftAndRescale=shiftAndRescale,
      tolerance=tolerance, iter.max=iter.max, lambda=lambda)

}

# overloaded method which checks the training parameters of the linear model
setMethod("hydrar.model.validateTrainingParameters", signature="hydrar.lm", def =
  function(model, args) {
    logSource <- "hydrar.model.validateTrainingParameters"
    with(args, {
      .hydrar.checkParameter(logSource, method, "character", c("direct-solve", "iterative"))
      .hydrar.checkParameter(logSource, intercept, "logical", c(TRUE, FALSE))
      .hydrar.checkParameter(logSource, shiftAndRescale, "logical", c(TRUE, FALSE))
      .hydrar.checkParameter(logSource, tolerance, "numeric", isOptional = T)
      .hydrar.checkParameter(logSource, iter.max, "numeric", isOptional=T)
      .hydrar.checkParameter(logSource, lambda, "numeric", isOptional=T)
      if (!missing(iter.max) && (iter.max < 0)) {
        hydrar.err(logSource, "Parameter iter.max must be a natural number.")
      }
      if (!missing(tolerance) && (tolerance <= 0)) {
        hydrar.err(logSource, "Parameter tolerance must be a positive number.")
      }
      if (!missing(lambda) && (lambda < 0)) {
        hydrar.err(logSource, "Parameter lambda must be a non-negative number.")
      }
      if (!intercept & shiftAndRescale) {
        hydrar.err(logSource, "The shiftAndRescale should be FALSE when intercept is FALSE.")
      }
      if ((method == "iterative") && !missing(iter.max)) {
        if ((iter.max < 1) | (round(iter.max) != iter.max) | is.infinite(iter.max)) {
          hydrar.err(logSource, "The iter.max argument has an invalid value.")
        }
      }
      return(model)
    })
  }
)

# Overwrite the base model's method to build the traning args which will be
# passed to the dml script to run
setMethod("hydrar.model.buildTrainingArgs", signature="hydrar.lm", def =
  function(model, args) {
    with(args,  {
      model@method <- method
      model@intercept <- intercept
      model@shiftAndRescale <- shiftAndRescale

      if (args$intercept == TRUE) {
        model@featureNames <- c("(intercept)", model@featureNames)
      }

      #coefPath <- model@modelPath %++% "/coefficients.csv"
      #statsPath <- model@modelPath %++% "/stats.csv"
      workspace <- hydrar.env$WORKSPACE_ROOT("hydrar.lm")
      statsPath <- file.path(workspace, "stats.csv")
      dmlPath <- file.path(hydrar.env$SYSML_ALGO_ROOT(),
                   ifelse(args$method=="direct-solve",
                     hydrar.env$DML_LM_DS_SCRIPT, hydrar.env$DML_LM_CG_SCRIPT))
      # invoke DML script
      dmlArgs <- list(
        dml = dmlPath,
        X = args$X,
        y = args$Y,
        icpt = ifelse(!args$intercept, 0, ifelse(!args$shiftAndRescale, 1, 2)),
        "beta_out", # output from DML script
        O = statsPath,
        fmt = "csv")
      if (!missing(lambda)) {
        dmlArgs <- c(dmlArgs, reg = args$lambda)
      }
      if (args$method == "iterative") {
        if (!missing(tolerance)) {
          dmlArgs <- c(dmlArgs, tol = args$tolerance)
        }
        if (!missing(iter.max)) {
          dmlArgs <- c(dmlArgs, maxi = args$iter.max)
        }
      }
      #DEBUG browser()
      model@dmlArgs <- dmlArgs
      return(model)
    })
  }
)

# overwrite the base model's post training function so that one can
# post process the final outputs from the dml scripts
setMethod("hydrar.model.postTraining", signature="hydrar.lm", def =
  function(model) {
    outputs <- model@dmlOuts$sysml.execute
    #DEBUG browser()
    #stats calculation
    statsPath <- model@dmlArgs$O
    statsCsv <- as.data.frame(read.csv(statsPath, header=FALSE, stringsAsFactors=FALSE))
    stats <- statsCsv[, 2, drop=F]
    row.names(stats) <- statsCsv[, 1]
    colnames(stats) <- "value"
    model@dmlOuts$stats = stats

    outNames <- names(outputs)
    i <- 1
    for (output in outputs) {
      outName <- outNames[i]
      i <- i + 1
      model@dmlOuts[outName] <- output
    }

    model@coefficients <- coef(model)
    return(model)
  }
)

#' @title print linear model fitted coefficients
#' @description Pritn the coefficients of the fitted model
#' @name show
#' @param object (hydrar.lm) The linear regression model
#' @return None
#' @seealso \link{coef}
setMethod(f = "show", signature = "hydrar.lm", definition =
  function(object) {
    logSource <- "hydrar.lm"
      callNextMethod()
      cat("\n\nCoefficients: \n")
      coeff <- coef(object)
      print(coeff)
  }
)

#' @title linear model fitted coefficients
#' @description Get the coefficients of the fitted model
#' @name coef
#' @param object (hydrar.lm) The linear regression model
#' @return A data.frame with the coefficient for the learned model
#' @seealso \link{show}
setMethod("coef", signature="hydrar.lm", def =
  function(object) {
    SparkR:::as.data.frame(object@dmlOuts[['beta_out']])
  }
)

setGeneric("stats", function(object) attributes(object))
setGeneric("stats")

#' @title Get the statistics data of a learned model
#' @description With this method we can get the statistics of a learned model
#'              represented by the object
#' @name stats
#' @param object (hydrar.lm) The linear regression model
#' @return A data.frame with the statistics data for the learned model
setMethod("stats", signature="hydrar.lm", def =
  function(object) {
    return(object@dmlOuts$stats)
  }
)
