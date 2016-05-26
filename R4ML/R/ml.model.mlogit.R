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
#' @include ml.model.base.R

setClass("hydrar.mlogit",
         slots = c(
             beta = "data.frame",
             modelPath = "character", 
             classes = "numeric",
             yIdx = "numeric",
             yColName = "character",
             labelNames = "character",
             intercept = "logical",
             shiftAndRescale = "logical",
             transformPath = "character",
             call="character"
         ),
         contains = "hydrar.model"
)

#' @name hydrar.mlogit
#' @title Multinomial Logistic Regression
#' @export
#' @description Fits a logistic regression model from a hydrar.matrix
#' @details The largest label represents the baseline category; if label -1 or 0 is present, then it is the baseline label.
#' @details The classes need to be specified as 1,2...K where K is equal to the overall number of classes.
#'
#' @param formula (formula) A formula in the form Y ~ ., where Y is the response variable.
#' @param data (hydrar.matrix) A hydrar.matrix to be fitted.
#' @param intercept (logical) Logical value for whether the intercept should be used.
#' @param shiftAndRescale (logical) Logical value indicating if the data should be normalized to zero mean, variance/standard deviation = 1.
#' @param lambda (numeric) Regularization parameter.
#' @param tolerance (numeric) Epsilon degree of tolerance, criterion for convergence.
#' @param outer.iter.max (numeric) maximum number of outer (Newton) iterations
#' @param inner.iter.max (numeric) maximum number of inner (conjugate gradient) iterations, 0 = no max
#'
#'
#' @return An S4 object of class \code{hydrar.mlogit} which contains the arguments above as well
#' as the following additional fields:
#'  \tabular{rlll}{
##'\tab\code{beta}          \tab (numeric) \tab Coefficients of the regression\cr
##'\tab\code{modelPath}     \tab (character) \tab HDFS location where the model files are stored\cr
##'\tab\code{transformPath} \tab (character)   \tab HDFS location where the \code{bigr.transform()} metadata are stored \cr
##'\tab\code{yIdx}          \tab (numeric) \tab Column id of the response variable\cr
##'\tab\code{labelColname}      \tab (character) \tab Column name of the response variable \cr
##'\tab\code{call}          \tab (character) \tab String representation of this method's call, including the parameters and values passed to it.\cr
##'}
#' 
# @TODO update algorithm after SystemML labels bug is fixed
# @TODO update example after GLM Predict is added with train/test split + predict
#' @examples \dontrun{
#' 
#' require(SparkR)
#' require(HydraR)
#' df <- iris
#' df$Species <- (as.numeric(df$Species))
#' iris_df <- as.hydrar.frame(df)
#' iris_mat <- as.hydrar.matrix(iris_df)
#' ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
#' iris_log_reg <- hydrar.mlogit(Species ~ . , data = iris_mat)
#' 
#' show(iris_log_reg)
#' coef(iris_log_reg)
#'
#' }
#'

hydrar.mlogit <- function(formula, data, intercept=F, shiftAndRescale=F, tolerance,
                               outer.iter.max, inner.iter.max, lambda, labelNames=character(0)) {
  new("hydrar.mlogit", modelType="classification", formula=formula, data = data, 
    intercept = intercept, shiftAndRescale=shiftAndRescale, tolerance=tolerance,
      outer.iter.max=outer.iter.max, inner.iter.max=inner.iter.max, lambda=lambda,labelNames=labelNames
      )
}

# check the training parameters of the model
setMethod("hydrar.model.validateTrainingParameters", signature="hydrar.mlogit", def =
  function(model, args) {
    logSource <- "hydrar.model.validateTrainingParameters"
    with(args, {
      # Convert labelNames into a transform object later
      .hydrar.checkParameter(logSource, intercept, "logical", c(TRUE, FALSE))
      .hydrar.checkParameter(logSource, shiftAndRescale, "logical", c(TRUE, FALSE))
      .hydrar.checkParameter(logSource, tolerance, "numeric", isOptional = T)
      .hydrar.checkParameter(logSource, outer.iter.max, "numeric", isOptional=T)
      .hydrar.checkParameter(logSource, inner.iter.max, "numeric", isOptional=T)
      .hydrar.checkParameter(logSource, lambda, "numeric", isOptional=T)
      .hydrar.checkParameter(logSource, labelNames, "character", isOptional=T, isSingleton=F)
      if (missing(data)) {
        hydrar.err(logSource, "Must provide data.")
      }
      if (!missing(inner.iter.max) && (inner.iter.max < 0)) {
        hydrar.err(logSource, "Parameter inner.iter.max must be a natural number.")
      }
      if (!missing(outer.iter.max) && (outer.iter.max < 0)) {
        hydrar.err(logSource, "Parameter outer.iter.max must be a natural number.")
      }
      if (!missing(lambda) && (lambda < 0)) {
        hydrar.err(logSource, "Parameter lambda must be a non-negative number.")
      }
      if (!intercept & shiftAndRescale) {
        hydrar.err(logSource, "The shiftAndRescale should be FALSE when intercept is FALSE.")
      }
      return(model)
    })
  }
)

# Once predict is added, return hashtable as well to re-map the results
recode <-function(x){
  env <- new.env()
  uq <- (unique(x))
  i <- 1
  for(u in (unique(x))){
    env[[as.character(u)]] = i
    i <- i + 1
  }
  as.hydrar.matrix(
    as.hydrar.frame(
      as.data.frame(
        sapply((x), function(y) env[[as.character(y)]]))))
}

# Organize arguments for the Multinomial Logistic Regression dml script
setMethod("hydrar.model.buildTrainingArgs", signature="hydrar.mlogit", def =
  function(model, args) {
    with(args, {
      model@labelNames <- labelNames
      model@intercept <- intercept
      model@shiftAndRescale <- shiftAndRescale
      
      if (args$intercept == TRUE) {
        model@featureNames <- c("(intercept)", model @featureNames)
      }
      
      dmlPath <- file.path(hydrar.env$SYSML_ALGO_ROOT(), hydrar.env$DML_MULTI_LOGISTIC_REGRESSION_SCRIPT)
      
      # invoke DML script
      dmlArgs <- list(
        dml = dmlPath,
        X = args$X,
        # Update to be scalable with transform
        y = recode(collect(args$Y)[[1]]),
        icpt = ifelse(!args$intercept, 0, ifelse(!args$shiftAndRescale, 1, 2)),
        "B_out", # Output from DML script
        fmt = "csv")
      if (!missing(lambda)) {
        dmlArgs <- c(dmlArgs, reg = args$lambda)
      }
      if (!missing(outer.iter.max)) {
        dmlArgs <- c(dmlArgs, moi = args$outer.iter.max)
      }
      if (!missing(inner.iter.max)) {
        dmlArgs <- c(dmlArgs, mii = args$inner.iter.max)
      }
      model@dmlArgs <- dmlArgs
      return (model)
    })
  }
)

# Set up display for output
setMethod("hydrar.model.postTraining", signature = "hydrar.mlogit", def =
  function (model) {
    outputs <- model@dmlOuts$sysml.execute
    outNames <- names(outputs)
    i <- 1
    for (output in outputs) {
      outName <- outNames[i]
      model@dmlOuts[outName] <- output
      i <- i + 1
    }
    slot(model, "beta") = SparkR::as.data.frame(model@dmlOuts[["B_out"]])
    slot(model, "yColName") = model@yColname
    slot(model, "transformPath") = ""
    # Throw error if length of labels not equal to B_out+1?
    if(length(model@labelNames)!=0){
      slot(model, "labelNames") = model@labelNames
      colnames(model@beta) <- model@labelNames[1 : length(model@labelNames)-1]
    }
    else{
      slot(model, "labelNames") = "Class: " %++% (0 : ncol(model@beta))
      colnames(model@beta) <- model@labelNames[1 : length(model@labelNames)-1]
    }
    slot(model, "yIdx") = model@yColId
    slot(model, "modelPath") = ""
    slot(model, "classes") = (dim(SparkR::as.data.frame(model@dmlOuts[["B_out"]]))[2]+1)
    return(model)
  }
)

# look at options for output display after by looking at output matrix, likely applying 'predict' which 
# doesn't exist as yet

setMethod(f = "show", signature = "hydrar.mlogit", definition =
  function(object) {
    logSource <- "hydrar.lm"
    callNextMethod()
    cat("\n\nCoefficients: \n")
    coeff <- coef(object)
    print(coeff)
  }
)

setMethod("coef", signature="hydrar.mlogit", def =
  function(object) {
    SparkR:::as.data.frame(object@beta)
  }
)


