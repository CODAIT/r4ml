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
#' @include zzz.R
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
             labelColumnName="character",
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
#' @examples \dontrun{
#' 
#'  # Load Dataset
#'  df <- iris
# @TODO Replace with transform once it is avaliable
#'  df$Species <- (as.numeric(df$Species))
#'  iris_df <- as.hydrar.frame(df)
#'  iris_mat <- as.hydrar.matrix(iris_df)
#'  
#'  Split data in to 80% train and 20% test
#'  s <- hydrar.sample(iris_mat, perc=c(0.2,0.8))
#'  test <- s[[1]]
#'  train <- s[[2]]
#'  ml.coltypes(train) <- c("scale", "scale", "scale", "scale", "nominal") 
#'  ml.coltypes(test) <- c("scale", "scale", "scale", "scale", "nominal") 
#'  
#'  # Build a Logistic Regression Classifier
#'  iris_log_reg <- hydrar.mlogit(Species ~ . , data = train, labelNames=c("Setosa","Versicolor","Virginica")) 
#'  
#'  #Configure the test data
#'  test = as.hydrar.matrix(as.hydrar.frame(test[,c(1:4)]))
#'
#'  # Compute probabilities for the testing set
#'  output <- predict.hydrar.mlogit(iris_log_reg, test)
#'
#' }
#'

hydrar.mlogit <- function(formula, data, intercept = FALSE, shiftAndRescale = FALSE, tolerance,
                               outer.iter.max, inner.iter.max, lambda, labelNames=character(0)) {
  new("hydrar.mlogit", modelType="classification", formula=formula, data = data, 
    intercept = intercept, shiftAndRescale=shiftAndRescale, tolerance=tolerance,
      outer.iter.max=outer.iter.max, inner.iter.max=inner.iter.max, lambda=lambda,labelNames=labelNames
      )
}

# check the training parameters of the model
setMethod("hydrar.model.validateTrainingParameters", signature = "hydrar.mlogit", def =
  function(model, args) {
    logSource <- "hydrar.model.validateTrainingParameters"
    with(args, {
      # Convert labelNames into a transform object later
      .hydrar.checkParameter(logSource, intercept, "logical", c(TRUE, FALSE))
      .hydrar.checkParameter(logSource, shiftAndRescale, "logical", c(TRUE, FALSE))
      .hydrar.checkParameter(logSource, tolerance, "numeric", isOptional = TRUE)
      .hydrar.checkParameter(logSource, outer.iter.max, "numeric", isOptional = TRUE)
      .hydrar.checkParameter(logSource, inner.iter.max, "numeric", isOptional = TRUE)
      .hydrar.checkParameter(logSource, lambda, "numeric", isOptional = TRUE)
      .hydrar.checkParameter(logSource, labelNames, "character", isOptional = TRUE, isSingleton = FALSE)
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


# Organize arguments for the Multinomial Logistic Regression dml script
setMethod("hydrar.model.buildTrainingArgs", signature = "hydrar.mlogit", def =
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
        y = args$Y,
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
    slot(model, "labelColumnName") = model@yColname
    slot(model, "transformPath") = ""
    # Throw error if length of labels not equal to B_out+1?
    if(length(model@labelNames)!=0){
      slot(model, "labelNames") = model@labelNames
      colnames(model@beta) <- model@labelNames[1 : length(model@labelNames)-1]
    }
    else{
      slot(model, "labelNames") = "class:" %++% (1 : (ncol(model@beta)+1))
      colnames(model@beta) <- model@labelNames[1 : length(model@labelNames)-1]
    }
    rownames(model@beta) <- model@featureNames
    slot(model, "yIdx") <- model@yColId
    slot(model, "modelPath") <- ""
    slot(model, "classes") <- (dim(SparkR::as.data.frame(model@dmlOuts[["B_out"]]))[2]+1)
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
    SparkR::as.data.frame(object@beta)
  }
)

#' @title Predict method for Multinomial Logistic Regression models
#' @name predict.hydrar.mlogit
#' @description This method allows to score/test a Multinomial Logistic Regression model for a given hydrar.matrix. If the testing set is labeled,
#' testing will be done and some statistics will be computed to measure the quality of the model. Otherwise, scoring will be performed
#' and only the probabilities will be computed.
#' @param object (hydrar.mlogit) :
#'  A Multinomial Logistic Regression model built by Hydra R
#' @param data (hydrar.matrix) :
#'  Testing dataset which may or may not be labeled
#' @return If the testing dataset is not labeled, the result will be a list with per-class probabilities for each row. 
#' Otherwise, the result will be a list with (1) a list with per-class probabilities for each row (\code{$probabilities}), and
#' (2) a list with goodness-of-fit statistics ($statistics) for each column. Please refer to \link{predict.hydrar.glm} for the
#' definitions of these statistics.
#' @examples \dontrun{
#' 
#'  # Load Dataset
#'  df <- iris
# @TODO Replace with transform once it is avaliable
#'  df$Species <- (as.numeric(df$Species))
#'  iris_df <- as.hydrar.frame(df)
#'  iris_mat <- as.hydrar.matrix(iris_df)
#'  
#'  Split data in to 80% train and 20% test
#'  s <- hydrar.sample(iris_mat, perc=c(0.2,0.8))
#'  test <- s[[1]]
#'  train <- s[[2]]
#'  ml.coltypes(train) <- c("scale", "scale", "scale", "scale", "nominal") 
#'  ml.coltypes(test) <- c("scale", "scale", "scale", "scale", "nominal") 
#'  
#'  # Build a Logistic Regression Classifier
#'  iris_log_reg <- hydrar.mlogit(Species ~ . , data = train, labelNames=c("Setosa","Versicolor","Virginica")) 
#'  
#'  #Configure the test data
#'  test = as.hydrar.matrix(as.hydrar.frame(test[,c(1:4)]))
#'
#'  # Compute probabilities for the testing set
#'  output <- predict(iris_log_reg, test)
#'
#' }
#' @export
#' @seealso \link{hydrar.mlogit}
predict.hydrar.mlogit <- function(object, data) {
  logSource <- "predict.hydrar.mlogit"
  hydrar.info(logSource, "Predicting labels using given Logistic Regression model on data" %++% data)
  mlogit <- object
  
  .hydrar.checkParameter(logSource, data, inheritsFrom="hydrar.matrix")

  # Establish location to store statistics
  statsPath <- file.path(hydrar.env$WORKSPACE_ROOT("hydrar.mlogit"), "stats_predict.csv")

  # Generate arguments to pass to SystemML script
  args <- list(dml = file.path(hydrar.env$SYSML_ALGO_ROOT(), hydrar.env$DML_GLM_TEST_SCRIPT),
               B_full = as.hydrar.matrix(as.hydrar.frame(coef(object))),
               "means", # this is output $M
               O = statsPath,
               dfam = 3, # dfam = 3 gives us Multinomial in GLM_Predict script.
               fmt = "csv"
  )

  # Check the input test data for a labels/outputs column. If it already exists, score the model predictions against this column.
  # Otherwise, simply generate and return predictions.
  testing <- hydrar.ml.checkModelFeaturesMatchData(coef(object), data, object@intercept, object@labelColumnName, object@yIdx)  

  # Apply different arguments to the DML script depending on if we're scoring a dataset or generating new predictions
  if(testing) {
    XY <- hydrar.model.splitXY(data, object@yColname)        
    testset_x <- XY$X
    testset_y <- XY$Y
    args <- c(args, 
              X = testset_x,
              Y = testset_y)
    args <- c(args, scoring_only = "no")

    dmlOuts <- do.call("sysml.execute", args)  
  } else { #only scoring (no Y is passed)
    args <- c(args, X = data)
    args <- c(args, scoring_only = "yes")
    dmlOuts <- do.call("sysml.execute", args)
  }
  
  # If family is binomial, probabilities will be returned (two columns). Otherwise,
  # predictions themselves will be output
  colnames <- object@yColname
  
  preds <- as.hydrar.matrix(as.hydrar.frame(dmlOuts[['means']]))

  # Output probabilities/predictions accordingly
  output <- list("probabilities" = base::as.data.frame(SparkR::as.data.frame(preds)))
  
  # Add stats
  if (testing) {
    statsCsv <- SparkR::as.data.frame(hydrar.read.csv(statsPath, header = FALSE, stringsAsFactors = FALSE))
    output <- c(output, list("statistics"=statsCsv))
    colnames(output$statistics) <- c("Name", "Y-column", "Scaled", "Value")
  }

  colnames(output$probabilities) <- object@labelNames
  
  return(output)
}
