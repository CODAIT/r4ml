# Class hydrar.svm
#
# This class represents a learned SVM model for classification.

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
# Class hydrar.svm
#
# This class represents a learned SVM model for classification.


setClass("hydrar.svm",
  slots = c(
    coefficients = "data.frame",
    modelPath = "character",
    classes = "numeric",
    yIdx = "numeric",
    labelColname = "character",
    labelNames = "character",
    intercept = "logical",
    transformPath = "character",
    l2svm = "logical",
    call = "character",
    dmlOuts = "list"
  )
)
#' Return the coefficients of a machine learning model built by HydraR.
#' 
#' @name coef
#' @title Extract model coefficients
#' @param object (hydrar.svm, hydrar.lm, hydrar.glm, hydrar.mlogit) A model built 
#'        by HydraR
#' @export        
#' @return A data.frame with the model coefficients
setGeneric("coef")
setMethod("coef", "hydrar.svm", function(object) {
  return(object@coefficients)
})

setMethod("initialize", "hydrar.svm",
  function(.Object,
    modelPath,
    yIdx,
    intercept,
    transformPath = "",
    labelColname,
    labelNames,
    featureNames,
    is.binary.class,
    l2svm,
    call,
    dmlOuts
  ) {
    #@TODO do we need to read the modelPath from the path or can we ignore  
    classes <- if(is.binary.class) 2 else length(names(dmlOuts$w))
    coltypes <- if(is.binary.class) "numeric" else rep("numeric", classes)
   
    d <- SparkR:::as.data.frame(dmlOuts[['w']])
    dmlOutNames <- names(dmlOuts)
    if (!any(is.na(match(c("extra_model_params", "weights"), dmlOutNames)))) {
      w <- SparkR:::as.data.frame(dmlOuts[['weights']])
      e <- SparkR:::as.data.frame(dmlOuts[['extra_model_params']])
      e <- setNames(e, names(w))
      d <- rbind(w, e)
    }
   
    .Object@dmlOuts <- dmlOuts
    .Object@coefficients <- as.data.frame(d[1:length(featureNames),])
            
    if (!is.null(labelNames)) {
      if (l2svm) {
        .Object@labelNames <- c(labelNames[d[nrow(d)-3,]], labelNames[d[nrow(d)-2,]])
      } else {
        .Object@labelNames <- labelNames
      }
    } else {
      if (l2svm) {
        .Object@labelNames <- "class:" %++% c(d[nrow(d)-3,], d[nrow(d)-2,])
      } else {
        .Object@labelNames <- "class:" %++% 1:classes
      }
    }
            
    if (classes > 2 || !l2svm) {
      if (!is.null(labelNames)) {
        colnames(.Object@coefficients) <- labelNames
      } else {
        colnames(.Object@coefficients) <- "class:" %++% 1:ncol(.Object@coefficients)
      }
    } else {
      colnames(.Object@coefficients) = .Object@labelNames[1]
    }
    rownames(.Object@coefficients) = featureNames
    .Object@labelColname = labelColname
    .Object@classes = classes
    .Object@modelPath = modelPath
    .Object@yIdx = yIdx
    .Object@intercept = intercept
    .Object@transformPath = transformPath
    .Object@call = call
    .Object@l2svm = l2svm
    return(.Object)
  }
)

#' @description Builds a Support Vector Machine model from a hydrar.matrix or
#' Both the binary and multinomial cases are supported in this implementation. 
#' @details This implementation optimizes the primal directly (Chapelle, 2007). It uses
#' nonlinear conjugate gradient descent to minimize the objective function coupled
#' with choosing step-sizes by performing one-dimensional Newton minimization
#' in the direction of the gradient.
#' 
#' @name hydrar.svm
#' @title Support Vector Machine Classifier
#'
#' @param formula (formula) A formula in the form Y ~ ., where Y is the response variable.
#' @param data (hydrar.matrix) A dataset to be fitted.
#' @param intercept (logical) Boolean value specifying whether the intercept term should be part of the model.
#' @param tolerance (numeric) Tolerance value to control the termination of the algorithm.
#' @param iter.max (numeric) Number of iterations.
#' @param lambda (numeric) L2 regularization parameter. lambda must be greater than or equal to 0. A value of 0 is not recommended as it may
#' take a long time to learn the model.
#' @references Olivier Chapelle. Training a Support Vector Machine in the Primal. Neural Computation, 2007.
#' @export
#' @return An S4 object of class \code{hydrar.svm} which contains the arguments above as well as the following additional fields:
#' 
##' \tabular{rlll}{
##'\tab\code{coefficients}          \tab (data.frame) \tab Support vectors\cr
##'\tab\code{modelPath}     \tab (character) \tab HDFS location where the model files are stored\cr
##'\tab\code{transformPath} \tab (character)   \tab HDFS location where the \code{hydrar.transform()} metadata are stored \cr
##'\tab\code{yIdx}          \tab (numeric) \tab Column id of the response variable\cr
##'\tab\code{labelColname}      \tab (character) \tab Column name of the response variable \cr
##'\tab\code{call}          \tab (character) \tab String representation of this method's call, including the parameters and values passed to it.\cr
##'}
#' 
#' @examples \dontrun{
#' 
#' # Load the Iris dataset to HDFS 
#' iris_hf <- as.hydrar.frame(iris)
#' 
#' # Create a hydrar.matrix from the Iris dataset
#' iris_hm <- hydrar.transform(bf = iris_hm)
#' 
#' # Split the data into 70% for training and 30% for testing
#' samples <- hydrar.sample(iris_hm, perc=c(0.7, 0.3))
#' train <- samples[[1]]
#' test <- samples[[2]]
#'                          
#' # Build a Support Vector Machine classifier using the training set
#' svm <- hydrar.svm(Species~., data=train, intercept=TRUE)
#' 
#' # Get the coefficients of the model
#' coef(svm)
#' 
#' # Compute scores for the testing set
#' preds <- predict(svm, test)
#' 
#' preds$scores
#' 
#' # Get the confusion matrix
#' preds$ctable
#' 
#' # Get the overall accuracy
#' preds$accuracy
#' }
#' 
#' @seealso \link{predict.hydrar.svm}
hydrar.svm <- function (
  formula,
  data,                      
  is.binary.class = FALSE,
  intercept = FALSE,
  tolerance,
  iter.max,
  lambda
) {
  
  logSource <- "hydrar.svm"
  if (missing(formula) && missing(data)) {
    hydrar.err(logSource, "Do you mean to read the data from the older model? Currently, Both formula and data arguments must be provided.")
  } else if (missing(formula) || missing(data)) {
    hydrar.err(logSource, "Both formula and data arguments must be provided.")
  }
  
  .hydrar.checkParameter(logSource, formula, "formula")
  .hydrar.checkParameter(logSource, data, inheritsFrom="hydrar.matrix")
  .hydrar.checkParameter(logSource, is.binary.class, "logical")
  .hydrar.checkParameter(logSource, tolerance, c("integer", "numeric"), isOptional=T)
  .hydrar.checkParameter(logSource, iter.max, c("integer", "numeric"), isOptional=T)
  .hydrar.checkParameter(logSource, lambda, c("integer", "numeric"), isOptional=T)
  .hydrar.checkParameter(logSource, intercept, "logical")
  
  directory <- hydrar.env$WORKSPACE_ROOT("hydrar.svm")
  if(!missing(tolerance) && (tolerance <= 0)) {
    hydrar.err(logSource, "Tolerance must be a positive real number")
  }
  
  if(!missing(iter.max) && (iter.max <= 0 || iter.max%%1 != 0)) {
    hydrar.err(logSource, "iter.max must be a positive integer")
  }
  
  if(!missing(lambda) && (lambda < 0)) {
    hydrar.err(logSource, "lambda must be a non negative real number")
  }
  
  if(ncol(data) < 2) {
    hydrar.err(logSource, "Given training dataset must have at least two columns.")
  }
  
  lnames <- NULL
  f = .hydrar.parseFormulaToPlot(formula, data)
  
  # NOTE: this is not a grace way to check whether the right side is just .
  # but it works so far since we only support such formula
  if (formula[[3]] != ".") {
    hydrar.err(logSource, "Right-hand side of the formula only supports '.'")
  }
  
  yIndex <- f[[2]]
  yColName <- f[[3]]
  
 
  responseType <- ml.coltypes(data)[match(yColName, SparkR::colnames(data))]    
  if (responseType != "nominal") {
    hydrar.err(logSource, "Response variable must be nominal for classification algorithms.")
  }
  
  #@TODO add the dummy function and currently it is not there
  #if (hydrar.ml.is.column.dummycoded(data, yIndex)) {
  #  hydrar.err(logSource, "Dummy coded columns are not supported as targets.")
  #}
  
  # level names @ASK do we need it?
  #if(data@transformPath != "") {
  #  if (.hydrar.fileExists(data@transformPath %++% "/Recode/" %++% yColName %++% hydrar.env$DOT_MAP)) {
  #    lnames <- hydrar.ml.read.categoricalColumnValues(data@transformPath, yColName)
  #  }
  #}
  
  modelPath <- directory %++% hydrar.env$COEFFICIENTS
  debugOutputPath <- directory %++% hydrar.env$DEBUG_LOG
  
  
  xAndY <- .hydrar.separateXAndY(data, yColName)
  trainset_x <- xAndY$X
  trainset_y <- xAndY$Y
  model = "w" # from the *svm.dml script
  args <- list(X = trainset_x, 
               Y = trainset_y, 
               icpt = if(intercept) 1 else 0,
               model, # output
               "extra_model_params",
               "weights",
               Log = debugOutputPath,
               fmt = hydrar.env$CSV)
  
  dmlFile <- NULL
  dmlFilePath <- function(dmlFile) {
    dmlPath <- file.path(hydrar.env$SYSML_ALGO_ROOT(), dmlFile)
  }
  
  if(is.binary.class){
    l2svm <- TRUE
    args <- c(args, dml = dmlFilePath(hydrar.env$DML_L2_SVM_SCRIPT))
  } else {
    l2svm <- FALSE
    args <- c(args, dml = dmlFilePath(hydrar.env$DML_M_SVM_SCRIPT))
  }
 
  if (!missing(lambda)) {
    args <- c(args, reg=lambda)
  }
  if (!missing(tolerance)) {
    args <- c(args, tol=tolerance)
  }
  if (!missing(iter.max)) {
    args <- c(args, maxiter=iter.max)
  }
  
  dmlOutputs <- do.call("sysml.execute", args)
  
  #.Object@dmlOuts$sysml.execute <- dmlOutputs
  #.Object <- hydrar.model.postTraining(.Object)
  
  featureNames <- SparkR::colnames(trainset_x)
  if(intercept == TRUE) {
    featureNames <- c(featureNames, hydrar.env$INTERCEPT)
  }
  #Persist recode maps into the SVM model directory for use during predict
  # @TODO do we want to store the transformation with the code
  svmtransformPath <- ""
  svm <- new("hydrar.svm",
           modelPath=directory, 
           yIdx = yIndex, 
           intercept = intercept, 
           transformPath = svmtransformPath, 
           labelNames=lnames, 
           featureNames=featureNames,
           labelColname=yColName,
           is.binary.class = is.binary.class,
           call = deparse(sys.call(), 500, nlines=1),
           l2svm = l2svm,
           dmlOuts = dmlOutputs
  
        )
  #@TODO store the model later.
  #writeSvmMetadata(svm)
  #garbage collection
  # @TODO remove the temp info from the directory WORKSPACE_DIR etc
  return(svm)
}


setMethod(f = "show", signature = "hydrar.svm", definition = 
            function(object) {
              logSource <- "hydrar.svm.show"
              
              cat("\n\nCoefficients: \n")
              print(object@coefficients)
            }
)

#' @name predict.hydrar.svm
#' @title Predict method for Support Vector Machine classifiers
#' @description This method allows to score/test a Support Vector Machine model for a given hydrar.matrix If the testing set is labeled,
#' testing will be done and some statistics will be computed to measure the quality of the model. Otherwise, scoring will be performed
#' and only the scores will be computed.
#' @param object (hydrar.svm) :
#'  A Support Vector Machine model build by Hydra R
#' @param data (hydrar.matrix) :
#'  Testing dataset
#' @param returnScores (logical) :
#'  A logical value indicating to return the scores or not
#' @return If the testing dataset is not labeled, the result will be a hydrar.matrix with per-class probabilities for each row. 
#' Otherwise, the result will be a list with (1) a hydrar.matrix with per-class probabilities for each row (\code{$probabilities}),
#' (2) the overall accuracy (\code{$accuracy}), and (3) the confusion matrix (\code{$ctable})

#' @examples \dontrun{
#' 
#' # Load the Iris dataset to HDFS 
#' iris_hf <- as.hydrar.frame(iris)
#' 
#' # Create a hydrar.matrix from the Iris dataset
#' iris_hm <- hydrar.transform(bf = iris_hm)
#' 
#' # Split the data into 70% for training and 30% for testing
#' samples <- hydrar.sample(iris_hm, perc=c(0.7, 0.3))
#' train <- samples[[1]]
#' test <- samples[[2]]
#'                          
#' # Build a Support Vector Machine classifier using the training set
#' svm <- hydrar.svm(Species~., data=train, intercept=TRUE)
#' 
#' # Get the coefficients of the model
#' coef(svm)
#' 
#' # Compute scores for the testing set
#' preds <- predict(svm, test)
#' 
#' preds$scores
#' 
#' # Get the confusion matrix
#' preds$ctable
#' 
#' # Get the overall accuracy
#' preds$accuracy
#' }
#' 
#' @export
#' @seealso \link{hydrar.svm}
predict.hydrar.svm <- function(object, data, returnScores=T) {
  logSource <- "predict.hydrar.svm"
  .hydrar.checkParameter(logSource=logSource, parm=data, inheritsFrom = "hydrar.matrix")
  .hydrar.checkParameter(logSource=logSource, parm=returnScores, expectedClasses= "logical")
  
  hydrar.info(logSource, "Predicting labels using given SVM model" %++% object@modelPath %++% "on data")
  
  
  #@TODO eventually we have to support read and write
  #svm <- tryCatch( { hydrar.svm(directory = object@modelPath)
  #}, error = function(err) {
  #  errorMsg <- 'Error reading the model from cluster: ' %++% '{' %++% object@modelPath %++% '}, ' %++% err
  #  hydrar.err(errorMsg)
  #}
  #)
  numFeaturesInModel = nrow(object@coefficients) - if(object@intercept) 1 else 0
  numColumnsInData = length(SparkR:::colnames(data))
  
  testing <- hydrar.ml.checkModelFeaturesMatchData(object@coefficients, data, object@intercept, object@labelColname, object@yIdx)
  
  directory <- hydrar.env$WORKSPACE_ROOT("hydrar.svm")
  # create the partial args 
  augmentArgs <- function(args) {
    args <- c(args, icpt = if(object@intercept) 1 else 0)
    coef_hf <- as.hydrar.matrix(as.hydrar.frame(object@dmlOuts[['w']]))
    #ALOK BEGIN bugfix
    dmlOutNames <- names(object@dmlOuts)
    if (!any(is.na(match(c("extra_model_params", "weights"), dmlOutNames)))) {
      w <- SparkR:::as.data.frame(object@dmlOuts[['weights']])
      e <- SparkR:::as.data.frame(object@dmlOuts[['extra_model_params']])
      e <- setNames(e, names(w))
      d <- rbind(w, e)
      coef_hf <- as.hydrar.matrix(as.hydrar.frame(as.DataFrame(sqlContext, d)))
    }
    #ALOK BEGIN bugfix
    
    args <- c(args, W = coef_hf)
  
    if (returnScores) {
      args <- c(args, "scores") #output
    }
    args <- c(args, fmt = hydrar.env$CSV)
  
  
    dmlFilePath <- function(dmlFile) {
      dmlPath <- file.path(hydrar.env$SYSML_ALGO_ROOT(), dmlFile)
    }
    if (object@l2svm) {
      args <- c(args, dml = dmlFilePath(hydrar.env$DML_L2_SVM_TEST_SCRIPT))
    } else {
      args <- c(args, dml = dmlFilePath(hydrar.env$DML_M_SVM_TEST_SCRIPT))
    }
    
    return(args)
  }
  
 if(testing) {
    
    xAndY <- .hydrar.separateXAndY(data, object@labelColname)
    testset_x <- xAndY$X
    testset_y <- xAndY$Y
    
    args <- list(X = testset_x, y = testset_y)
    
    # we want to have more stats
    args <- c(args, "scoring_only" = "no")
    
    accuracyPath <- file.path(directory, "accuracy.csv")
    confusionPath <- file.path(directory, "confusion.csv")
    
    args <- c(args, "accuracy" = accuracyPath) #output
    args <- c(args, "confusion" = confusionPath) #output , we have to pass this due to dml logic
    args <- c(args, "confusion_mat") #output
    
    args <- augmentArgs(args)
  } else { #only scoring
    if(!returnScores) {
      hydrar.warn(logSource, "Parameter returnScores must be TRUE when scoring using the model.")
      returnScores = TRUE
    }
    #xAndY <- .hydrar.separateXAndY(data, object@labelColname)
    #testset_x <- xAndY$X
    #testset_y <- xAndY$Y
    
    scoreset_x <- data
    #scoreset_x <- testset_x
    args = list(X = scoreset_x)
    args <- c(args, "scoring_only" = "yes")
    args <- augmentArgs(args)
    
  }
  
  dmlOutputs <- do.call("sysml.execute", args)
  
  return(packagePredictSvmOutput(directory, dmlOutputs, object, returnScores, testing, data))

}


packagePredictSvmOutput <- function(outputDir, predictDmlOut, svm, scores, testing, data) {
  logSource <- "packagePredictSvmOutput"
  hydrar.info(logSource, "Packaging output of svm predict")
  acc <- NULL
  con <- NULL
  sc <- NULL
  if(testing) {
    suppressWarnings(
      acc <- read.csv(file.path(outputDir, "accuracy.csv"), header=FALSE, 
                      stringsAsFactors=FALSE, sep=" ")[1,3]
    )
    coltypes <- if(svm@classes==2) "numeric" else as.vector(rep("numeric", svm@classes))
    
    con <- SparkR:::as.data.frame(predictDmlOut$confusion_mat)
    con[is.na(con)] <- 0
    
    if(nrow(con) != ncol(con)) {
      hydrar.err(logSource, "Invalid confusion matrix.")
    }
    
    if(svm@classes > 2) {
      colnames(con) <- colnames(svm@coefficients)
      rownames(con) <- colnames(svm@coefficients)
    }
  }
  if(scores) {
    coltypes = if(svm@classes == 2) "numeric" else rep("numeric", svm@classes)
   
     sc <- predictDmlOut$scores
    
    if((svm@classes > 2) && !is.null(colnames(svm@coefficients))) {
      SparkR::colnames(sc) <- colnames(svm@coefficients)
    } else if((svm@classes == 2) && (svm@labelColname != "") && (svm@l2svm)) {
      SparkR::colnames(sc) <- svm@labelColname
    } else if((svm@classes == 2) && (svm@labelColname != "") && (!svm@l2svm)) {
      SparkR::colnames(sc) <- colnames(svm@coefficients)
    }
  }
  
  if(testing) {
    if(scores) {
      return(list("accuracy"= acc, "ctable" = con, "scores" = sc))
    } else {
      return(list("accuracy"= acc, "ctable" = con))
    }
  } else {
    if(scores) {
      return(list("scores" = sc))
    }
  }
}
