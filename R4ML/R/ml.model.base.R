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
#' @include util.commons.R
#' @include r4ml.generic.R
#' @include r4ml.matrix.R

NULL

#' An S4 class to represent the r4ml.model
#'
#' r4ml.model acts as the base class of all the other model and it
#' follows the factory method design pattern
#' @slot modelPath (character):  Location where the model files are stored
#' @slot transformPath (character):  Location to store the transform metadata
#' @slot yColname (character):  Column name of the response variable
#' @slot featureNames (character):  The features used to build the model
#' @slot call (character):  String representation of this method's call, including
#' the parameters and values passed to it
#' @slot modelType (character):  Classification, regression, clustering,
#' feature-extraction, factorization, or other
#' @slot yColId (interger):  Column id of the response variable
#' @slot dmlArgs (list):  Arguments passed to the DML script
#' @slot dmlOuts (list):  Arguments returned from the DML script
#' @export
setClass("r4ml.model",
  slots = c(
    modelPath = "character",
    transformPath = "character",
    yColname = "character",
    featureNames = "character",
    call = "character",
    modelType = "character",
    yColId = "integer",
    dmlArgs = "list",
    dmlOuts = "list",
    training_data = "r4ml.matrix"
  ), contains = "VIRTUAL"
)

# Abstract method: r4ml.model.validateTrainingParameters
# ------------------------------------------------------
# This method must be overloaded by every subclass of r4ml.model. In here, specific
# model parameters can be validated: e.g., 'laplace' for Naive Bayes or 'intercept' for linear models.
setGeneric("r4ml.model.validateTrainingParameters", def = 
             r4ml.model.validateTrainingParameters <- function(model, args) {
    r4ml.err("r4ml.model.validateParmaters", "Class " %++% class(model) %++%
               " doesn't implement method r4ml.model.validateTrainingParameters.")
  }
)

# Abstract method: r4ml.model.buildTrainingArgs
# ----------------------------------------------
# This method must be overloaded by every subclass of r4ml.model. In here, each model
# constructs the list of arguments that will be passed along to the DML script through
# sysml.execute().
setGeneric("r4ml.model.buildTrainingArgs", def =
  r4ml.model.buildTrainingArgs <- function(model, args) {
    r4ml.err("r4ml.model.buildTrainingArgs", "Class " %++% class(model) %++%
                " doesn't implement method r4ml.model.buildTrainingArgs.")
    }
)

# Abstract method: r4ml.model.postTraining
# ----------------------------------------------
# This method must be overloaded by every subclass of r4ml.model. In here, each model
# process, the output information created by dml scripts by calling sysml.execute()
setGeneric("r4ml.model.postTraining", def =
      r4ml.model.postTraining <- function(model) {
        r4ml.err("r4ml.model.postTraining", "Class " %++% class(model) %++%
       " doesn't implement method r4ml.model.postTraining.")
      }
)

# Abstract method: r4ml.model.import
# ----------------------------------
# This method must be overloaded by every subclass of r4ml.model. In here, each model
# reads the model metadata from mem/HDFS, as well as the model representation itself (e.g.,
# coefficients for Linear Models or probabilities for Naive Bayes).
# Note: Argument emptyModelForDispatch is just an empty model. It is used for the
# generic function to dispatch the corresponding version of r4ml.model.import for
# each class extending from r4ml.model.
setGeneric("r4ml.model.import", def =
  function(emptyModelForDispatch, directory) {
    r4ml.err("r4ml.model.import", "Class " %++% class(emptyModelForDispatch) %++%
              " doesn't implement method r4ml.model.import.")
  }
)

#export will not be implemented


# r4ml.model's constructor
# ------------------------
# This method defines the general procedure to build any model
# **IMPORTANT**: Subclasses of r4ml.model do not need to implement an initialize
# method but rather, only implement methods r4ml.model.validateTrainingParameters,
# r4ml.model.buildTrainingArgs and r4ml.model.import. The remaining operations,
# e.g., validation of common parameters such as formula and directory, XY splitting,
# DML invocation, writing the model in JSON format, among others, are already implemented in the constructor
# of r4ml.model.
setMethod("initialize", "r4ml.model",
  function(.Object, modelType, ...) {
    logSource <- "r4ml.model.initialize"

   objectClass <- class(.Object)
    r4ml.debug(logSource, "Initializing model: " %++% objectClass)

    # Obtain the corresponding value for each argument name
    argValues <- r4ml.getFunctionArguments(match.call(), sys.parent())

    # Create an empty model. This is useful for method r4ml.import, which
    # needs to be dispatched according to the corresponding class.
    if (is.null(argValues)) {
      return(.Object)
    }

    .r4ml.checkParameter(logSource, modelType, "character", c("classification", "regression",
                                                                      "clustering", "feature-extraction", "factorization", "other"))

    isSupervised <- modelType %in% c("classification", "regression")
    isFeatureExtraction <- modelType %in% c("feature-extraction")
    # Assign common parameters for any r4ml.model objects
    formula <- argValues$formula
    data <- argValues$data
    directory <- argValues$directory
    .Object@call <- deparse(sys.call(sys.parent(3)), 500, nlines = 1)
    .Object@modelType <- modelType

    # Validation of common parameters
    r4ml.debug(logSource, "Common parameter validation...")
    .r4ml.checkParameter(logSource, data, inheritsFrom = "r4ml.matrix", isOptional = T)
    .r4ml.checkParameter(logSource, formula, "formula", isOptional = T, isNullOK = T)

    # If neither formula nor data are provided, we should import the model from the
    # specified directory.
    if (isSupervised == TRUE) {
      if (missing(formula) && missing(data)) {
        .r4ml.checkParameter(logSource, directory, "character")
        return (r4ml.model.import(new(class(.Object)), directory))
      } else if (missing(formula) || missing(data)) {
        # If the model requires a formula, check that both formula and data are provided
        r4ml.err(logSource, "Both formula and data arguments must be provided.")
      }
    } else if (missing(data)) {
      .r4ml.checkParameter(logSource, directory, "character")
      return (r4ml.model.import(new(class(.Object)), directory))
    }

    if (isFeatureExtraction == TRUE) {
      r4ml.debug(logSource, "Feature Extraction is true, assigning data as INPUT param...")
      # place holder for future feature-extraction specific code
      #@TODO
    }
    # Automatically generate a directory path if not provided
    #if (missing(directory)) {
    #    directory <- r4ml.generateFileName()
    #}

    # Check that the dataset is not empty or single-column/single-row.
    #@TODO check if we really need it
    #if(ncol(data) < 2) r4ml.err(logSource, "Given training dataset must have at least two columns.")
    #if(nrow(data) < 2) r4ml.err(logSource, "Given training dataset must have at least two rows")

    # Validate algorithm-specific parameters invoking abstract method
    r4ml.debug(logSource, "Algorithm-specific parameter validation...")
    .Object <- r4ml.model.validateTrainingParameters(.Object, argValues)

    # Check that method r4ml.model.validateTrainingParameters did return an r4ml.model object
    if (class(.Object) != objectClass) {
      r4ml.err(logSource, "Method r4ml.model.validateTrainingParameters must return a '" %++%
                 objectClass %++% "' object." )
    }

    # Extract response variable from the formula if given. Some algorithms require Y to be
    # dummycoded (e.g., random forests and decision trees). yColname represents the response
    # variable in the formula, not in the dummycoded matrix.Therefore, if Species is
    # dummycoded to Species_setosa, Species_virginica, etc., yColname will still be "Species"
    .Object@yColname <-
      if (isSupervised == TRUE) {
         r4ml.debug(logSource, "Getting response variable...")
         r4ml.model.getResponseVariable(formula = formula, data = data, model = .Object)
      } else {
         as.character(NA)
      }

    # Split X and Y.
    if (isSupervised) {
      XY <- r4ml.model.splitXY(data, .Object@yColname)
      argValues$X <- XY$X
      argValues$Y <- XY$Y

      # Assigns yColId
      #DEBUG browser()
      data_colnames <- SparkR::colnames(data)
      .Object@yColId <- match(SparkR::colnames(argValues$Y),
                                      data_colnames)

      # Adds feature names by removing the y column(s) by its/their respective index
      .Object@featureNames <- data_colnames[-.Object@yColId]
    } else {
      .Object@featureNames <- SparkR::colnames(data)
    }

    #browser()

    # Assign additional common attributes

    # Invoke abstract method to build the list of DML arguments
    r4ml.debug(logSource, "Building argument list...")
    .Object <- r4ml.model.buildTrainingArgs(.Object, argValues)
    #browser()
    # Check that method r4ml.model.buildTrainingArgs returned an r4ml.model object
    if (class(.Object) != objectClass) {
      r4ml.err(logSource, "Method r4ml.model.buildTrainingArgs must return a '" %++%
                 objectClass %++% "' object." )
    }
    dmlArgs <- .Object@dmlArgs

    # Run the corresponding DML script and capture potential errors
    r4ml.debug(logSource, "Running DML...")
    r4ml.debugShow(logSource, dmlArgs)
    outputs <- do.call("sysml.execute", dmlArgs)
    .Object@dmlOuts$sysml.execute <- outputs
    .Object <- r4ml.model.postTraining(.Object)

    #
    # Write the model metadata to HDFS @TODO
    #.Object <- r4ml.model.export(.Object, data, directory)
    # test it also


    return(.Object)
  }
)

setGeneric("r4ml.model.getResponseVariable", def =
  function(model, formula, data) {
    r4ml.err("r4ml.model.getResponseVariable", "Class " %++% class(model) %++%
              " doesn't implement method r4ml.model.getResponseVariable")
  }
)

# Extract the response variable from a given formula
setMethod("r4ml.model.getResponseVariable", signature = "r4ml.model", definition =
  function(model, formula, data) {
    logSource <- "r4ml.model.getResponseVariable"
    #@TODO implement later
    f = .r4ml.parseFormulaToPlot(formula, data)
    #f = formula
    # we only support y ~ .

    if (formula[[3]] != ".") {
      r4ml.err(logSource, "Right-hand side of the formula only supports '.'")
    }

    responseName <- f[[3]]
    #responseType <- ml.coltypes(data)[match(responseName, colnames(data))]
    responseType <- ml.coltypes(data)[match(responseName, SparkR::colnames(data))]
    if (model@modelType == "regression") {
      if (responseType != "scale") {
        r4ml.err(logSource, "Response variable must be 'scale' for regression algorithms.")
      }
    } else if (model@modelType == "classification") {
      if (responseType != "nominal") {
        r4ml.err(logSource, "Response variable must be 'nominal' for classification algorithms.")
      }
    }
    return(responseName)
})

# Split a matrix into X and Y
r4ml.model.splitXY <- function(data, yColname) {
  logSource <- "r4ml.model.splitXY"
  r4ml.debug(logSource, "Splitting X and Y...")

  numCols <- 1
  #@TODO do it later
  #dummyCols <- r4ml.ml.getDummyCodedColumns(data, yColname)

  #if (!is.null(dummyCols)) {
  #  yColname <- dummyCols[1]
  #  numCols <- length(dummyCols)
  #}
  return(.r4ml.separateXAndY(data, yColname))
}

r4ml.model.getFeatureVector <- function(data) {
  cols <- SparkR::colnames(data)
  features <- as.r4ml.matrix(SparkR::select(data,cols))
  return(features)
}


# Given a call (which could come from ...), this method retrieves
# the list of arguments passed to the parent function
# @TODO: If an undefined variable is passed as parameter value, missing ends up
# being true. For example, if data=irisBM, but irisBM is not defined, the error
# message will be: "Both formula and data must be specified"
r4ml.getFunctionArguments <- function(call, env) {
  logSource <- "r4ml.getFunctionArguments"
  if (missing(env)) {
    env <- sys.parent()
  }
  argNames <- as.list(call)
  if (length(argNames) < 3) {
    return(NULL)
  }
  argStrings <- names(argNames)
  argValues <- list()

  r4ml.debug(logSource, "Length of argStrings: " %++% length(argStrings))
  r4ml.debug(logSource, "Length of argValues: " %++% length(argValues))
  r4ml.debug(logSource, "Length of argNames: " %++% length(argNames))
  r4ml.debugShow(logSource,  argStrings)
  for (i in 3:length(argNames)) {
    #env <- sys.parent()
    if (!.r4ml.isNullOrEmpty(argStrings[[i]])) {
      #if (exists(as.character(argNames[[i]]), envir=env)) {
      argValues[[argStrings[[i]]]] <- tryCatch( {
        eval(argNames[[i]], env)
      }, error = function(err) {
        errMsg <- as.character(err)
        # If the error was that the parameter is missing, treat it as
        # a missing parameter
        if (.r4ml.contains(errMsg, "is missing, with no default")) {
   r4ml.debug(logSource, "Missing argument: " %++% argStrings[[i]])
   # Empty symbol will be later on recognized as a missing parameter value
   r4ml.emptySymbol()
        } else {
   # If another error was thrown, just rethrow it
   r4ml.err(logSource, errMsg)
        }
      })
    } else {
      r4ml.err(logSource, "Missing argument name was fond in position " %++% i)
      #argNames[[i]] <- NULL#####################
    }
  }
  return(argValues)
}

# This function extracts the required parameters from a formula in order to
# calculate statistics for boxplots and histograms.
# also will be useful for ml formulas
#
# @param formula a formula or r4ml.vector
# @return
.r4ml.parseFormulaToPlot <- function(formula, data=NULL, checkNamesInData=T) {
  logSource <- ".r4ml.parseFormulaToPlot"

  # Validate formula
  if (class(formula) != "formula" && class(formula) != "r4ml.vector") {
    return(NULL)
  }
  if (class(formula) == "formula") {
    charFormula <- as.character(formula)

    # A formula must have three elements: '$', target column and
    # grouping columns
    if (length(formula) != 3) {
      r4ml.warn(logSource, "Incomplete formula.")
      return(NULL)
    }
    if (charFormula[1] != "~") {
      return(NULL)
    }
    if (.r4ml.contains(charFormula[[2]], "+")) {
      r4ml.err(logSource, "Left side of the formula cannot contain the '+' symbol.")
    }

    # Check for invalid operators /symbols
    if (.r4ml.contains(charFormula[2], "-") | .r4ml.contains(charFormula[3],
                                                             "-")) {
      r4ml.err(logSource, "Invalid symbol: -")
    }
    if (.r4ml.contains(charFormula[2], "*") | .r4ml.contains(charFormula[3],
                                                             "*")) {
      r4ml.err(logSource, "Invalid symbol: *")
    }
    if (.r4ml.contains(charFormula[2], "/") | .r4ml.contains(charFormula[3],
                                                             "/")) {
      r4ml.err(logSource, "Invalid symbol: /")
    }
    if (.r4ml.contains(charFormula[2], "^") | .r4ml.contains(charFormula[3],
                                                             "^")) {
      r4ml.err(logSource, "Invalid symbol: ^")
    }
    if (.r4ml.contains(charFormula[2], ":") | .r4ml.contains(charFormula[3],
                                                             ":")) {
      r4ml.err(logSource, "Invalid symbol: :")
    }
    if (.r4ml.contains(charFormula[2], "~") | .r4ml.contains(charFormula[3],
                                                             "~")) {
      r4ml.err(logSource, "Invalid formula: multiple ~ operators are not supported")
    }
    if (.r4ml.contains(charFormula[2], "|") | .r4ml.contains(charFormula[3],
                                                             "|")) {
      r4ml.err(logSource, "Invalid symbol: |")
    }
    if (.r4ml.contains(charFormula[2], "poly(") | .r4ml.contains(charFormula[3],
                                                                 "poly(")) {
      r4ml.err(logSource, "Invalid function: poly(")
    }
    if (.r4ml.contains(charFormula[2], "Error(") | .r4ml.contains(charFormula[3],
                                                                  "Error(")) {
      r4ml.err(logSource, "Invalid function: Error(")
    }
    if (.r4ml.contains(charFormula[2], "I(") | .r4ml.contains(charFormula[3],
                                                              "I(")) {
      r4ml.err(logSource, "Invalid function: I(")
    }

    leftSide <- formula[[2]]
    rightSide <- formula[[3]]

    leftSideChar <- as.character(formula)[2]
    rightSideChar <- as.character(formula)[3]
    groupByColnames <- NULL
    targetColname <- NULL

    #####################################
    # Case 1: No data parameter specified
    #####################################
    if (is.null(data)) {

      # Check that an r4ml.frame is specified for both left and right sides of the formula
      if (!.r4ml.contains(leftSideChar, "$") | !.r4ml.contains(rightSideChar, "$")) {
        r4ml.err(logSource, "an r4ml.frame must be specified (1)")
      }
      # Parse left side
      if (length(leftSide) == 3) {
        data <- eval(formula[[2]][[2]])
        targetColname <- as.character(formula[[2]][[3]])
      } else {
        r4ml.err(logSource, "Invalid formula left side. Incorrect number of arguments.")
      }
      groupByColnames <- all.vars(formula[[3]])[-1]

      ##################################
      # Case 2: data parameter specified
      ##################################
    } else {
      if (.r4ml.contains(leftSideChar, "$") | .r4ml.contains(rightSideChar, "$")) {
        r4ml.err(logSource, "Ambiguous r4ml.frame was found. If argument 'data' is specified, " %++%
                   " operator '$' should not appear in the formula (1)")
      }
      if (length(leftSide) != 1) {
        r4ml.err(logSource, "Ambiguous r4ml.frame was found. If argument 'data' is specified, " %++%
                   " operator '$' should not appear in the formula (2)")
      }
      targetColname <- all.vars(formula[[2]])
      groupByColnames <- all.vars(formula[[3]])
    }
    if (length(targetColname) > 1) {
      r4ml.err(logSource, "More than one target column were found but only one was expected")
    }
    #@TODO change it
    if (class(data) != "r4ml.frame" & !inherits(data, "r4ml.matrix")) {
      r4ml.err(logSource, "Parameter data must be an r4ml.frame or a subclass of r4ml.matrix in order to compute boxplot or histogram statistics.")
    }
    r4ml.debug(logSource, "targetColname: " %++% targetColname)

    # Obtain column id's
    targetColId <- match(targetColname, SparkR::colnames(data))

    if (.r4ml.isNullOrEmpty(targetColname)) {
      r4ml.err(logSource, "Invalid left side of the formula.")
    }

    if (checkNamesInData) {
      if (!(.r4ml.is.integer(targetColId) & targetColId > 0)) {
        r4ml.err(logSource, "Column '" %++% targetColname %++%
                   "' does not belong to the given dataset.")
      }

      # Check that target column is numeric
      coltype <- SparkR::coltypes(data)[targetColId]
      if (coltype != "numeric" & coltype != "integer") {
        r4ml.err(logSource, "Target column for histograms and box plots must be numeric")
      }

      if (length(groupByColnames) == 1 && groupByColnames == ".") {
        groupByColnames = SparkR::colnames(data)[-targetColId]
        if (length(groupByColnames) < 1) {
          r4ml.err(logSource, sprintf("The %s specified does not have any groupby column. Colnames='%s'",
                                      class(data), SparkR::colnames(data)))
        }
      }
    }

    groupByColIds <- match(groupByColnames, SparkR::colnames(data))

    if (checkNamesInData) {
      if (.r4ml.hasNullOrEmpty(groupByColIds)) {
        r4ml.err(logSource, "At least one specified column does not belong to the given dataset")
      }
    }

    return(list(data, targetColId, targetColname, groupByColIds,
         groupByColnames))
  } else {
    # If an r4ml.vector is provided
    if (formula@dataType != "numeric" & formula@dataType != "integer") {
      r4ml.err(logSource, "Target column for plot must be of type 'numeric' or 'integer'.")
    }
    data <- formula
    targetColId <- 1
    targetColname <- formula@name
    groupByColIds <- vector()
    groupByColnames <- vector()
    return(list(data, targetColId, targetColname, groupByColIds,
         groupByColnames))
  }
}
