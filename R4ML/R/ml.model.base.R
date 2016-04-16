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
NULL

#' An S4 class to represent the hydrar.model
#'
#' hydrar.model acts as the base class of all the other model and it
#' follows the factory method design pattern
#' @slot modelPath A length-one numeric vector
#' @export
setClass("hydrar.model",
  representation(
    modelPath = "character",
    transformPath = "character",
    yColname = "character",
    featureNames = "character",
    call = "character",
    modelType = "character",
    yColId = "integer",
    dmlArgs = "list",
    dmlOuts = "list"
  ), "VIRTUAL"
)

# Abstract method: bigr.model.validateTrainingParameters
# ------------------------------------------------------
# This method must be overloaded by every subclass of hydrar.model. In here, specific
# model parameters can be validated: e.g., 'laplace' for Naive Bayes or 'intercept' for linear models.
setGeneric("hydrar.model.validateTrainingParameters", def =
  hydrar.model.validateTrainingParameters <- function(model, args) {
    hydrar.err("hydrar.model.validateParmaters", "Class " %++% class(model) %++%
               " doesn't implement method hydrar.model.validateTrainingParameters.")
  }
)

# Abstract method: hydrar.model.buildTrainingArgs
# ----------------------------------------------
# This method must be overloaded by every subclass of hydrar.model. In here, each model
# constructs the list of arguments that will be passed along to the DML script through
# hydrar.execute().
setGeneric("hydrar.model.buildTrainingArgs", def =
  hydrar.model.buildTrainingArgs <- function(model, args) {
    hydrar.err("hydrar.model.buildTrainingArgs", "Class " %++% class(model) %++%
                " doesn't implement method hydrar.model.buildTrainingArgs.")
    }
)

# Abstract method: hydrar.model.postTraining
# ----------------------------------------------
# This method must be overloaded by every subclass of hydrar.model. In here, each model
# process, the output information created by dml scripts by calling hydrar.execute()
setGeneric("hydrar.model.postTraining", def =
             hydrar.model.postTraining <- function(model) {
               hydrar.err("hydrar.model.postTraining", "Class " %++% class(model) %++%
                            " doesn't implement method hydrar.model.postTraining.")
             }
)

# Abstract method: hydrar.model.import
# ----------------------------------
# This method must be overloaded by every subclass of hydrar.model. In here, each model
# reads the model metadata from mem/HDFS, as well as the model representation itself (e.g.,
# coefficients for Linear Models or probabilities for Naive Bayes).
# Note: Argument emptyModelForDispatch is just an empty model. It is used for the
# generic function to dispatch the corresponding version of hydrar.model.import for
# each class extending from bigr.model.
setGeneric("hydrar.model.import", def =
  function(emptyModelForDispatch, directory) {
    hydrar.err("hydrar.model.import", "Class " %++% class(emptyModelForDispatch) %++%
              " doesn't implement method hydrar.model.import.")
  }
)

#export will not be implemented


# hydrar.model's constructor
# ------------------------
# This method defines the general procedure to build any model
# **IMPORTANT**: Subclasses of hydrar.model do not need to implement an initialize
# method but rather, only implement methods hydrar.model.validateTrainingParameters,
# hydrar.model.buildTrainingArgs and hydrar.model.import. The remaining operations,
# e.g., validation of common parameters such as formula and directory, XY splitting,
# DML invocation, writing the model in JSON format, among others, are already implemented in the constructor
# of hydrar.model.
setMethod("initialize", "hydrar.model",
  function(.Object, modelType, ...) {
    logSource <- "hydrar.model.initialize"

   objectClass <- class(.Object)
    hydrar.info(logSource, "Initializing model: " %++% objectClass)

    # Obtain the corresponding value for each argument name
    argValues <- hydrar.getFunctionArguments(match.call(), sys.parent())

    # Create an empty model. This is useful for method hydrar.import, which
    # needs to be dispatched according to the corresponding class.
    if (is.null(argValues)) {
      return(.Object)
    }

    .hydrar.checkParameter(logSource, modelType, "character", c("classification", "regression",
                                                                      "clustering", "feature-extraction", "factorization", "other"))

    isSupervised <- modelType %in% c("classification", "regression")

    # Assign common parameters for any hydrar.model objects
    formula <- argValues$formula
    data <- argValues$data
    directory <- argValues$directory
    .Object@call <- deparse(sys.call(sys.parent(3)), 500, nlines=1)
    .Object@modelType <- modelType

    # Validation of common parameters
    hydrar.info(logSource, "Common parameter validation...")
    .hydrar.checkParameter(logSource, data, inheritsFrom="hydrar.matrix", isOptional=T)
    .hydrar.checkParameter(logSource, formula, "formula", isOptional=T, isNullOK=T)

    # If neither formula nor data are provided, we should import the model from the
    # specified directory.
    if (isSupervised == TRUE) {
      if (missing(formula) && missing(data)) {
        .hydrar.checkParameter(logSource, directory, "character", checkExistence=T, expectedExistence=T)
        return (hydrar.model.import(new(class(.Object)), directory))
      } else if (missing(formula) || missing(data)) {
        # If the model requires a formula, check that both formula and data are provided
        bigr.err(logSource, "Both formula and data arguments must be provided.")
      }
    } else if (missing(data)) {
      .hydrar.checkParameter(logSource, directory, "character", checkExistence=T, expectedExistence=T)
      return (hydrar.model.import(new(class(.Object)), directory))
    }

    # Automatically generate a directory path if not provided
    #if (missing(directory)) {
    #    directory <- hydrar.generateFileName()
    #}

    # Check that the dataset is not empty or single-column/single-row.
    #@TODO check if we really need it
    #if(ncol(data) < 2) hydrar.err(logSource, "Given training dataset must have at least two columns.")
    #if(nrow(data) < 2) hydrar.err(logSource, "Given training dataset must have at least two rows")

    # Validate algorithm-specific parameters invoking abstract method
    hydrar.info(logSource, "Algorithm-specific parameter validation...")
    .Object <- hydrar.model.validateTrainingParameters(.Object, argValues)

    # Check that method hydrar.model.validateTrainingParameters did return a hydrar.model object
    if (class(.Object) != objectClass) {
      hydrar.err(logSource, "Method hydrar.model.validateTrainingParameters must return a '" %++%
                 objectClass %++% "' object." )
    }

    # Extract response variable from the formula if given. Some algorithms require Y to be
    # dummycoded (e.g., random forests and decision trees). yColname represents the response
    # variable in the formula, not in the dummycoded matrix.Therefore, if Species is
    # dummycoded to Species_setosa, Species_virginica, etc., yColname will still be "Species"
    .Object@yColname <-
      if (isSupervised == TRUE) {
         hydrar.info(logSource, "Getting response variable...")
         hydrar.model.getResponseVariable(formula=formula, data=data, model=.Object)
      } else {
         as.character(NA)
      }

    # Split X and Y.
    if (isSupervised) {
      XY <- hydrar.model.splitXY(data, .Object@yColname)
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
    hydrar.info(logSource, "Building argument list...")
    .Object <- hydrar.model.buildTrainingArgs(.Object, argValues)
    #browser()
    # Check that method hydrar.model.buildTrainingArgs returned a hydrar.model object
    if (class(.Object) != objectClass) {
      hydrar.err(logSource, "Method hydrar.model.buildTrainingArgs must return a '" %++%
                 objectClass %++% "' object." )
    }
    dmlArgs <- .Object@dmlArgs

    # Run the corresponding DML script and capture potential errors
    hydrar.info(logSource, "Running DML...")
    hydrar.infoShow(logSource, dmlArgs)
    outputs <- do.call("sysml.execute", dmlArgs)
    .Object@dmlOuts$sysml.execute <- outputs
    .Object <- hydrar.model.postTraining(.Object)

    #
    # Write the model metadata to HDFS @TODO
    #.Object <- hydrar.model.export(.Object, data, directory)
    # test it also


    return(.Object)
  }
)

setGeneric("hydrar.model.getResponseVariable", def =
  function(model, formula, data) {
    hydrar.err("hydrar.model.getResponseVariable", "Class " %++% class(model) %++%
              " doesn't implement method hydrar.model.getResponseVariable")
  }
)

# Extract the response variable from a given formula
setMethod("hydrar.model.getResponseVariable", signature="hydrar.model", definition =
  function(model, formula, data) {
    logSource <- "hydrar.model.getResponseVariable"
    #@TODO implement later
    f = .hydrar.parseFormulaToPlot(formula, data)
    #f = formula
    # we only support y ~ .

    if (formula[[3]] != ".") {
      hydrar.err(logSource, "Right-hand side of the formula only supports '.'")
    }

    responseName <- f[[3]]
    #responseType <- ml.coltypes(data)[match(responseName, colnames(data))]
    responseType <- ml.coltypes(data)[match(responseName, SparkR:::colnames(data))]
    if (model@modelType == "regression") {
      if (responseType != "scale") {
        hydrar.err(logSource, "Response variable must be 'scale' for regression algorithms.")
      }
    } else if (model@modelType == "classification") {
      if (responseType != "nominal") {
        hydrar.err(logSource, "Response variable must be 'nominal' for classification algorithms.")
      }
    }
    return(responseName)
})

# Split a matrix into X and Y
hydrar.model.splitXY <- function(data, yColname) {
  logSource <- "hydrar.model.splitXY"
  hydrar.info(logSource, "Splitting X and Y...")

  numCols <- 1
  #@TODO do it later
  #dummyCols <- hydrar.ml.getDummyCodedColumns(data, yColname)

  #if (!is.null(dummyCols)) {
  #  yColname <- dummyCols[1]
  #  numCols <- length(dummyCols)
  #}

  colnames <- SparkR::colnames(data)
  isYColName <- function (c) c %in% yColname
  yCols <- Filter(isYColName, colnames)
  xCols <- Filter(Negate(isYColName), colnames)
  #DEBUG browser()
  yHM <- as.hydrar.matrix(as.hydrar.frame(SparkR::select(data, yCols)))
  xHM <- as.hydrar.matrix(as.hydrar.frame(SparkR::select(data, xCols)))
  XandY <- c(X=xHM, Y=yHM)
  return(XandY)
}


# Given a call (which could come from ...), this method retrieves
# the list of arguments passed to the parent function
# TODO: If an undefined variable is passed as parameter value, missing ends up
# being true. For example, if data=irisBM, but irisBM is not defined, the error
# message will be: "Both formula and data must be specified"
hydrar.getFunctionArguments <- function(call, env) {
  logSource <- "hydrar.getFunctionArguments"
  if (missing(env)) {
    env <- sys.parent()
  }
  argNames <- as.list(call)
  if (length(argNames) < 3) {
    return(NULL)
  }
  argStrings <- names(argNames)
  argValues <- list()

  hydrar.info(logSource, "Length of argStrings: " %++% length(argStrings))
  hydrar.info(logSource, "Length of argValues: " %++% length(argValues))
  hydrar.info(logSource, "Length of argNames: " %++% length(argNames))
  hydrar.infoShow(logSource,  argStrings)
  for (i in 3 : length(argNames)) {
    #env <- sys.parent()
    if (!.hydrar.isNullOrEmpty(argStrings[[i]])) {
      #if (exists(as.character(argNames[[i]]), envir=env)) {
      argValues[[argStrings[[i]]]] <- tryCatch( {
        eval(argNames[[i]], env)
      }, error = function(err) {
        errMsg <- as.character(err)
        # If the error was that the parameter is missing, treat it as
        # a missing parameter
        if (.hydrar.contains(errMsg, "is missing, with no default")) {
          hydrar.info(logSource, "Missing argument: " %++% argStrings[[i]])
          # Empty symbol will be later on recognized as a missing parameter value
          hydrar.emptySymbol()
        } else {
          # If another error was thrown, just rethrow it
          hydrar.err(logSource, errMsg)
        }
      })
    } else {
      hydrar.err(logSource, "Missing argument name was fond in position " %++% i)
      #argNames[[i]] <- NULL#####################
    }
  }
  return(argValues)
}

# This function extracts the required parameters from a formula in order to
# calculate statistics for boxplots and histograms.
# also will be useful for ml formulas
#
# @param formula a formula or a bigr.vector
# @return
.hydrar.parseFormulaToPlot <- function(formula, data=NULL, checkNamesInData=T) {
  logSource <- ".hydrar.parseFormulaToPlot"

  # Validate formula
  if (class(formula) != "formula" && class(formula) != "bigr.vector") {
    return(NULL)
  }
  if (class(formula) == "formula") {
    charFormula <- as.character(formula)

    # A formula must have three elements: '$', target column and
    # grouping columns
    if (length(formula) != 3) {
      hydrar.warn(logSource, "Incomplete formula.")
      return(NULL)
    }
    if (charFormula[1] != "~") {
      return(NULL)
    }
    if (.hydrar.contains(charFormula[[2]], "+")) {
      hydrar.err(logSource, "Left side of the formula cannot contain the '+' symbol.")
    }

    # Check for invalid operators /symbols
    if (.hydrar.contains(charFormula[2], "-") | .hydrar.contains(charFormula[3],
                                                             "-")) {
      hydrar.err(logSource, "Invalid symbol: -")
    }
    if (.hydrar.contains(charFormula[2], "*") | .hydrar.contains(charFormula[3],
                                                             "*")) {
      hydrar.err(logSource, "Invalid symbol: *")
    }
    if (.hydrar.contains(charFormula[2], "/") | .hydrar.contains(charFormula[3],
                                                             "/")) {
      hydrar.err(logSource, "Invalid symbol: /")
    }
    if (.hydrar.contains(charFormula[2], "^") | .hydrar.contains(charFormula[3],
                                                             "^")) {
      hydrar.err(logSource, "Invalid symbol: ^")
    }
    if (.hydrar.contains(charFormula[2], ":") | .hydrar.contains(charFormula[3],
                                                             ":")) {
      hydrar.err(logSource, "Invalid symbol: :")
    }
    if (.hydrar.contains(charFormula[2], "~") | .hydrar.contains(charFormula[3],
                                                             "~")) {
      hydrar.err(logSource, "Invalid formula: multiple ~ operators are not supported")
    }
    if (.hydrar.contains(charFormula[2], "|") | .hydrar.contains(charFormula[3],
                                                             "|")) {
      hydrar.err(logSource, "Invalid symbol: |")
    }
    if (.hydrar.contains(charFormula[2], "poly(") | .hydrar.contains(charFormula[3],
                                                                 "poly(")) {
      hydrar.err(logSource, "Invalid function: poly(")
    }
    if (.hydrar.contains(charFormula[2], "Error(") | .hydrar.contains(charFormula[3],
                                                                  "Error(")) {
      hydrar.err(logSource, "Invalid function: Error(")
    }
    if (.hydrar.contains(charFormula[2], "I(") | .hydrar.contains(charFormula[3],
                                                              "I(")) {
      hydrar.err(logSource, "Invalid function: I(")
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

      # Check that a hydrar.frame is specified for both left and right sides of the formula
      if (!.hydrar.contains(leftSideChar, "$") | !.hydrar.contains(rightSideChar, "$")) {
        hydrar.err(logSource, "A hydrar.frame must be specified (1)")
      }
      # Parse left side
      if (length(leftSide) == 3) {
        data <- eval(formula[[2]][[2]])
        targetColname <- as.character(formula[[2]][[3]])
      } else {
        hydrar.err(logSource, "Invalid formula left side. Incorrect number of arguments.")
      }
      groupByColnames <- all.vars(formula[[3]])[-1]

      ##################################
      # Case 2: data parameter specified
      ##################################
    } else {
      if (.hydrar.contains(leftSideChar, "$") | .hydrar.contains(rightSideChar, "$")) {
        hydrar.err(logSource, "Ambiguous hydrar.frame was found. If argument 'data' is specified, " %++%
                   " operator '$' should not appear in the formula (1)")
      }
      if (length(leftSide) != 1) {
        hydrar.err(logSource, "Ambiguous hydrar.frame was found. If argument 'data' is specified, " %++%
                   " operator '$' should not appear in the formula (2)")
      }
      targetColname <- all.vars(formula[[2]])
      groupByColnames <- all.vars(formula[[3]])
    }
    if (length(targetColname) > 1) {
      hydrar.err(logSource, "More than one target column were found but only one was expected")
    }
    #@TODO change it
    if (class(data) != "hydrar.frame" & !inherits(data, "hydrar.matrix")) {
      hydrar.err(logSource, "Parameter data must be a bigr.frame or a subclass of bigr.matrix in order to compute boxplot or histogram statistics.")
    }
    hydrar.info(logSource, "targetColname: " %++% targetColname)

    # Obtain column id's
    targetColId <- match(targetColname, SparkR::colnames(data))

    if (.hydrar.isNullOrEmpty(targetColname)) {
      hydrar.err(logSource, "Invalid left side of the formula.")
    }

    if (checkNamesInData) {
      if (!(.hydrar.is.integer(targetColId) & targetColId > 0)) {
        hydrar.err(logSource, "Column '" %++% targetColname %++%
                   "' does not belong to the given dataset.")
      }

      # Check that target column is numeric
      coltype <- coltypes(data)[targetColId]
      if (coltype != "numeric" & coltype != "integer") {
        hydrar.err(logSource, "Target column for histograms and box plots must be numeric")
      }

      if (length(groupByColnames) == 1 && groupByColnames==".") {
        groupByColnames = SparkR::colnames(data)[-targetColId]
        if (length(groupByColnames) < 1) {
          hydrar.err(logSource, sprintf("The %s specified does not have any groupby column. Colnames='%s'", class(data), colnames(data)))
        }
      }
    }

    groupByColIds <- match(groupByColnames, SparkR::colnames(data))

    if (checkNamesInData) {
      if (.hydrar.hasNullOrEmpty(groupByColIds)){
        hydrar.err(logSource, "At least one specified column does not belong to the given dataset")
      }
    }

    return(list(data, targetColId, targetColname, groupByColIds,
                groupByColnames))
  } else {
    # If a bigr.vector is provided
    if (formula@dataType != "numeric" & formula@dataType != "integer") {
      hydrar.err(logSource, "Target column for plot must be of type 'numeric' or 'integer'.")
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
