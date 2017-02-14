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
NULL

# This file contains utility functions that can be used all
# accross the project

# A method to easily concatenate two strings
#' @keywords internal
"%++%" <- function(x, y) {
  paste(x, y, sep = "")
}

# a hack to get an empty symbol
hydrar.emptySymbol <- function() (quote(f(,)))[[2]]


# Checks whether a given object is NULL or empty.
#
# This function returns true if the given object is NULL, NA, "". Also, if
# it is an array which only contains NULL, NA or "" values.
# @param x an object or variable to be evaluated
# @return a logical value indicating whether the given object is NULL, NA or empty
# @rdname internal.hydrar.isNullOrEmpty
# @keywords internal
.hydrar.isNullOrEmpty <- function(x) {
  logSource <- ".hydrar.isNullOrEmpty"
  if (missing(x)) {
    hydrar.err(logSource,
             sprintf("A value for parameter '%s' must be specified.", deparse(substitute(x))))
  }
  if (is.null(x)) {
    return(TRUE)
  } else if (length(x) < 1) {
    return(TRUE)
  }
  if (class(x) == "character" | class(x) == "numeric" | class(x) == "logical" | class(x) == "integer") {
    if (all(is.na(x) | is.null(x) | x == "") ) {
      return(TRUE)
    }
  }
  return(FALSE)
}

# check if the seq of inputs has at least one Null or Empty
.hydrar.hasNullOrEmpty <- function(x) {
  logSource <- ".hydrar.isNullOrEmpty"
  if (.hydrar.isNullOrEmpty(x)) {
    return(TRUE)
  }
  for (i in 1:length(x)) {
    if (.hydrar.isNullOrEmpty(x[i])) {
      return(TRUE)
    }
  }
  return(FALSE)
}

# .hydrar.checkParameter
#
# Utility function used to raise errors when invalid parameter values are specified.
.hydrar.checkParameter <- function(logSource, parm, expectedClasses, expectedValues,
                                 isOptional = F, isNullOK = F, isNAOK=F, isSingleton=T,
                                 checkExistence=F, expectedExistence=F, inheritsFrom) {

  # If the parm is missing, and it's optional, everything is OK
  if (missing(parm) & isOptional)
    return(T)

  # If param is not missing, do a quick check for NULLs
  if (! missing(parm)) {
    if (is.null(parm) & isNullOK)
      return(T)
  }

  # If parm is not missing or NULL, do a quick check for NA's
  if (!missing(parm)) {
    if (length(parm) == 1 & !isS4(parm)) {
      if (is.na(parm) & isNAOK) {
        return(T)
      }
    }
  }

  # Format the parameter name
  parmName <- as.character(substitute(parm))

  # Format expected classes, if passed in.
  ecStr <- NULL
  evStr <- NULL
  if (! missing(expectedClasses)) {
    ecStr <- dQuote(expectedClasses)
    ecStr <- paste(ecStr, collapse=", ")
    if (length(expectedClasses) > 1)
      ecStr <- "(" %++% ecStr %++% ")"

  }

  # Format expected values, if passed in
  if (! missing(expectedValues)) {
    if (length(expectedValues) > 1) {
      if (is.character(expectedValues))
        evStr <- dQuote(expectedValues)
      else
        evStr <- expectedValues
      evStr <- paste(evStr, collapse=", ")
      evStr <- "(" %++% evStr %++% ")"
    }
    else {
      evStr <- ifelse(is.null(expectedValues), "NULL",
                      ifelse(is.character(expectedValues),
                             dQuote(expectedValues), expectedValues))
    }
  }

  # Check if parameter is missing
  if (missing(parm)) {
    if (! is.null(ecStr))
      error <- sprintf("Parameter %s is missing: Expected %s.", dQuote(parmName), ecStr)
    else
      error <- sprintf("Parameter %s is missing: Expected %s.", dQuote(parmName), evStr)
    hydrar.err(logSource, error)
  }
  # Check parameter classes, if passed in.
  if (! missing(expectedClasses)) {
    if (! (class(parm) %in% expectedClasses)) {
      error <- sprintf("Invalid class for parameter %s. %s was expected but %s was specified.",
                       dQuote(parmName), ecStr, dQuote(class(parm)))
      hydrar.err(logSource, error)
    }
  } else if (!missing(inheritsFrom)) {
    if (!inherits(parm, inheritsFrom)) {
      error <- sprintf("Invalid class for parameter %s. A subclass of \"%s\" was expected but %s was specified.",
                       dQuote(parmName), inheritsFrom, dQuote(class(parm)))
      hydrar.err(logSource, error)
    }
  }

  if (! missing(expectedValues)) {
    # Check for NULL equivalency right away
    if (is.null(parm)) {
      if (is.null(expectedValues))
        return (T)
    } else {
      if (all(parm %in% expectedValues))
        return (T)
    }
    if (class(parm) %in% c("logical", "integer", "numeric", "character")) {
      specified <- ifelse(is.null(parm), "NULL",
                          ifelse(is.character(parm), dQuote(parm), parm))
    } else {
      specified <- "object of class " %++% dQuote(class(parm))
    }
    error <- sprintf("Parameter %s has invalid value: Expected %s, specified %s. ",
                     dQuote(parmName), evStr, specified)
    hydrar.err(logSource, error)
  }

  # Check if parameter is an atomic value
  if (class(parm) %in% c("logical", "integer", "numeric", "character")) {
    if (length(parm) != 1 & isSingleton) {
      error <- sprintf("Parameter %s must be a singleton value.", dQuote(parmName))
      hydrar.err(logSource, error)
    }
  }
  return (F)
}

# Indicates whether a string pattern is contained into a given string x
.hydrar.contains <- function(x, pattern) {
  logSource <- "contains"
  if (!.hydrar.isNullOrEmpty(x) & !.hydrar.isNullOrEmpty(pattern)) {
    if (class(x) != "character" | class(pattern) != "character") {
      hydrar.err(logSource, "Contains cannot be applied to " %++% class(x) %++% ", " %++% class(pattern))
    }
    return(regexec(pattern, x, fixed=TRUE)[[1]][1] != -1)
  } else {
    return(FALSE)
  }
}

# since is.integer() of R doesn't check for integer check docs
# , we have this code
.hydrar.is.integer <- function(x) {
  if (.hydrar.isNullOrEmpty(x)) {
    return(FALSE)
  }
  if (length(x) != 1) {
    return(FALSE)
  }
  if (!is.numeric(x)) {
    return(FALSE)
  }
  if (x != trunc(x)) {
    return(FALSE)
  }
  return(TRUE)
}

.hydrar.separateXAndY <- function(xy, yColnames) {
  .hydrar.binary.splitXY(xy, yColnames)
}

.hydrar.binary.splitXY <- function(xy, yColnames) {
  colnames <- SparkR::colnames(xy)
  isYColName <- function (c) c %in% yColnames
  yCols <- Filter(isYColName, colnames)
  xCols <- Filter(Negate(isYColName), colnames)
  #DEBUG browser()
  yHM <- as.hydrar.matrix(SparkR::select(xy, yCols))
  xHM <- as.hydrar.matrix(SparkR::select(xy, xCols))
  XandY <- c(X=xHM, Y=yHM)
  return(XandY)
}


hydrar.ml.checkModelFeaturesMatchData <- function (modelMatrix, dataMatrix, intercept, labelColumnName, labelColumnIndex) {
  logSource <- "hydrar.ml.checkModelFeaturesMatchData"
  hydrar.info(logSource, "Ensuring the consistency between model and data features")
  
  #@TODO add the matrix's nrow and ncol in hydrar.matrix
  #@TODO update rows/columns so that intercept=T does not create inconsistency
  colnames_dm <- SparkR::colnames(dataMatrix)
  rownames_mm <- rownames(modelMatrix)
  nrow_mm <- nrow(modelMatrix) # 
  ncol_dm <- length(SparkR::colnames(dataMatrix))
  str_rownames_mm <- function() {paste(rownames_mm, collapse = ",")}
  str_colnames_dm <- function() {paste(colnames_dm[-labelColumnIndex], collapse = ",")}
  log_error <- function() {
    hydrar.err(logSource, "Data matrix features: [ " %++% 
      str_colnames_dm() %++%
      "] do not match the features of the given model [" %++% 
      str_rownames_mm() %++%"]")
  }
  if (intercept) {
    if (nrow_mm == ncol_dm) {
      if (all(c(colnames_dm[-labelColumnIndex], hydrar.env$INTERCEPT) == rownames_mm)) { 
        testing <- TRUE
      } else {
        log_error()
      }
    } else if (ncol_dm == nrow_mm - 1) {
      if (all(c(colnames_dm, hydrar.env$INTERCEPT) == rownames_mm)) { 
        testing <- FALSE
      } else {
        log_error()
      }
    } else {
      log_error()
    }
  } else {
    if (nrow_mm == ncol_dm) {
      if(all(colnames_dm == rownames_mm)) { 
        testing <- FALSE
      } else {
        log_error()
      }
    } else if (ncol_dm == nrow_mm + 1) {
      if (all(colnames_dm[-labelColumnIndex] == rownames_mm) && 
          colnames_dm[labelColumnIndex] == labelColumnName) { 
        testing <- TRUE
      } else {
        log_error()
      }
    } else {
      log_error()
    }
  }
  return(testing)
}
  

# This function returns the leaves of a formula tree in a preorder fashion
.hydrar.tree.traversal <- function(root) {    
  if (!is.null(root)) {
    # If the current node has children
    if (length(root) > 1) {        
      for (i in 1:length(root)) {                
        .hydrar.tree.traversal(root[[i]])
      }
      # Only add the current node to the list if it has no children    
    } else {
      hydrar.env$leaves[[length(hydrar.env$leaves) + 1]] <- as.character(root)
    }
  }
  hydrar.env$leaves
}

# checks if x is a valid column name based on a regex
.hydrar.validColname <- function(x) {
  if (class(x) != "character") {
    return(FALSE)
  }
  regexans <- gregexpr("[,;+$]",x,perl=FALSE)
  matches <- regexans[[1]][1] > -1
  
  return (!matches)
}

# Extracts a list of column ids using input formula tree: root
# It uses .hydrar.tree.traversal to traverse over nodes of the input tree
# Afterwards it identifies the column-nodes and maps them to the corresponding ids
# This method returns a vector of column-ids
hydrar.extractColsFromFormulaTree  <- function(root, dataset=NULL, delimiter){
  logSource <- "parseFormula"
  # Repeat the same process for formula[[2]]
  hydrar.env$leaves <- list()
  leaves <- .hydrar.tree.traversal(root)
  
  # Output list of group-ids
  columnIds <- list()
  # Output list index
  columnIdx <- 1
  
  # Input list index
  i <- 1
  
  # The hydrar.frame name (optional)
  bfmName <- NULL
  while (i <= length(leaves)) {
    # The column name which an aggregate function will be applied to
    col <- NULL
    
    # If a column name is specified
    if (.hydrar.validColname(leaves[[i]])) {
      col <- leaves[[i]]
      i <- i + 1
      
      # If a dollar symbol was found, the next token should be the dataset name and
      # the next one should be the column name                
    } else if (leaves[[i]] == "$") {
      if (i >= length(leaves) - 1) {
        hydrar.err(logSource, "Invalid formula. A column name must be specified after the $ operator")
      }
      col <- leaves[[i + 2]]
      if (is.null(bfmName)) {
        bfmName <- leaves[[i + 1]]
      } else if (bfmName != leaves[[i + 1]]) {
        hydrar.err(logSource, "All columns specified in the formula must belong to the same hydrar.frame or hydrar.matrix.")
      } else {
        bfmName <- leaves[[i + 1]]
      }
      i <- i + 3
    } else if (leaves[[i]] == delimiter) {
      # Do nothing
      i <- i + 1
    } else {
      hydrar.err(logSource, "Invalid symbol in the formula: '" %++% leaves[[i]] %++% "'")
    }
    
    # If current symbol is not +, there is something to do
    if (!is.null(col)) {
      # If a hydrar.frame was specified in the formula
      if (!is.null(bfmName)) {
        # Check that datasets are consistent
        if (is.null(dataset)) {
          dataset <- get(bfmName)
        } 
      } else if (is.null(dataset)) {
        hydrar.err(logSource, "A hydrar.frame or hydrar.matrix must be specified in the formula.")
      }
      colid <- match(col, SparkR::colnames(dataset))
      if (.hydrar.isNullOrEmpty(colid)) {
        hydrar.err(logSource, "2. Invalid column: '" %++% col %++% "'" )
      }
      columnIds[columnIdx] <- colid
      columnIdx <- columnIdx + 1
    }
  }
  return (columnIds)
}

# Parses survival formula and store them in 2 different frames
# One of those frames stores event and status ids and the other one stores the feature-ids
hydrar.parseSurvivalArgsFromFormulaTree <- function(formula, dataset, directory){
  logSource <- "parseFormula"
  
  # Validate formula
  if (class(formula) != "formula") {
    return(NULL)
  }
  
  # A formula must have three elements: 1. ~, 2. Surv(time,event), 3. col1+col2 + ... colN 
  if (length(formula) != 3) {
    hydrar.err(logSource, "Incomplete formula...")
    return(NULL)
  }
  
  if (formula[[1]] != "~") {
    return(NULL)
  }
  
  left <- formula[[2]]
  right <- formula[[3]]
  
  # Parsing the left side of the formula
  if (length(left) != 3) {
    hydrar.err(logSource, "Incomplete left side of the formula. The left side has to have the following format: Surv(event, status)")
  }
  
  if (left[[1]] != "Surv"){
    hydrar.err(logSource, "Incomplete left side of the formula. The left side has to have the following format: Surv(event, status)")
  }
  
  # The list of time and status
  timeAndStatusIds <- hydrar.extractColsFromFormulaTree(list(left[[2]],left[[3]]), dataset,",")
  survTSFList <- list(unlist(timeAndStatusIds))
  # Parsing the right side of the formula
  if (formula[[3]] != "1") {
    # Using common tree.traversal function
    featureIds <- hydrar.extractColsFromFormulaTree(formula[[3]], dataset, "+")
    # flatten list in case we had dummy-ids
    featureIds <- unlist(featureIds)
    survTSFList[[2]] <- featureIds
  }
  return(survTSFList)
}

#' Find NA columns
#' 
#' A function to find columns in a hydrar.frame which have NA values. This
#' function can be very costly for large datasets.
#' @param hf a hydrar.frame
#' @export
hydrar.which.na.cols <- function(hf) {
 logSource <- "hydrar.which.na.cols"
  
 if (!inherits(hf, "SparkDataFrame")) {
   hydrar.err(logSource, "data type not supported")
 }
 cols <- SparkR::columns(hf)
 rows <- SparkR::nrow(hf)
 
 na_cols <- c()
 
 for (col in cols) {
   hf2 <- SparkR::dropna(x = hf, cols = col)
   if (SparkR::nrow(hf2) < rows) {
     na_cols <- c(na_cols, col)
   }
   
 }
 return(na_cols)
}

