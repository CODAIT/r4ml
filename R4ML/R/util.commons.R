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
#' @include r4ml.env.R

NULL

# This file contains utility functions that can be used all
# accross the project

# A method to easily concatenate two strings
#' @keywords internal
"%++%" <- function(x, y) {
  paste(x, y, sep = "")
}

# a hack to get an empty symbol
r4ml.emptySymbol <- function() (quote(f(,)))[[2]]


# Checks whether a given object is NULL or empty.
#
# This function returns true if the given object is NULL, NA, "". Also, if
# it is an array which only contains NULL, NA or "" values.
# @param x an object or variable to be evaluated
# @return a logical value indicating whether the given object is NULL, NA or empty
# @rdname internal.r4ml.isNullOrEmpty
# @keywords internal
.r4ml.isNullOrEmpty <- function(x) {
  logSource <- ".r4ml.isNullOrEmpty"
  if (missing(x)) {
    r4ml.err(logSource,
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
.r4ml.hasNullOrEmpty <- function(x) {
  logSource <- ".r4ml.isNullOrEmpty"
  if (.r4ml.isNullOrEmpty(x)) {
    return(TRUE)
  }
  for (i in 1:length(x)) {
    if (.r4ml.isNullOrEmpty(x[i])) {
      return(TRUE)
    }
  }
  return(FALSE)
}

# .r4ml.checkParameter
#
# Utility function used to raise errors when invalid parameter values are specified.
.r4ml.checkParameter <- function(logSource, parm, expectedClasses,
                                 expectedValues, isOptional = FALSE,
                                 isNullOK = FALSE, isNAOK = FALSE,
                                 isSingleton = TRUE, inheritsFrom) {

  # If the parm is missing, and it's optional, everything is OK
  if (missing(parm) & isOptional) {
    return(TRUE)
  }

  # If param is not missing, do a quick check for NULLs
  if (!missing(parm)) {
    if (is.null(parm) & isNullOK) {
      return(TRUE)
    } else if (is.null(parm) & !isNullOK) {
      r4ml.err(logSource, paste(dQuote(parm), "cannot be NULL"))
    }
  }

  # If parm is not missing or NULL, do a quick check for NA's
  if (!missing(parm)) {
    if (length(parm) == 1 & !isS4(parm)) {
      if (is.na(parm) & isNAOK) {
        return(TRUE)
      } else if (is.na(parm) & !isNAOK) {
        r4ml.err(logSource, paste(dQuote(parm), "cannot be NA"))
      }
    }
  }

  # Format the parameter name
  parmName <- as.character(substitute(parm))

  # Format expected classes, if passed in.
  ecStr <- NULL
  evStr <- NULL
  if (!missing(expectedClasses)) {
    ecStr <- dQuote(expectedClasses)
    ecStr <- paste(ecStr, collapse = ", ")
    if (length(expectedClasses) > 1) {
      ecStr <- "(" %++% ecStr %++% ")"
    }
  }

  # Format expected values, if passed in
  if (!missing(expectedValues)) {
    if (length(expectedValues) > 1) {
      if (is.character(expectedValues)) {
        evStr <- dQuote(expectedValues)
      } else {
        evStr <- expectedValues
      }
      evStr <- paste(evStr, collapse = ", ")
      evStr <- "(" %++% evStr %++% ")"
    } else {
      evStr <- ifelse(is.null(expectedValues), "NULL",
                      ifelse(is.character(expectedValues),
                             dQuote(expectedValues), expectedValues))
    }
  }

  # Check if parameter is missing
  if (missing(parm)) {
    if (!is.null(ecStr)) {
      error <- sprintf("Parameter %s is missing: Expected %s.", dQuote(parmName), ecStr)
    } else {
      error <- sprintf("Parameter %s is missing: Expected %s.", dQuote(parmName), evStr)
    }
    r4ml.err(logSource, error)
  }
  # Check parameter classes, if passed in.
  if (!missing(expectedClasses)) {
    if (!(class(parm) %in% expectedClasses)) {
      error <- sprintf("Invalid class for parameter %s. %s was expected but %s was specified.",
                       dQuote(parmName), ecStr, dQuote(class(parm)))
      r4ml.err(logSource, error)
    }
  } else if (!missing(inheritsFrom)) {
    if (!inherits(parm, inheritsFrom)) {
      error <- sprintf("Invalid class for parameter %s. A subclass of \"%s\" was expected but %s was specified.",
                       dQuote(parmName), inheritsFrom, dQuote(class(parm)))
      r4ml.err(logSource, error)
    }
  }

  if (!missing(expectedValues)) {
    # Check for NULL equivalency right away
    if (is.null(parm)) {
      if (is.null(expectedValues))
        return(TRUE)
    } else if (all(parm %in% expectedValues)) {
      return(TRUE)
    }
    if (class(parm) %in% c("logical", "integer", "numeric", "character")) {
      specified <- ifelse(is.null(parm), "NULL",
                          ifelse(is.character(parm), dQuote(parm), parm))
    } else {
      specified <- "object of class " %++% dQuote(class(parm))
    }
    error <- sprintf("Parameter %s has invalid value: Expected %s, specified %s. ",
                     dQuote(parmName), evStr, specified)
    r4ml.err(logSource, error)
  }

  # Check if parameter is an atomic value
  if (class(parm) %in% c("logical", "integer", "numeric", "character")) {
    if (length(parm) != 1 & isSingleton) {
      error <- sprintf("Parameter %s must be a singleton value.", dQuote(parmName))
      r4ml.err(logSource, error)
    }
  }
  return(TRUE)
}

# Indicates whether a string pattern is contained into a given string x
.r4ml.contains <- function(x, pattern) {
  logSource <- "contains"
  if (!.r4ml.isNullOrEmpty(x) & !.r4ml.isNullOrEmpty(pattern)) {
    if (class(x) != "character" | class(pattern) != "character") {
      r4ml.err(logSource, "Contains cannot be applied to " %++% class(x) %++% ", " %++% class(pattern))
    }
    return(regexec(pattern, x, fixed=TRUE)[[1]][1] != -1)
  } else {
    return(FALSE)
  }
}

# since is.integer() of R doesn't check for integer check docs
# , we have this code
.r4ml.is.integer <- function(x) {
  if (.r4ml.isNullOrEmpty(x)) {
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

.r4ml.separateXAndY <- function(xy, yColnames) {
  .r4ml.binary.splitXY(xy, yColnames)
}

.r4ml.binary.splitXY <- function(xy, yColnames) {
  colnames <- SparkR::colnames(xy)
  isYColName <- function(c) c %in% yColnames
  yCols <- Filter(isYColName, colnames)
  xCols <- Filter(Negate(isYColName), colnames)
  yHM <- as.r4ml.matrix(SparkR::select(xy, yCols))
  xHM <- as.r4ml.matrix(SparkR::select(xy, xCols))
  XandY <- c(X = xHM, Y = yHM)
  return(XandY)
}


r4ml.ml.checkModelFeaturesMatchData <- function (modelMatrix, dataMatrix, intercept, labelColumnName, labelColumnIndex) {
  logSource <- "r4ml.ml.checkModelFeaturesMatchData"
  r4ml.info(logSource, "Ensuring the consistency between model and data features")
  
  #@TODO add the matrix's nrow and ncol in r4ml.matrix
  #@TODO update rows/columns so that intercept=T does not create inconsistency
  colnames_dm <- SparkR::colnames(dataMatrix)
  rownames_mm <- rownames(modelMatrix)
  nrow_mm <- nrow(modelMatrix) # 
  ncol_dm <- length(SparkR::colnames(dataMatrix))
  str_rownames_mm <- function() {paste(rownames_mm, collapse = ",")}
  str_colnames_dm <- function() {paste(colnames_dm[-labelColumnIndex], collapse = ",")}
  log_error <- function() {
    r4ml.err(logSource, "Data matrix features: [ " %++% 
      str_colnames_dm() %++%
      "] do not match the features of the given model [" %++% 
      str_rownames_mm() %++%"]")
  }
  if (intercept) {
    if (nrow_mm == ncol_dm) {
      if (all(c(colnames_dm[-labelColumnIndex], r4ml.env$INTERCEPT) == rownames_mm)) { 
        testing <- TRUE
      } else {
        log_error()
      }
    } else if (ncol_dm == nrow_mm - 1) {
      if (all(c(colnames_dm, r4ml.env$INTERCEPT) == rownames_mm)) { 
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
.r4ml.tree.traversal <- function(root) {    
  if (!is.null(root)) {
    # If the current node has children
    if (length(root) > 1) {        
      for (i in 1:length(root)) {                
        .r4ml.tree.traversal(root[[i]])
      }
      # Only add the current node to the list if it has no children    
    } else {
      r4ml.env$leaves[[length(r4ml.env$leaves) + 1]] <- as.character(root)
    }
  }
  r4ml.env$leaves
}

# checks if x is a valid column name based on a regex
.r4ml.validColname <- function(x) {
  if (class(x) != "character") {
    return(FALSE)
  }
  regexans <- gregexpr("[,;+$]",x,perl=FALSE)
  matches <- regexans[[1]][1] > -1
  
  return (!matches)
}

# Extracts a list of column ids using input formula tree: root
# It uses .r4ml.tree.traversal to traverse over nodes of the input tree
# Afterwards it identifies the column-nodes and maps them to the corresponding ids
# This method returns a vector of column-ids
r4ml.extractColsFromFormulaTree  <- function(root, dataset = NULL, delimiter = NA){
  logSource <- "parseFormula"
  # Repeat the same process for formula[[2]]
  r4ml.env$leaves <- list()
  leaves <- .r4ml.tree.traversal(root)
  
  # Output list of group-ids
  columnIds <- list()
  # Output list index
  columnIdx <- 1
  
  # Input list index
  i <- 1
  
  # The r4ml.frame name (optional)
  bfmName <- NULL
  while (i <= length(leaves)) {
    # The column name which an aggregate function will be applied to
    col <- NULL
    
    # If a column name is specified
    if (.r4ml.validColname(leaves[[i]])) {
      col <- leaves[[i]]
      i <- i + 1
      
      # If a dollar symbol was found, the next token should be the dataset name and
      # the next one should be the column name                
    } else if (leaves[[i]] == "$") {
      if (i >= length(leaves) - 1) {
        r4ml.err(logSource, "Invalid formula. A column name must be specified after the $ operator")
      }
      col <- leaves[[i + 2]]
      if (is.null(bfmName)) {
        bfmName <- leaves[[i + 1]]
      } else if (bfmName != leaves[[i + 1]]) {
        r4ml.err(logSource, "All columns specified in the formula must belong to the same r4ml.frame or r4ml.matrix.")
      } else {
        bfmName <- leaves[[i + 1]]
      }
      i <- i + 3
    } else if (leaves[[i]] == delimiter) {
      # Do nothing
      i <- i + 1
    } else {
      r4ml.err(logSource, "Invalid symbol in the formula: '" %++% leaves[[i]] %++% "'")
    }
    
    # If current symbol is not +, there is something to do
    if (!is.null(col)) {
      # If a r4ml.frame was specified in the formula
      if (!is.null(bfmName)) {
        # Check that datasets are consistent
        if (is.null(dataset)) {
          dataset <- get(bfmName)
        } 
      } else if (is.null(dataset)) {
        r4ml.err(logSource, "A r4ml.frame or r4ml.matrix must be specified in the formula.")
      }
      colid <- match(col, SparkR::colnames(dataset))
      if (.r4ml.isNullOrEmpty(colid)) {
        r4ml.err(logSource, "2. Invalid column: '" %++% col %++% "'" )
      }
      columnIds[columnIdx] <- colid
      columnIdx <- columnIdx + 1
    }
  }
  return (columnIds)
}

# Parses survival formula and store them in 2 different frames
# One of those frames stores event and status ids and the other one stores the feature-ids
r4ml.parseSurvivalArgsFromFormulaTree <- function(formula, dataset, directory){
  logSource <- "parseFormula"
  
  # Validate formula
  if (class(formula) != "formula") {
    return(NULL)
  }
  
  # A formula must have three elements: 1. ~, 2. Surv(time,event), 3. col1+col2 + ... colN 
  if (length(formula) != 3) {
    r4ml.err(logSource, "Incomplete formula...")
    return(NULL)
  }
  
  if (formula[[1]] != "~") {
    return(NULL)
  }
  
  left <- formula[[2]]
  right <- formula[[3]]
  
  # Parsing the left side of the formula
  if (length(left) != 3) {
    r4ml.err(logSource, "Incomplete left side of the formula. The left side has to have the following format: Surv(event, status)")
  }
  
  if (left[[1]] != "Surv"){
    r4ml.err(logSource, "Incomplete left side of the formula. The left side has to have the following format: Surv(event, status)")
  }
  
  # The list of time and status
  timeAndStatusIds <- r4ml.extractColsFromFormulaTree(list(left[[2]],left[[3]]), dataset,",")
  survTSFList <- list(unlist(timeAndStatusIds))
  # Parsing the right side of the formula
  if (formula[[3]] != "1") {
    # Using common tree.traversal function
    featureIds <- r4ml.extractColsFromFormulaTree(formula[[3]], dataset, "+")
    # flatten list in case we had dummy-ids
    featureIds <- unlist(featureIds)
    survTSFList[[2]] <- featureIds
  }
  return(survTSFList)
}

#' Find NA columns
#' 
#' A function to find columns in a r4ml.frame which have NA values. This
#' function can be very costly for large datasets.
#' @param object a r4ml.frame
#' @export
r4ml.which.na.cols <- function(object) {
 logSource <- "r4ml.which.na.cols"
  
 if (!inherits(object, "SparkDataFrame")) {
   r4ml.err(logSource, "data type not supported")
 }
 cols <- SparkR::columns(object)
 rows <- SparkR::nrow(object)
 
 na_cols <- c()
 
 for (col in cols) {
   hf2 <- SparkR::dropna(x = object, cols = col)
   if (SparkR::nrow(hf2) < rows) {
     na_cols <- c(na_cols, col)
   }
   
 }
 return(na_cols)
}

