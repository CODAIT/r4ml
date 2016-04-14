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
# @rdname internal.bigr.isNullOrEmpty
# @keywords internal
.hydrar.isNullOrEmpty <- function(x) {
  logSource <- ".hydrar.isNullOrEmpty"
  if (missing(x)) {
    bigr.err(logSource,
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

# check if the seq of inputs has atleast one Null or Empty
.hydrar.hasNullOrEmpty <- function(x) {
  logSource <- ".bigr.isNullOrEmpty"
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

