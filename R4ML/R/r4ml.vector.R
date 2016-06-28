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

requireNamespace("SparkR")

setOldClass("hydrar.frame")
setClassUnion("hydrar.frame.OrNull", c("hydrar.frame", "NULL"))

#' @export
setClass("hydrar.vector", 
         slots = list(hf = "hydrar.frame.OrNull"),
         contains="Column")

setMethod("initialize", "hydrar.vector", function(.Object, jc, hf) {
  .Object@jc <- jc
  
  # Some Column objects don't have any referencing DataFrame. In such case, hf will be NULL.
  if (missing(hf)) {
    hf <- NULL
  }
  .Object@hf <- hf
  .Object
})

#' @export
setMethod("show", signature = "hydrar.vector", definition = function(object) {
  head.df <- head(object, hydrar.env$DEFAULT_SHOW_ROWS)
  
  if (length(head.df) == 0) {
    colname <- callJMethod(object@jc, "toString")
    cat(paste0(colname, "\n"))
    cat(paste0("<Empty column>\n"))
  } else {
    show(head.df)
  }
  if (length(head.df) == hydrar.env$DEFAULT_SHOW_ROWS)  {
    cat(paste0("\b...\nDisplaying up to ", as.character(hydrar.env$DEFAULT_SHOW_ROWS), " elements only.\n"))
  }
})

#' @export
setMethod("collect", signature = "hydrar.vector", definition = function(x) {
  if (is.null(x@hf)) {
    character(0)
  } else {
    collect(select(x@hf, x))[, 1]
  }
})

#' @export
setMethod("head", signature = "hydrar.vector", definition = function(x, n=6) {
  if (is.null(x@hf)) {
    collect(x)
  } else {
    head(select(x@hf, x), n)[, 1]
  }
})

#' @export
setMethod("$", signature(x = "hydrar.frame"),
          function(x, name) {
            col <- SparkR:::getColumn(x, name)
            new("hydrar.vector", jc=col@jc, hf=x)
          })


#' Convert the various  data.frame into the hydraR data frame.
#'
#' This is the convenient method of converting the hydrar.vector into the SparkR::Column
#'
#' @name as.sparkr.column
#' @param object hydrar.vector
#' @return SparkR::Column
#' @export
#' @examples \dontrun{
#'    iris_hf <- as.hydrar.frame(iris)
#'    pl_mean <- mean(as.sparkr.column(hf1$Petal_Length))
#'    mval <- agg(iris_hf, pl_mean)
#'    mval
#' }
#'    
setGeneric("as.sparkr.column", function(hv, ...) {
  standardGeneric("as.sparkr.column")
})

setMethod("as.sparkr.column",
          signature(hv = "hydrar.vector"),
          function(hv, ...) {
            SparkR:::column(hv@jc)
          }
)

#########################
# Clone existing methods
#########################

fnames <- ls("package:SparkR", all.names=T)

# Generic methods appear with an extra .__T__ at the beginning. The code below is to remove that prefix:
fnames <- sapply(fnames, function(name) {
  x <- if (substring(name, 1, 3) == ".__") {
    substring(
      strsplit(name, ":")[[1]][1],
      7,
      nchar(name) + 1)
  } else {
    name
  }
  x
})

# Remove duplicates
fnames <- unique(fnames)

names(fnames) <- NULL

# Only methods with Column arguments
methodNames <- sapply(fnames, function(x) {
  methods <- findMethods(x, classes=c("Column"))
  if (length(methods) == 0) {
    NA
  } else {
    x
  }
})

# Filter not needed methods
methodNames <- methodNames[!is.na(methodNames)]
names(methodNames) <- NULL

# Remove certain methods that may cause conflicts
methodNames <- methodNames[-which(methodNames %in% c("length", "show", "head", "collect", "select", "withColumn"))]

createHydraRColumnMethod <- function(funName) {
  
  # Get the method with the given name
  method <- findMethods(funName, classes="Column")
  
  # Get argument names and types
  argNames <- method@arguments
  argTypes <- strsplit(method@names, "#")[[1]]
  
  # Fill argTypes with ANY for parameters not defined in the signature
  argTypes <- c(argTypes, rep("ANY", length(argNames) - length(argTypes)))
  
  # Build signature
  signature <- argTypes
  names(signature) <- argNames
  signature <- ifelse(signature == "Column", "hydrar.vector", signature)
  
  # Special case for operators: Second operator needs to be casted to Column if it was a hydrar.vector
  if (argNames[1] == "e1" & any(argTypes == "Column") & argNames[2] == "e2" & length(argNames) == 2) {
    cat("Creating HydraR operator", funName, "...")
    setMethod(funName, 
              signature,
              function(e1, e2) {
                if (class(e2) == "hydrar.vector") {
                  e2 <- new("Column", jc=e2@jc)
                }
                value <- callNextMethod()
                if (class(value) == "Column") {
                  return(new("hydrar.vector", jc=value@jc, hf=e1@hf))
                }
                return(value)
              })
    cat(" OK\n")
  } else {
    cat("Cloning function", funName, "(", paste(argNames, collapse=", "), ") into HydraR...")
    
    functionCode <- 
      paste(paste0("function(", paste(argNames, collapse=", "), ") {"),
            'value <- callNextMethod()',
            'if (class(value) == "Column") {',
            'colArgName <- names(signature[which(signature == "hydrar.vector")])',
            'args <- as.list(match.call())',
            'colArg <- eval(args[[colArgName]])',
            'return(new("hydrar.vector", jc=value@jc, hf=colArg@hf))',
            '}',
            'return(value)}',
            sep="\n")
    
    setMethod(funName, 
              signature,
              eval(parse(text=functionCode)))
    cat(" OK\n")
  }
}

# Create all methods
for (name in methodNames) {
  createHydraRColumnMethod(name)
}
cat("\n", length(methodNames), "methods were created.")