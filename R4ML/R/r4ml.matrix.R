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
#' @include hydrar.frame.R

#' check if the given object is hydrar.matrix
#'
#' it checks the class attribute and checks the columns of DF to be numeric
#'
#' @export
#' @seealso \link{is.hydrar.numeric}
is.hydrar.matrix <- function(object) {
  if (vector(class(object)) != "hydrar.matrix") { return (FALSE)}
  if (!is.hydrar.numeric(object)) { return (FALSE)}
  return (TRUE)
}

#' hydrar.matrix an S4 class representing a dataframe with numeric entry.
#'
#' This is used for all the operation on matrix and with interfacing with
#' systemML.
#'
#' @slot type can be numeric or "Vector" or string. numeric is most common
#' @slot ml.coltypes machine learning types i.e one of c("scale", "nominal", "ordinal", "dummy")
#' @export
#' @seealso \link{hydrar.frame}, \link{ml.coltypes}
setClass("hydrar.matrix",
         representation(type = "character", ml.coltypes = "numeric"),
         contains = "hydrar.frame",
         validity = is.hydrar.matrix
)

#' @export
setGeneric("as.hydrar.matrix", function(object) {
  standardGeneric("as.hydrar.matrix")
})

#' @title Convert a hydrar.frame to hydrar.martix by
#' @description It converts the hydrar.frame to hydrar.matrix so that it is consumable by all the relevant ml algo
#'   - assigning machine learning types i.e (scale, nominal, ordinal, dummy).
#'     (default = scale)
#'   - use transform to convert the type so that it is numeric. Not yet implemented
#'   - currently it just check if input are are numeric
#' @param object A hydrar.frame.
#' @return A hydrar.matrix.  The sum of \code{x} and \code{y}.
#' @examples \dontrun{
#' hydrar_frame <-  ...
#' hydrar_matrix <- as.hydrar.matrix(hydrar_frame)
#' }
#' @export
setMethod("as.hydrar.matrix",
  signature(object = "hydrar.frame"),
  function(object) {
    result = NULL
    if (is.hydrar.numeric(object)) {
      ncol <- length(SparkR:::colnames(object))
      result = new("hydrar.matrix",
                   sdf = object@sdf,
                   isCached = object@env$isCached
              )
      result@ml.coltypes <- rep(1, ncol)
      #@TODO try to figure our all the details if needed
      ml.coltypes(result) <- rep("scale", ncol)
    } else {
      #@TODO auto convert
      # else throw error
      stop("can't convert the hydrar.frame to hydrar.matrix")
    }
    result
  }
)



#' @export
setGeneric("ml.coltypes<-",
  function(x, value) standardGeneric("ml.coltypes<-"))

# @describeIn ml.coltypes
#' @export
#' @seealso \link{ml.coltypes}
setMethod("ml.coltypes<-", signature(x = "hydrar.matrix"),
  function(x, value) {
    logSource <- "ml.coltype"

    .hydrar.checkParameter(logSource, value, "character", isSingleton=F)

    if (length(value) != length(ml.coltypes(x))) {
      hydrar.err(logSource, "Invalid number of columns. " %++% length(ml.coltypes(x)) %++%
                         " were expected but " %++% length(value) %++% " were specified")
    }

    # Validate column types
    invalidTypes <- which(!(value %in% hydrar.env$DML_DATA_TYPES))
    if (length(invalidTypes) > 0) {
      hydrar.err(logSource, "Invalid type: " %++% value[invalidTypes[1]])
    }

    coltypesid <- match(value, hydrar.env$DML_DATA_TYPES)
    x@ml.coltypes <- coltypesid
    x
  }
)


#' @name ml.coltypes, ml.coltypes<-
#' @title Set and get column types of a hydrar.matrix
#' @description
#'
#' With this pair of methods, one can get and set column types of
#' a hydrar.matrix. The column types of a hydrar.matrix could be any of "scale" (i.e., continuous) or "nominal" (i.e., categorical).
#' Scale columns can take any real value while nominal columns can only take positive integer numbers, corresponding
#' to each category.
#'
#' Machine learning algorithms have different behavior
#' depending of the specified values in ml.coltypes. For example, descriptive statistics functions
#' such as \code{hydrar.univariateStats()} and \code{hydrar.bivariateStats()} compute different sets of statistics
#' for scale or nominal attributes.
#'
#' @details In the \code{hydrar.matrix()} constructor, \code{ml.coltypes} are initialized as follows:
#'
#' - If no transform metadata are available, \code{ml.coltypes} is set to "scale" for all attributes.
#'
#' TODO: The following has not been done yet
#' - If transform metadata are available, \code{ml.coltypes} is set to "nominal" for such attributes that
#' have been binned or recoded. All other attributes are considered as "scale".

#' @usage
#'
#' \code{ml.coltypes(bm)}
#'
#' \code{ml.coltypes(bm) <- value}
#'
#'
#' @param x (hydrar.matrix)
#' @param value (character) Vector of names with the same length as the number
#' of columns of the hydrar.matrix
#' @return For \code{ml.coltypes()}, return a character vector holding the column
#'  names. For \code{ml.coltypes()<-}, return an updated hydrar.matrix with the updated column types.
#' @rdname ml.coltypes
#' @examples \dontrun{
#'
#' # Load the Iris dataset to HDFS
#' irisbf <- as.hydrar.frame(iris)
#'
#' # Create a hdyrar.matrix after dummycoding, binning, and scaling some attributes
#' # this features is not implemented
#' irisBM <- hydrar.transform(bf = irisbf, outData = "/user/hydrar/examples/irisbf.rcd",
#'                                       transformPath = "/user/hydrar/examples/irisbf.maps",
#'                                       dummycodeAttrs = "Species",
#'                                       binningAttrs = c("Sepal.Length", "Sepal.Width"),
#'                                       numBins=4,
#'                                       scalingAttrs=c("Petal.Length"))
#'
#' # Get ml.coltypes
#' print(ml.coltypes(irisBM))
#'
#' # Set all columns as "scale"
#' ml.coltypes(irisBM) <- rep("scale", 5)
#'
#' # Visualize the structure of the dataset after changing ml.coltypes
#' str(irisBM)
#'
#' }
ml.coltypes <- function(bm) {
  logSource <- "ml.coltypes"
  .hydrar.checkParameter(logSource, bm, inheritsFrom="hydrar.matrix")
  return(hydrar.env$DML_DATA_TYPES[bm@ml.coltypes])
}





