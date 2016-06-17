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
#' @include hydrar.vector.R
requireNamespace("SparkR")
#'
#' hydrar.frame, An S4 class which is inherited from SparkR DataFrame
#'
#' Ideally one shouldn't be calling it constructor and use as.hydrar.frame
#' in case one has to call this see the examples
#'
#' @name hydrar.frame
#' @slot same as SparkR::DataFrame
#'
#' @examples \dontrun{
#'
#'  spark_df <- SparkR::createDataFrame(sqlContext, iris)
#'  hydra_frame <- new("hydrar.frame", sdf=spark_df@@sdf, isCached=spark_df@@env$isCached)
#'
#' }
#'
#' @export
#'
#' @seealso \link{as.hydrar.frame} and SparkR::DataFrame
setClass("hydrar.frame", contains="DataFrame")

#' Check if hydrar.frame is numeric or not
#'
#' This method can be used for working with various matrix utils which requires
#' input hydrar.frame to be numeric
#'
#' @name is.hydrar.numeric
#' @param object a hydrar.frame
#' @return TRUE if all the cols are numeric else FALSE
#' @export
#'
#' @examples \dontrun{
#'
#'    hf <- as.hydrar.frame(as.data.frame(iris))
#'    is.hydrar.numeric(hf) #This is FALSE
#'    hf2=as.hydrar.frame(iris[c("Sepal.Length", "Sepal.Width")])
#'    is.hydrar.numeric(hf2) # This is TRUE
#'
#'}
#'
setGeneric("is.hydrar.numeric", function(object, ...) {
  standardGeneric("is.hydrar.numeric")
})


setMethod("is.hydrar.numeric",
  signature(object = "hydrar.frame"),
  function(object, ...) {
    cnames <- SparkR:::colnames(object)
    ctypes <- SparkR:::coltypes(object)
    bad_cols <- cnames[which(sapply(ctypes, function(e) !e %in% c("numeric", "integer", "double")))]
    if (length(bad_cols) >= 1) {
      return (FALSE)
    }
    return (TRUE)
  }
)

#' Convert the various  data.frame into the hydraR data frame.
#'
#' This is the convenient method of converting the data in the distributed hydraR
#'
#' @name as.hydrar.frame
#' @param object a R data.frame or SparkR::DataFrame
#' @return hdf a hydraR dataframe
#' @export
#' @examples \dontrun{
#'    hf1 <- as.hydrar.frame(iris)
#'    hf2 <- as.hydrar.frame(SparkR::createDataFrame(sqlContext, iris))
#' }
setGeneric("as.hydrar.frame", function(object, ...) {
  standardGeneric("as.hydrar.frame")
})

#' @export
setMethod("as.hydrar.frame",
  signature(object = "DataFrame"),
  function(object, ...) {
    hydra_frame <- new("hydrar.frame", sdf=object@sdf, isCached=object@env$isCached)
    hydra_frame
  }
)

#' @export
setMethod("as.hydrar.frame",
  signature(object = "data.frame"),
  function(object, ...) {
    spark_df <- SparkR::createDataFrame(sqlContext, object)
    as.hydrar.frame(spark_df)
  }
)

#' @export
setMethod(f = "show", signature = "hydrar.frame", definition = 
            function(object) {
              logSource <- "hydrar.frame.show"
              # Get the query result as a data.frame
              df <- as.data.frame(
                if (hydrar.env$DEFAULT_SHOW_ROWS > 0) {
                  SparkR:::head(object, hydrar.env$DEFAULT_SHOW_ROWS)
                } else {
                  object                
                }
              )

              if (.hydrar.isNullOrEmpty(df)) {
                df <- data.frame()
              }            
              if (ncol(object) == 0) {
                cat("hydrar.frame with 0 columns\n")
              } else if (nrow(df) == 0) {
                cat(paste(colnames(object), collapse="    "))
                cat("\n<0 rows>\n")
              } else {
                # Show the contents of the hydra.frame as a data.frame
                show(df)
                cat("... " %++% " showing first " %++% hydrar.env$DEFAULT_SHOW_ROWS %++% " rows only.\n")
              }
              invisible(NULL);
            }
)