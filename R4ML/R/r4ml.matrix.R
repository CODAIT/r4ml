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
#' @include r4ml.frame.R

#' @name is.r4ml.matrix
#' @title is.r4ml.matrix
#' @description check if the given object is r4ml.matrix
#' @param object the object to test
#' @export
#' @seealso \link{is.r4ml.numeric}
is.r4ml.matrix <- function(object) {
  if (class(object) != "r4ml.matrix") { return (FALSE)}
  if (!is.r4ml.numeric(object)) { return (FALSE)}
  return (TRUE)
}

#' r4ml.matrix an S4 class representing a dataframe with numeric entry.
#'
#' This is used for all the operation on matrix and with interfacing with
#' systemML.
#'
#' @slot type can be numeric or "Vector" or string. numeric is most common
#' @slot ml.coltypes machine learning types i.e one of c("scale", "nominal", "ordinal", "dummy")
#' @export
#' @seealso \link{r4ml.frame}, \link{ml.coltypes}
setClass("r4ml.matrix",
         representation(type = "character", ml.coltypes = "numeric"),
         contains = "r4ml.frame",
         validity = is.r4ml.matrix
)
#' Coerce to a R4ML Matrix
#'
#' Convert an object to a r4ml.matrix
#' @param object A r4ml.frame, data.frame, or SparkDataFrame
#' @param cache (logical) should the object be cached
#' @return A r4ml.matrix
#' @examples \dontrun{
#' r4ml_matrix <- as.r4ml.matrix(datasets::beaver1, cache = TRUE)
#' }
#' @export
setGeneric("as.r4ml.matrix", function(object, cache = TRUE) {
  standardGeneric("as.r4ml.matrix")
})

setMethod("as.r4ml.matrix",
  signature(object = "r4ml.frame"),
  function(object, cache) {
    logSource <- "as.r4ml.matrix"
    result = NULL
    if (is.r4ml.numeric(object)) {
      ncol <- length(SparkR::colnames(object))
      result = new("r4ml.matrix",
                   sdf = object@sdf,
                   isCached = object@env$isCached
              )
      result@ml.coltypes <- rep(1, ncol)
      #@TODO try to figure our all the details if needed
      ml.coltypes(result) <- rep("scale", ncol)
    } else {
      r4ml.err(logSource, "Use hydrar.ml.preprocess to convert r4ml.frame to numeric r4ml.matrix")
    }
    if (cache & !result@env$isCached) {
      result <- SparkR::cache(result)
    }
    return(result)
  }
)

setMethod("as.r4ml.matrix",
  signature(object = "data.frame"),
  function(object, cache) {
  hf <- as.r4ml.frame(object)
  hm <- as.r4ml.matrix(hf, cache)
  return(hm)
  }
)

setMethod("as.r4ml.matrix",
  signature(object = "SparkDataFrame"),
  function(object, cache) {
  hf <- as.r4ml.frame(object)
  hm <- as.r4ml.matrix(hf, cache)
  return(hm)
  }
)

#' @export
setGeneric("ml.coltypes<-",
  function(x, value) standardGeneric("ml.coltypes<-"))

#' @rdname ml.coltypes
#' @export
#' @seealso \link{ml.coltypes}
setMethod("ml.coltypes<-", signature(x = "r4ml.matrix"),
  function(x, value) {
    logSource <- "ml.coltype"

    .r4ml.checkParameter(logSource, value, "character", isSingleton=F)

    if (length(value) != length(ml.coltypes(x))) {
      r4ml.err(logSource, "Invalid number of columns. " %++% length(ml.coltypes(x)) %++%
                         " were expected but " %++% length(value) %++% " were specified")
    }

    # Validate column types
    invalidTypes <- which(!(value %in% r4ml.env$DML_DATA_TYPES))
    if (length(invalidTypes) > 0) {
      r4ml.err(logSource, "Invalid type: " %++% value[invalidTypes[1]])
    }

    coltypesid <- match(value, r4ml.env$DML_DATA_TYPES)
    x@ml.coltypes <- coltypesid
    x
  }
)


#' @name ml.coltypes
##' @name ml.coltypes, ml.coltypes<-
#' @title Set and get column types of a r4ml.matrix
#' @export
#' @description
#'
#' With this pair of methods, one can get and set column types of
#' a r4ml.matrix. The column types of a r4ml.matrix could be any of "scale" (i.e., continuous) or "nominal" (i.e., categorical).
#' Scale columns can take any real value while nominal columns can only take positive integer numbers, corresponding
#' to each category.
#'
#' Machine learning algorithms have different behavior
#' depending of the specified values in ml.coltypes. For example, descriptive statistics functions
#' such as \code{r4ml.univariateStats()} and \code{r4ml.bivariateStats()} compute different sets of statistics
#' for scale or nominal attributes.
#'
#' @details In the \code{r4ml.matrix()} constructor, \code{ml.coltypes} are initialized as follows:
#'
#' - If no transform metadata are available, \code{ml.coltypes} is set to "scale" for all attributes.
#'
## @TODO: The following has not been done yet
#' - If transform metadata are available, \code{ml.coltypes} is set to "nominal" for such attributes that
#' have been binned or recoded. All other attributes are considered as "scale".

#' @usage ml.coltypes(x)
#'
#'
#' @param x (r4ml.matrix)
#' @param value (character) Vector of names with the same length as the number
#' of columns of the r4ml.matrix
#' @return For \code{ml.coltypes()}, return a character vector holding the column
#'  names. For \code{ml.coltypes()<-}, return an updated r4ml.matrix with the updated column types.
#' @rdname ml.coltypes
#' @examples \dontrun{
#'
#' # Load the Iris dataset
#' irisbf <- as.r4ml.frame(iris)
#'
#' # Create a r4ml.matrix after dummycoding, binning, and scaling some attributes
#' irisBM <- r4ml.ml.preprocess(data = irisbf,
#'                              transformPath = "/tmp",
#'                              dummycodeAttrs = "Species",
#'                              binningAttrs = c("Sepal_Length", "Sepal_Width"),
#'                              numBins = 4,
#'                              scalingAttrs = c("Petal_Length"))
#'
#' irisBM$data <- as.r4ml.matrix(irisBM$data)
#'
#' # Get ml.coltypes
#' print(ml.coltypes(irisBM$data))
#'
#' # Set all columns as "scale"
#' ml.coltypes(irisBM$data) <- rep("scale", 7)
#'
#' # Visualize the structure of the dataset after changing ml.coltypes
#' str(irisBM)
#'
#' }
ml.coltypes <- function(x) {
  logSource <- "ml.coltypes"
  .r4ml.checkParameter(logSource, x, inheritsFrom="r4ml.matrix")
  return(r4ml.env$DML_DATA_TYPES[x@ml.coltypes])
}

#' r4ml.onehot.column
#' 
#' see  the \link{r4ml.onehot} for usages and details
#' 
#' @param hm a r4ml.matrix
#' @param colname name of column to be one hot encoded
#' @export  
r4ml.onehot.column <- function(hm, colname) {
  logSource <- "r4ml.onehot.column"
  dml= '
  #  # encode dml function for one hot encoding
  encode_onehot = function(matrix[double] X) return(matrix[double] Y) {
  N = nrow(X)
  Y = table(seq(1, N, 1), X)
  }
  # a dummy read, which allows sysML to attach variables
  X = read("") 
  
  col_idx = $onehot_index
  
  nc = ncol(X)
  if (col_idx < 1 | col_idx > nc) {
  stop("one hot index out of range")
  }
  Y = matrix(0, rows=1, cols=1)
  oneHot = encode_onehot(X[,col_idx:col_idx])
  if (col_idx == 1) {
  if (col_idx < nc) {
  X_tmp = X[, col_idx+1:nc]
  Y = append(oneHot, X_tmp)
  } else {
  Y = oneHot
  }
  } else if (1 < col_idx & col_idx < nc) {
  Y = append(append(X[,1:col_idx-1], oneHot), X[, col_idx+1:nc])
  } else { # col_idx == nc
  Y = append(X[,1:col_idx-1], oneHot)
  }
  # a dummy write, which allows sysML to attach varibles
  write(Y, "")
  
  '
  
  hm_names <- SparkR::colnames(hm)
  oh_colname <- colname
  col_idx <- match(oh_colname, hm_names)
  if (length(col_idx) > 1) {
    r4ml.err(logSource, "multiple one hot matches")
  }
  if (col_idx < 1 || col_idx > length(hm_names)) {
    r4ml.err(logSource, "col_idx out of range")
  }
  # call the sysml connector to execute the code in the cluster after
  # matrix optimization
  outputs <- sysml.execute(
    dml = dml, # dml code
    X = hm, # attach the input r4ml matrix to X var in dml
    onehot_index  = col_idx,
    "Y" # attach the output Y from dml
  )
  Y = outputs[['Y']] # get the output dataframes
  
  # assign the proper column names
  hm_names <- SparkR::colnames(hm)
  hm_colsize <- length(hm_names)
  oh_name <- hm_names[col_idx]
  y_ncol <- SparkR::ncol(Y)
  oh_colsize <- y_ncol - (length(hm_names) - 1)
  oh_new_names <- paste(oh_name, 1:oh_colsize, sep="_")
  y_names <- character(0)
  if (col_idx == 1) {
    if (col_idx < hm_colsize) {
      y_names <- c(oh_new_names, hm_names[2:hm_colsize])
    } else {
      y_names <- oh_new_names
    }
  } else if (1 < col_idx && col_idx < hm_colsize) {
    y_names <- c(hm_names[1:col_idx-1], oh_new_names, hm_names[(col_idx+1):hm_colsize])
  } else{
    y_names <- c(hm_names[1:col_idx-1], oh_new_names)
  }
  SparkR::colnames(Y) <- y_names
  data <- as.r4ml.matrix(Y)
  metadata <- new.env(parent=emptyenv())
  assign(colname, oh_new_names, envir=metadata)
  
  list(data=data, metadata=metadata)
}

#' @name r4ml.onehot
#' @title Onehot encode the value that has been previously onehot encoded.
#' @description Specified recoded columns will be 
#'  mapped into onehot encoding. For example, if a column (c1) has values 
#'  then one hot columns will be c1_1 == c(0,0,1); c1_2 == c(0, 1, 0) and 
#'  c1_3== c(0,0,1), 
#' @param hm a r4ml matrix
#' @param ... list of columns to be onehot encoded. If no columns are given
#'  all are the columns are onehot encoded
#' @details The transformed dataset will be returned as a \code{r4ml.matrix}
#'  object.
#'  The transform meta-info is also returned. This is helpful to keep track 
#'  of what is the new onehot encoded column name.
#'  The structure of the metadata is the nested env
#'    
#' @export
#'      
#' @examples \dontrun{
#' hf <- as.r4ml.frame(iris)
#' hf <- r4ml.recode(hf, c("Species"))
#' hm <- as.r4ml.matrix(hf$data)
#' hm <- r4ml.onehot(hm, "Species")
#' }
#'
r4ml.onehot <- function(hm, ... ) {
  logSource <- "r4ml.onehot"
  args <- list(...)
  if (length(args) <= 0) {
    r4ml.warn(logSource, "no columns specified")
    return(list(data = hm, metadata = NA))
  }
  # check if it is list
  colnames <- args
  if (length(args) == 1 && class(args[1]) == "list") {
    colnames <- args[[1]]
  }
  
  hm_list <- list(hm)
  md_list <- list()
  out_hm <- hm
  for (i in 1:length(colnames)) {
    colname <- colnames[[i]]
    inp_hm <- hm_list[[length(hm_list)]] # last element
    oh_db <- r4ml.onehot.column(inp_hm, colname)
    hm_list[[length(hm_list)+1]] <- oh_db$data
    md_list[[length(md_list)+1]] <- oh_db$metadata
  }
  data <- hm_list[[length(hm_list)]]
  metadata <- as.environment(sapply(md_list, as.list)) # combine the environ
  list(data=data, metadata=metadata)
}

