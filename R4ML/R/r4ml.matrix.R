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
#' @include hydrar.frame.R

#' @name is.hydrar.matrix
#' @title is.hydrar.matrix
#' @description check if the given object is hydrar.matrix
#' @param object the object to test
#' @param ... other arguments to be passed to as.hydrar.frame
#' @export
#' @seealso \link{is.hydrar.numeric}
is.hydrar.matrix <- function(object) {
  if (class(object) != "hydrar.matrix") { return (FALSE)}
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
      ncol <- length(SparkR::colnames(object))
      result = new("hydrar.matrix",
                   sdf = object@sdf,
                   isCached = object@env$isCached
              )
      result@ml.coltypes <- rep(1, ncol)
      #@TODO try to figure our all the details if needed
      ml.coltypes(result) <- rep("scale", ncol)
    } else {
      #@TODO auto convert
      stop("conversion from non numeric hydrar.frame is not supported yet")
    }
    result
  }
)

setMethod("as.hydrar.matrix",
  signature(object = "data.frame"),
  function(object) {
  hf <- as.hydrar.frame(object)
  hm <- as.hydrar.matrix(hf)
  return(hm)
  }
)

setMethod("as.hydrar.matrix",
  signature(object = "SparkDataFrame"),
  function(object) {
  hf <- as.hydrar.frame(object)
  hm <- as.hydrar.matrix(hf)
  return(hm)
  }
)

#' @export
setGeneric("ml.coltypes<-",
  function(x, value) standardGeneric("ml.coltypes<-"))

#' @rdname ml.coltypes
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


#' @name ml.coltypes
##' @name ml.coltypes, ml.coltypes<-
#' @title Set and get column types of a hydrar.matrix
#' @export
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
## @TODO: The following has not been done yet
#' - If transform metadata are available, \code{ml.coltypes} is set to "nominal" for such attributes that
#' have been binned or recoded. All other attributes are considered as "scale".

#' @usage \code{
#' ml.coltypes(bm)
#' ml.coltypes(bm) <- value
#' }
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

#' see  the \link{hydrar.onehot} for usages and details
#' @export  
hydrar.onehot.column <- function(hm, colname) {
  logSource <- "hydrar.onehot.column"
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
    hydrar.err(logSource, "multiple one hot matches")
  }
  if (col_idx < 1 || col_idx > length(hm_names)) {
    hydrar.err(logSource, "col_idx out of range")
  }
  # call the sysml connector to execute the code in the cluster after
  # matrix optimization
  outputs <- sysml.execute(
    dml = dml, # dml code
    X = hm, # attach the input hydrar matrix to X var in dml
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
  data <- as.hydrar.matrix(Y)
  metadata <- new.env(parent=emptyenv())
  assign(colname, oh_new_names, envir=metadata)
  
  list(data=data, metadata=metadata)
}

#' @name hydrar.onehot
#' @title Onehot encode the value that has been previously onehot encoded.
#' @description Specified recoded columns will be 
#'  mapped into onehot encoding. For example, if a column (c1) has values 
#'  then one hot columsn will be c1_1 == c(0,0,1); c1_2 == c(0, 1, 0) and 
#'  c1_3== c(0,0,1), 
#' @param hm a hydrar matrix
#' @param ... list of columns to be onehot encoded. If no columns are given
#'  all are the columns are onehot encoded
#' @details The transformed dataset will be returned as a \code{hydrar.matrix}
#'  object.
#'  The transform meta-info is also returned. This is helpful to keep track 
#'  of what is the new onehot encoded column name.
#'  The structure of the metadata is the nested env
#'    
#' @export
#'      
#' @examples \dontrun{
#'  hf <- as.hydrar.frame(as.data.frame(iris))
#'  hf_rec = hydrar.recode(hf, c("Species"))
#'
#'  # make sure that recoded value is right
#'  rhf_rec <- SparkR::as.data.frame(hf_rec$data)
#'  rhf_data <- rhf_rec$data # recoded hydrar.frame
#'  as.hydrar.matrix(rhf_data)
#'  hf_oh_db <- hydrar.onehot(hm)
#'  hf_oh <- hf_oh_db$data
#'  showDF(hf_oh)
#' }
#'
hydrar.onehot <- function(hm, ... ) {
  args <- list(...)
  if (length(args) <= 0) return(hm)
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
    oh_db <- hydrar.onehot.column(inp_hm, colname)
    hm_list[[length(hm_list)+1]] <- oh_db$data
    md_list[[length(md_list)+1]] <- oh_db$metadata
  }
  data <- hm_list[[length(hm_list)]]
  metadata <- as.environment(sapply(md_list, as.list)) # combine the environ
  list(data=data, metadata=metadata)
}

