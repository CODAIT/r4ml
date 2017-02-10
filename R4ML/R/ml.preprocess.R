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


# Method to transform a hydrar.frame to a hydrar.matrix that can be read by DML 
#'
#' @name hydrar.ml.preprocess
#' @title Data preparation for statistical analysis and machine learning algorithms
#' @description Performs various data preparation operations on a \code{hydrar.frame} and produces a \code{hydrar.matrix}.
#'              Five operations are supported:
#'              
#'              1. \emph{Imputation of missing values}: Missing values (i.e., NA's in the input \code{hydrar.frame})
#'              on the specified columns will be replaced according to the method set in \code{imputationMethod}.
#'              \strong{Note}: NA values must be handled by either imputation (parameter \code{missingAttrs})
#'              or exclusion (parameter \code{omit.na}).
#'              
#'              2. \emph{Recoding}: Specified categorical columns will be 
#'              mapped into consecutive numeric categories. For example, if a column has values "Low", "Medium", and "High",
#'              these will be mapped to 1, 2, and 3. \strong{Note}: All columns of type character will be automatically recoded.
#'              The order of the recoded values is non-deterministic.
#'              
#'              3. \emph{Dummycoding} aka \emph{OneHotEncoding}: Each specified column will result into as many binary columns as categories in it. 
#'              For instance, if a column named \emph{income} has values "Low", "Medium", and "High", it will be mapped 
#'              into three boolean columns: "income_Low", "income_Medium", and "income_High". First column will be 1 if \emph{income}
#'              is "Low" or 0 otherwise, and similarly for categories "Medium" and "High".
#'              
#'              4. \emph{Binning}: Specified numeric columns will be discretized into as many bins as set in \code{numBins},
#'              using the binning method set in \code{binningMethod}.
#'              
#'              5. \emph{Scaling}: Specified numeric columns will be scaled using the method set on \code{scalingMethod}.
#'
#'              The resulting matrix and the transform metadata will be returned and it's user's responsibility to store the data
#'             \code{transformPath}). The transform metadata can be  
#'              used to perform the same set of transformations on another \code{hydrar.frame}, through parameter \code{applyTransformPath}.
#' @param hf (hydrar.frame) The data to be transformed.
#' @param transformPath (character) Path on HDFS where the transform metadata (e.g., recode maps, dummy-code maps, etc.) will be stored.
#' @param applyTransformPath (character) Path of an existing transform metadata folder. 
#'                                       If specified, \code{hydrar.ml.preprocess()} will apply the same transformations to \code{bf}.
#'                                       Transform metadata for the resulting hydrar.matrix will be copied to the location specified in \code{transformPath}.
#'                                       Parameter \code{applyTransformPath} is optional.
#' @param recodeAttrs (character) Names of attributes to be recoded. All columns of type 'character' will be recoded, even if they do not
#'                                appear in \code{recodeAttrs}.
#' @param missingAttrs (character) Names of of attributes containing missing values to be filled in using an imputation method.
#'                                 \strong{Note}: 1) Function \code{hydrar.which.na.cols()} can be run to find out which columns contain missing values.
#'                                       2) If no imputation is done on a column with missing values, and such column is not part of \code{omit.na}, hydrar.ml.preprocess 
#'                                       will throw an error, as all NA values must be handled. Parameter \code{omit.na} can be used to remove all rows which have missing values
#'                                       in specific columns.
#' @param imputationMethod (character) String value or vector containing the imputation method(s) to be used to fill in missing values.
#'                         Note: the order of imputation methods should coincide with the order of the attributes specified in \code{missingAttrs}. 
#'                         If a single string is provided as \code{imputationMethod}, such method will be used for all columns in \code{missingAttrs}.
#'                         Supported methods: (1) "global_mean", (2) "constant".
#'                         (a). If the "constant" imputation method is used then the corresponding replacement values
#'                         must be provided in \code{imputationValues} parameter (see below).
#'                         (b) If the "global_mode" imputation method is used and the dataset has multiple modes for the corresponding column,
#'                         then one of the modes is chosen to impute for the missing values arbitrarily.
#' @param imputationValues (list) A List of values to be used as replacement values for the columns to be imputated using the "constant" imputation method.                        
#' @param binningAttrs (character) Name(s) of numeric attribute(s) to be binned.
#' @param numBins (integer) Integer or vector denoting the number of bins to use on each column.
#'                      Note: the order of the values specified in \code{numbins} should coincide with the order of the 
#'                      attributes specified in \code{binningAttrs}.  If a single integer is provided, such number of bins
#'                      will be used for all attributes specified in \code{binningAttrs}.
#' @param binningMethod (character) String or vector specifying binning method(s) to be used. 
#'                      Note: the order of the elements of \code{binningMethod} should coincide with the order of the 
#'                      attributes specified in \code{binningAttrs}.
#'                      If a single string is provided, such method will be used for all attributes specified in \code{binningAttrs}.
#'                      Currently, "equi-width" is supported as binning method.
#' @param dummycodeAttrs (character) Names of attributes to by dummy-coded.
#' @param scalingAttrs (character) Names of attributes to be scaled.
#' @param scalingMethod (character) String or vector specifying the scaling method(s) to be used.
#'                      Note: the order of scaling methods must coincide with the order of the attributes specified in \code{scalingAttrs}.
#'                      If a single string is provided, such method will be used for all attributes specified in \code{scalingAttrs}.
#'                      Current options for scaling methods include "mean-subtraction" and "z-score".
#' @param omit.na (character) Names of columns for which, if there are any missing values not handled in \code{missingAttrs},
#'                          the corresponding rows are excluded from the dataset.
#' @return A \code{hydrar.matrix} object as the result of the transformations. \code{hydrar.matrix}
#' @details The transformed dataset will be returned as a \code{hydrar.matrix} object.
#'                      The transform meta-info is also returned. 
#'                      of the resulting \code{hydrar.matrix}. This is helpful to keep track of which transformations
#'                      were performed as well as to apply the same set of transformations to a different dataset.
#'           
#'                      
#'                      The structure of the transform metadata folder stored on \code{transformPath} is as follows:
#'  \tabular{rlll}{
#'                      \tab\emph{/transform.spec.json}\tab global metadata of the transformations \cr
#'                      \tab\emph{/Recode}\tab recoding metadata \cr
#'                      \tab\emph{/Impute}\tab missing value imputation metadata\cr
#'                      \tab\emph{/Dummycode.csv}\tab dummy-code metadata\cr
#'                      \tab\emph{/Bin}\tab binning metadata\cr
#'                      \tab\emph{/Scale}\tab scaling metadata\cr
#'  }
# @TODO implement the na find func @seealso link{hydrar.which.na.cols}
#' @export
#' @examples \dontrun{
#' 
#' # Load the Iris dataset to the cluster
#' irisBF <- as.hydrar.frame(iris)
#' 
#' # Find out which columns have NA values
#' #TBD hydrar.which.na.cols(irisBF)
#' 
#' # Create a hydrar.matrix object from the dataset. Column Species will be automatically recoded.
#' # as it is of type character. Since the dataset doesn't have missing values, no further
#' # action is required.
#' irisBM <- hydrar.ml.preprocess(irisBF, transformPath = "/tmp")
#' 
#' 
#' # Create a copy of the Iris dataset and introduce some NA values in it
#' iris2 <- iris
#' iris2[1, 2] <- NA
#' iris2[10, 3] <- NA
#' iris2[100, 4] <- NA
#' 
#' # Upload the modified version of Iris to HDFS
#' irisBF <- as.hydrar.frame(iris2)
#' 
#' # Find out which columns have NA values
#' # hydrar.which.na.cols(irisBF) #TODO implement this function
#' 
#' # Create a hydrar.matrix after dummycoding, binning, and scaling some attributes. Missing values
#' # must be handled accordingly.
#' irisBM <- hydrar.ml.preprocess(irisBF, transformPath = "/tmp",
#'             dummycodeAttrs = "Species",
#'             binningAttrs = c("Sepal_Length", "Sepal_Width"),
#'             numBins = 4,
#'             missingAttrs = c("Petal_Length", "Sepal_Width"),
#'             omit.na = "Petal_Width",
#'             scalingAttrs = c("Petal_Length")
#'  )
#'                                                                            
#' # Apply existing transformations to a new dataset
#' irisbf2 <- as.hydrar.frame(iris)
#' irisBM2 <- hydrar.ml.preprocess(irisbf2, applyTransformPath = "/tmp", 
#'                           transformPath = "/tmp")
#' }
hydrar.ml.preprocess <- function(
  hf,
  transformPath,
  applyTransformPath = NULL, 
  recodeAttrs = NULL,
  missingAttrs = NULL, 
  imputationMethod = "global_mean",
  imputationValues = list(),
  binningAttrs = NULL, 
  numBins = NULL,
  binningMethod = "equi-width",
  dummycodeAttrs = NULL,
  scalingAttrs = NULL, 
  scalingMethod = "mean-subtraction",
  omit.na = NULL)
{
  logSource <- "hydrar.ml.preprocess"
 .hydrar.transform.argumentsPreconditions(
    hf, 
    transformPath,
    applyTransformPath, 
    recodeAttrs,
    missingAttrs, 
    imputationMethod,
    imputationValues,
    binningAttrs, 
    numBins,
    binningMethod,
    dummycodeAttrs,
    scalingAttrs, 
    scalingMethod,
    omit.na)
  
  if (!is.null(recodeAttrs) && (recodeAttrs == "")) recodeAttrs <- NULL
  if (!is.null(missingAttrs) && (missingAttrs == "")) missingAttrs <- NULL
  if (!is.null(binningAttrs) && (binningAttrs == "")) binningAttrs <- NULL
  if (!is.null(dummycodeAttrs) && (dummycodeAttrs == "")) dummycodeAttrs <- NULL
  if (!is.null(scalingAttrs) && (scalingAttrs == "")) scalingAttrs <- NULL
  
  characterCols <- SparkR::colnames(hf)[SparkR::coltypes(hf) %in% c("character", "logical")]
  recodeAttrs <- c(recodeAttrs, characterCols[!(characterCols %in% recodeAttrs)])
  
  if (length(imputationMethod) == 1 && length(missingAttrs) > 1) {
    imputationMethod <- rep(imputationMethod, length(missingAttrs))
  }
  
  
  .hydrar.transform.validateTransformOptions(
    hf,
    transformPath,
    recodeAttrs,
    missingAttrs,
    imputationMethod,
    imputationValues,
    binningAttrs,
    numBins,
    binningMethod,
    dummycodeAttrs,
    scalingAttrs,
    scalingMethod,
    omit.na)
  
  #if (is.null(omit.na)) {
  #    omit.na <- setdiff (colnames(hf), missingAttrs)
  #}
  
  # @TODO create the meta for output
  #writeBigFrameMtdFile(bf)
  #colnamesFilePath <- writeColnames(hf)
  
  # @TODO ALOK comment this out and we will have it in future, when we have applyTransform
  # if (!is.null(applyTransformPath) && (applyTransformPath!="")) {
  #  return(hydrar.apply.transform(hf, outData, transformPath, applyTransformPath))
  #}
  
  # @TODO BEGIN dml_transform
  # in future, we might use the dml transform or may be not. The decision is open yet
  # transformSpecPath <- writeTransformSpec(
  #                       hf,
  #                       transformPath,
  #                       recodeAttrs,
  #                       missingAttrs, 
  #                       imputationMethod,
  #                       imputationValues,
  #                       binningAttrs, 
  #                       numBins,
  #                       binningMethod, 
  #                       dummycodeAttrs, 
  #                       scalingAttrs, 
  #                       scalingMethod,
  #                       omit.na)
  
  
  
  #tryCatch(sysml("transform.dml",
  #                      DATA_PATH=bf@dataPath,
  #                      TRANSFORM_PATH=transformPath,
  #                      TRANSFORM_SPEC_PATH=transformSpecPath,
  #                      OUTPUT_DATA_PATH=outData,
  #                      OUTPUT_NAMES=colnamesFilePath,
  #                      FMT=outDataFormat),
  #         error = function(e) )
  # END dml_transform
 
  #create a hydrar matrix 
  # here is the order of application of the code
  # 1. we apply the na omit
  # 2. we apply the na imputation
  # 3. we apply the scaling and centering
  # 4. we apply binning
  # 5. we apply the recode
  # 6. we apply one hot encoding
  #1.
  
  
  is.omit.na <- !missing(omit.na)
  
  proxy.omit.na <- function(hf) {
    rhf = hf
    if (is.omit.na) {
      rhf <- as.hydrar.frame(SparkR::dropna(hf), repartition = FALSE)
    }
    metadata <- list();
    list(data=rhf, metadata=metadata)
  }
  #2.
  proxy.impute <- function(hf) {
    rhf <- hf
    rmd <- NULL
    if (length(missingAttrs) > 0) {
      iargs <- list()
      #iargs <- hf1
      for (idx in 1:length(missingAttrs)) {
        missingAttr <- missingAttrs[[idx]]
        method <- imputationMethod[[idx]]
        if (method == "global_mean") {
          iargs[[missingAttr]] <- "mean"
        } else if (method == "constant") {
          iargs[[missingAttr]] <- imputationValues[[missingAttr]]
        } else {
          stop("unknown method")
        }
      }
      hf_info <- do.call("hydrar.impute", list(hf, iargs))
      #hf_info <- hydrar.impute(unlist(iargs))
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- hf
      rmd <- NULL
    }
    list(data=rhf, metadata=rmd)
  }
  #3.
  #hf3 = hydrar.scale(hf2$data)
  proxy.normalize <- function(hf) {
    if (length(scalingAttrs) > 0) {
      hf_info <- hydrar.normalize(hf, scalingAttrs)
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- hf
      rmd <- NULL
    }
    list(data=rhf, metadata=rmd)
  }
  #4.
  proxy.binning <- function(hf) {
    if (length(binningAttrs) > 0) {
      hf_info <- hydrar.binning(hf, binningAttrs, numBins)
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- hf
      rmd <- NULL
    }
    list(data=rhf, metadata=rmd)
  }
  #5.
  proxy.recode <- function(hf) {
    if (length(recodeAttrs) > 0) {
      rargs <- list(hf, recodeAttrs)
      hf_info <- hydrar.recode(hf, recodeAttrs)
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- hf
      rmd <- NULL
    }
    list(data=rhf, metadata=rmd)
  }
  #6.
  proxy.onehot <- function(hf) {
    if (length(dummycodeAttrs) > 0) {
      # now by this time everything should be convertable to 
      # matrix
      hf_m <- as.hydrar.matrix(hf)
      hf_info <- hydrar.onehot(hf_m, dummycodeAttrs)
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- hf
      rmd <- NULL
    }  
    list(data=rhf, metadata=rmd)
  }
  
  #this order can be changed in future depending on the user input
  # for now, we will follow the following order.
  pp_order <- c("proxy.omit.na", "proxy.impute", "proxy.normalize",
                "proxy.binning", "proxy.recode", "proxy.onehot")
  
  curr_frame <- hf
  next_frame <- curr_frame
  metadata = list(order=pp_order)
  for (pp in pp_order) {
    curr_frame <- next_frame
    pp_res_info <- do.call(pp, list(curr_frame))
    next_frame <- pp_res_info$data
    metadata[[pp]] <- pp_res_info$metadata
  }
  res_hm <- next_frame
  res_md <- metadata
  res <- list(data=res_hm, metadata=res_md)
  return(res)
}



.hydrar.transform.argumentsPreconditions <- function(
  hf, 
  transformPath,
  applyTransformPath, 
  recodeAttrs,
  missingAttrs, 
  imputationMethod,
  imputationValues,
  binningAttrs, 
  numBins,
  binningMethod,
  dummycodeAttrs,
  scalingAttrs, 
  scalingMethod,
  omit.na) {
  logSource <- "argumentsPreconditions"
  if (!is.null(applyTransformPath) && (applyTransformPath!="")) {
    otherParamsSpecified <- !is.null(recodeAttrs) && recodeAttrs!=""
    otherParamsSpecified <- otherParamsSpecified || !is.null(dummycodeAttrs) && dummycodeAttrs!=""
    otherParamsSpecified <- otherParamsSpecified || !is.null(scalingAttrs) && scalingAttrs!=""
    otherParamsSpecified <- otherParamsSpecified || !is.null(binningAttrs) && binningAttrs!=""
    otherParamsSpecified <- otherParamsSpecified || !is.null(missingAttrs) && missingAttrs!=""
    if (otherParamsSpecified) {
      hydrar.err(logSource, "Transform parameters (recodeAttrs, dummycodeAttrs, scalingAttrs, binningAttrs, missingAttrs)
                      must not be specified when applyTransformPath is provided.")
    }
  }
  .hydrar.checkParameter(logSource, hf, "hydrar.frame")
  .hydrar.checkParameter(logSource, transformPath, "character", checkExistence=T, expectedExistence=F)
  .hydrar.checkParameter(logSource, applyTransformPath, "character", isNullOK=T, isOptional=T, checkExistence=T, expectedExistence=T)
  .hydrar.checkParameter(logSource, recodeAttrs, "character", isNullOK=T, isOptional=T, isSingleton=F)
  .hydrar.checkParameter(logSource, missingAttrs, "character", isNullOK=T, isOptional=T, isSingleton=F)
  .hydrar.checkParameter(logSource, imputationMethod, "character", c("global_mean", "constant"), isNullOK=T, isOptional=T)
  .hydrar.checkParameter(logSource, imputationValues, "list", , isNullOK=F, isOptional=T)
  .hydrar.checkParameter(logSource, numBins, c("integer", "numeric"), isNullOK=T, isOptional=T, isSingleton=F)
  .hydrar.checkParameter(logSource, binningMethod, "character", c("equi-width"), isNullOK=T, isOptional=T)
  .hydrar.checkParameter(logSource, dummycodeAttrs, "character", isNullOK=T, isOptional=T, isSingleton=F)
  .hydrar.checkParameter(logSource, scalingAttrs, "character", isNullOK=T, isOptional=T, isSingleton=F)
  .hydrar.checkParameter(logSource, scalingMethod, "character", c("mean-subtraction", "z-score"), isNullOK=T, isOptional=T, isSingleton=F)
  .hydrar.checkParameter(logSource, omit.na, "character", isNullOK=T, isOptional=T, isSingleton=F)
  
  
  if (transformPath == "") {
    hydrar.err(logSource, "Parameter 'transformPath' is not a valid path.")
  }
  
  if (!is.null(applyTransformPath) && (applyTransformPath == "")) applyTransformPath <- NULL
  # @TODO ALOK in the future
  # if (!is.null(applyTransformPath) && (.hydrar.isFileOrDirectory(applyTransformPath) != "DIRECTORY")) {
  #  hydrar.err(logSource, "Parameter 'applyTransformPath' is not a valid path.")
  # }
}

.hydrar.transform.validateTransformOptions <- function(
  hf, 
  transformPath, 
  recodeAttrs,
  missingAttrs, 
  imputationMethod,
  imputationValues,
  binningAttrs, 
  numBins,
  binningMethod, 
  dummycodeAttrs, 
  scalingAttrs, 
  scalingMethod,
  omit.na) {
  
  logSource <- "validateTransformOptions"
  columnNames <- SparkR::colnames(hf)
  if (!is.null(recodeAttrs) && any(is.na(match(recodeAttrs, columnNames)))) {
    hydrar.err(logSource, "One or more columns specified in recodeAttrs do not exist in the dataset. [" %++%
               paste(recodeAttrs, sep=",") %++% "].")
  }
  if (!is.null(missingAttrs) ) {
    if (any(is.na(match(missingAttrs, columnNames)))) {
      hydrar.err(logSource, "One or more columns specified in missingAttrs do not exist in the dataset.")
    }
    missingAttrsIndex <- 1
    for (missingAttr in missingAttrs) {
      coltype <- SparkR::coltypes(hf)[SparkR::colnames(hf) %in% missingAttr]
      if (!coltype  %in% c("integer", "double", "numeric")) {
        if(!any(imputationMethod[missingAttrs %in% missingAttr] == c("constant"))){
          hydrar.err(logSource, "Only 'constant' imputation method is supported on non-numeric columns [" %++% missingAttr %++% "].")
        }
      }
      if (imputationMethod[missingAttrsIndex] == "constant" && length(imputationValues) != 0) {
        number_class = c("integer", "double", "numeric")
        if (coltype  %in% number_class) {
          if (!class(imputationValues[missingAttr][[1]]) %in% number_class) {
            hydrar.err (logSource, "The class of the imputationValues must match the column type.")
          }
        } else {
          if (coltype != class(imputationValues[missingAttr][[1]]) ) {
            hydrar.err (logSource, "The class of the imputationValues must match the column type.")
          }
        }
      }
      missingAttrsIndex <- missingAttrsIndex + 1
    }
  }
  
  if (!is.null(omit.na) ) {
    if (any(is.na(match(omit.na, columnNames)))) {
      hydrar.err(logSource, "One or more columns specified in omit.na do not exist in the dataset.")
    }
    if (!is.null(missingAttrs) ) {
      if (any(!is.na(match(omit.na, missingAttrs)))) {
        hydrar.err(logSource, "omit.na and missingAttrs should have an empty intersection.")
      }
    }
  }
  
  if (!is.null(binningAttrs)) {
    if (!is.null(numBins)) {
      rows <- nrow(hf)
      if (rows < max(numBins)) {
        hydrar.err(logSource, "Number of bins is larger than the number of rows in the dataset [" %++% rows %++% "].")
      }
    }
    if (any(is.na(match(binningAttrs, columnNames)))) {
      hydrar.err(logSource, "One or more columns specified in binningAttrs do not exist in the dataset.")
    }
  }
  if (!is.null(dummycodeAttrs)) {
    if(any(is.na(match(dummycodeAttrs, columnNames)))) {
      hydrar.err(logSource, "One or more columns specified in dummycodeAttrs do not exist in the dataset.")
    }
    
    #         if(isAnyAttrOfType(hm = hf, attrs = dummycodeAttrs, type = "numeric")) {
    #             hydrar.err(logSource, "Numeric attributes are not allowed to be dummy coded.")
    #         }
  }
  if (!is.null(scalingAttrs)) {
    if (any(is.na(match(scalingAttrs, columnNames)))) {
      hydrar.err(logSource, "One or more columns specified in scalingAttrs do not exist in the dataset.")
    }
    if (missing(scalingMethod)) {
      scalingMethod <- rep("mean-subtraction", length(scalingAttrs))
    }
    if (length(scalingAttrs) != length(scalingMethod)) {
      if (length(scalingMethod) == 1) {
        scalingMethod <- rep(scalingMethod, length(scalingAttrs))
      }
      else {
        hydrar.err(logSource, "The number of scaling columns does not match the length of scalingMethod.")
      }
    }
  
    if(isAnyAttrOfType(hf, scalingAttrs, "character")) {
      hydrar.err(logSource, "Scaling can not be performed on nominal columns.")
    }
    if (!is.null(recodeAttrs)) {
      if (any(scalingAttrs %in% recodeAttrs)) {
        hydrar.err(logSource, "recoded attributes can not be scaled.")
      }
    }
  }
  if (!is.null(binningAttrs)) {
    if (missing(numBins)) {
      hydrar.err(logSource, "numBins must be specified for each attribute in binningAttrs.")
    }
    if (length(numBins) != length(binningAttrs)) {
      if (length(numBins) == 1) {
        numBins <- rep(numBins, length(binningAttrs))
      }
      else {
        hydrar.err(logSource, "The number of binning columns does not match the length of numBins.")
      }
    }
  }
  
  if (!is.null(numBins)) {
    if (any(numBins < 2)) {
      hydrar.err(logSource, "The number of bins for binning columns must be greater than or equal to 2.")
    }
    if (any(ceiling(numBins) != floor(numBins))) {
      hydrar.err(logSource, "One or more values in the numBins is invalid, expect positive integers.")
    }
  }
  
  # generate the vector of recode columns
  columnTypes <- SparkR::coltypes(hf)
  strbools <- (columnTypes == "character" | columnTypes == "logical")
  seedList <- columnNames[strbools]     
  rcdList <- union(seedList, recodeAttrs)
  
  # the intersection between binningAttrs and rcdList should be empty
  if (!is.null(rcdList) && !is.null(binningAttrs) && (length(intersect(rcdList, binningAttrs)) != 0)) {
    hydrar.err(logSource, "The intersection between binningAttrs and the columns to be recoded should be empty.")
  }
  
  # dummy code column is either a recoded or binning column
  binningOrRecodeList <- union(binningAttrs, rcdList)
  if (!is.null(dummycodeAttrs) && any(is.na(match(dummycodeAttrs, binningOrRecodeList)))) {
    hydrar.err(logSource, "One or more columns specified in dummycodeAttrs are not part of the recoded or binning column list.")
  }
  
  # missing imputation can't be done on a recoded column
  #     if (!is.null(rcdList) && !is.null(missingAttrs) && (length(intersect(rcdList, missingAttrs)) != 0)) {
  #         hydrar.err(logSource, "The intersection between missingAttrs and the columns to be recoded should be empty.")
  #     }
  
  # a column can not be both binning and scaling
  if (!is.null(binningAttrs) && !is.null(scalingAttrs) && (length(intersect(binningAttrs, scalingAttrs) != 0))) {
    hydrar.err(logSource, "The intersection between binningAttrs and scalingAttrs should be empty.")
  }
  list("rcdList" = rcdList)
}

isAnyAttrOfType <- function(hm, attrs, type) {
  logSource <- "columnTypes"
  allColumnNames <- SparkR::colnames(hm)
  allColumnTypes <- SparkR::coltypes(hm)
  return(any(allColumnTypes[allColumnNames %in% attrs] == type))
}

