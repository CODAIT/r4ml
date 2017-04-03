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


# Method to transform a r4ml.frame to a r4ml.matrix that can be read by DML 
#'
#' @name r4ml.ml.preprocess
#' @title Data preparation for statistical analysis and machine learning algorithms
#' @description Performs various data preparation operations on a \code{r4ml.frame} and produces a \code{r4ml.matrix}.
#'              Five operations are supported:
#'              
#'              1. \emph{Imputation of missing values}: Missing values (i.e., NA's in the input \code{r4ml.frame})
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
#'              used to perform the same set of transformations on another \code{r4ml.frame}, through parameter \code{applyTransformPath}.
#' @param data (r4ml.frame) The data to be transformed.
#' @param transformPath (character) Path on HDFS where the transform metadata (e.g., recode maps, dummy-code maps, etc.) will be stored.
#' @param applyTransformPath (character) Path of an existing transform metadata folder. 
#'                                       If specified, \code{r4ml.ml.preprocess()} will apply the same transformations to \code{bf}.
#'                                       Transform metadata for the resulting r4ml.matrix will be copied to the location specified in \code{transformPath}.
#'                                       Parameter \code{applyTransformPath} is optional.
#' @param recodeAttrs (character) Names of attributes to be recoded. All columns of type 'character' will be recoded, even if they do not
#'                                appear in \code{recodeAttrs}.
#' @param missingAttrs (character) Names of of attributes containing missing values to be filled in using an imputation method.
#'                                 \strong{Note}: 1) Function \code{r4ml.which.na.cols()} can be run to find out which columns contain missing values.
#'                                       2) If no imputation is done on a column with missing values, and such column is not part of \code{omit.na}, r4ml.ml.preprocess 
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
#' @param imputationValues (list) A List of values to be used as replacement values for the columns to be imputed using the "constant" imputation method.                        
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
#' @return A \code{r4ml.matrix} object as the result of the transformations. \code{r4ml.matrix}
#' @details The transformed dataset will be returned as a \code{r4ml.matrix} object.
#'                      The transform meta-info is also returned. 
#'                      of the resulting \code{r4ml.matrix}. This is helpful to keep track of which transformations
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
#' @export
#' @examples \dontrun{
#' 
#' # Load the Iris dataset to the cluster
#' irisBF <- as.r4ml.frame(iris)
#' 
#' # Find out which columns have NA values
#' r4ml.which.na.cols(irisBF)
#' 
#' # Create a r4ml.matrix object from the dataset. Column Species will be automatically recoded.
#' # as it is of type character. Since the dataset doesn't have missing values, no further
#' # action is required.
#' irisBM <- r4ml.ml.preprocess(irisBF, transformPath = "/tmp")
#' 
#' 
#' # Create a copy of the Iris dataset and introduce some NA values in it
#' iris2 <- iris
#' iris2[1, 2] <- NA
#' iris2[10, 3] <- NA
#' iris2[100, 4] <- NA
#' 
#' # Upload the modified version of Iris to HDFS
#' irisBF <- as.r4ml.frame(iris2)
#' 
#' # Find out which columns have NA values
#' r4ml.which.na.cols(irisBF)
#' 
#' # Create a r4ml.matrix after dummycoding, binning, and scaling some attributes. Missing values
#' # must be handled accordingly.
#' irisBM <- r4ml.ml.preprocess(irisBF, transformPath = "/tmp",
#'             dummycodeAttrs = "Species",
#'             binningAttrs = c("Sepal_Length", "Sepal_Width"),
#'             numBins = 4,
#'             missingAttrs = c("Petal_Length", "Sepal_Width"),
#'             omit.na = c("Petal_Width"),
#'             scalingAttrs = c("Petal_Length")
#'  )
#'                                                                            
#' # Apply existing transformations to a new dataset
#' irisbf2 <- as.r4ml.frame(iris)
#' irisBM2 <- r4ml.ml.preprocess(irisbf2, applyTransformPath = "/tmp", 
#'                           transformPath = "/tmp")
#' }
r4ml.ml.preprocess <- function(
  data,
  transformPath = NULL,
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
  omit.na = SparkR::colnames(data))
{
  logSource <- "r4ml.ml.preprocess"
 .r4ml.transform.argumentsPreconditions(
    data, 
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
  
  characterCols <- SparkR::colnames(data)[SparkR::coltypes(data) %in% c("character", "logical")]
  recodeAttrs <- c(recodeAttrs, characterCols[!(characterCols %in% recodeAttrs)])
  
  if (length(imputationMethod) == 1 && length(missingAttrs) > 1) {
    imputationMethod <- rep(imputationMethod, length(missingAttrs))
  }
  
  
  .r4ml.transform.validateTransformOptions(
    data,
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
  #    omit.na <- setdiff (colnames(data), missingAttrs)
  #}
  
  # @TODO create the meta for output
  #writeBigFrameMtdFile(bf)
  #colnamesFilePath <- writeColnames(data)
  
  # @TODO ALOK comment this out and we will have it in future, when we have applyTransform
  # if (!is.null(applyTransformPath) && (applyTransformPath!="")) {
  #  return(r4ml.apply.transform(data, outData, transformPath, applyTransformPath))
  #}
  
  # @TODO BEGIN dml_transform
  # in future, we might use the dml transform or may be not. The decision is open yet
  # transformSpecPath <- writeTransformSpec(
  #                       data,
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
 
  #create a r4ml matrix 
  # here is the order of application of the code
  # 1. we apply the na omit
  # 2. we apply the na imputation
  # 3. we apply the scaling and centering
  # 4. we apply binning
  # 5. we apply the recode
  # 6. we apply one hot encoding
  #1.
  
  
  is.omit.na <- !missing(omit.na)
  
  proxy.omit.na <- function(data) {
    rhf = data
    if (is.omit.na) {
      rhf <- as.r4ml.frame(SparkR::dropna(data, cols = omit.na), repartition = FALSE)
      ignore <- cache(rhf)
    }
    metadata <- list()
    list(data=rhf, metadata=metadata)
  }
  #2.
  proxy.impute <- function(data) {
    logSource <- "proxy.impute"
    rhf <- data
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
          r4ml.err(logSource, "unknown method")
        }
      }
      hf_info <- do.call("r4ml.impute", list(data, iargs))
      #hf_info <- r4ml.impute(unlist(iargs))
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- data
      rmd <- NULL
    }
    list(data=rhf, metadata=rmd)
  }
  #3.
  #hf3 = r4ml.scale(hf2$data)
  proxy.normalize <- function(data) {
    if (length(scalingAttrs) > 0) {
      hf_info <- r4ml.normalize(data, scalingAttrs)
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- data
      rmd <- NULL
    }
    list(data=rhf, metadata=rmd)
  }
  #4.
  proxy.binning <- function(data) {
    if (length(binningAttrs) > 0) {
      hf_info <- r4ml.binning(data, binningAttrs, numBins)
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- data
      rmd <- NULL
    }
    list(data=rhf, metadata=rmd)
  }
  #5.
  proxy.recode <- function(data) {
    if (length(recodeAttrs) > 0) {
      rargs <- list(data, recodeAttrs)
      hf_info <- r4ml.recode(data, recodeAttrs)
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- data
      rmd <- NULL
    }
    list(data=rhf, metadata=rmd)
  }
  #6.
  proxy.onehot <- function(data) {
    if (length(dummycodeAttrs) > 0) {
      # now by this time everything should be convertable to 
      # matrix
      hf_m <- as.r4ml.matrix(data)
      hf_info <- r4ml.onehot(hf_m, dummycodeAttrs)
      rhf <- hf_info$data
      rmd <- hf_info$metadata
    } else {
      rhf <- data
      rmd <- NULL
    }  
    list(data=rhf, metadata=rmd)
  }
  
  #this order can be changed in future depending on the user input
  # for now, we will follow the following order.
  pp_order <- c("proxy.omit.na", "proxy.impute", "proxy.normalize",
                "proxy.binning", "proxy.recode", "proxy.onehot")
  
  curr_frame <- data
  next_frame <- curr_frame
  metadata = list(order=pp_order)
  for (pp in pp_order) {
    r4ml.info(logSource, paste("running", pp))
    curr_frame <- next_frame
    pp_res_info <- do.call(pp, list(curr_frame))
    next_frame <- pp_res_info$data
    metadata[[pp]] <- pp_res_info$metadata
  }
  res_hm <- as.r4ml.frame(next_frame)
  res_md <- metadata
  res <- list(data=res_hm, metadata=res_md)
  return(res)
}



.r4ml.transform.argumentsPreconditions <- function(
  data, 
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
      r4ml.err(logSource, "Transform parameters (recodeAttrs, dummycodeAttrs, scalingAttrs, binningAttrs, missingAttrs)
                      must not be specified when applyTransformPath is provided.")
    }
  }
  .r4ml.checkParameter(logSource, data, "r4ml.frame")
  .r4ml.checkParameter(logSource, transformPath, "character", isNullOK = TRUE, isOptional = TRUE)
  .r4ml.checkParameter(logSource, applyTransformPath, "character", isNullOK = TRUE, isOptional = TRUE)
  .r4ml.checkParameter(logSource, recodeAttrs, "character", isNullOK=T, isOptional=T, isSingleton=F)
  .r4ml.checkParameter(logSource, missingAttrs, "character", isNullOK=T, isOptional=T, isSingleton=F)
  .r4ml.checkParameter(logSource, imputationMethod, "character", c("global_mean", "constant"), isNullOK=T, isOptional=T)
  .r4ml.checkParameter(logSource, imputationValues, "list", , isNullOK=F, isOptional=T)
  .r4ml.checkParameter(logSource, numBins, c("integer", "numeric"), isNullOK=T, isOptional=T, isSingleton=F)
  .r4ml.checkParameter(logSource, binningMethod, "character", c("equi-width"), isNullOK=T, isOptional=T)
  .r4ml.checkParameter(logSource, dummycodeAttrs, "character", isNullOK=T, isOptional=T, isSingleton=F)
  .r4ml.checkParameter(logSource, scalingAttrs, "character", isNullOK=T, isOptional=T, isSingleton=F)
  .r4ml.checkParameter(logSource, scalingMethod, "character", c("mean-subtraction", "z-score"), isNullOK=T, isOptional=T, isSingleton=F)
  .r4ml.checkParameter(logSource, omit.na, "character", isNullOK=T, isOptional=T, isSingleton=F)
  

  #Disabling tranformPath checking for now
  r4ml.debug(logSource,"Disabling transformPath checking for now")  
  #if (transformPath == "") {
  #  r4ml.err(logSource, "Parameter 'transformPath' is not a valid path.")
  #}
  
  if (!is.null(applyTransformPath) && (applyTransformPath == "")) applyTransformPath <- NULL
  # @TODO ALOK in the future
  # if (!is.null(applyTransformPath) && (.r4ml.isFileOrDirectory(applyTransformPath) != "DIRECTORY")) {
  #  r4ml.err(logSource, "Parameter 'applyTransformPath' is not a valid path.")
  # }
}

.r4ml.transform.validateTransformOptions <- function(
  data, 
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
  columnNames <- SparkR::colnames(data)
  if (!is.null(recodeAttrs) && any(is.na(match(recodeAttrs, columnNames)))) {
    r4ml.err(logSource, "One or more columns specified in recodeAttrs do not exist in the dataset. [" %++%
               paste(recodeAttrs, sep=",") %++% "].")
  }
  if (!is.null(missingAttrs) ) {
    if (any(is.na(match(missingAttrs, columnNames)))) {
      r4ml.err(logSource, "One or more columns specified in missingAttrs do not exist in the dataset.")
    }
    missingAttrsIndex <- 1
    for (missingAttr in missingAttrs) {
      coltype <- SparkR::coltypes(data)[SparkR::colnames(data) %in% missingAttr]
      if (!coltype  %in% c("integer", "double", "numeric")) {
        if(!any(imputationMethod[missingAttrs %in% missingAttr] == c("constant"))){
          r4ml.err(logSource, "Only 'constant' imputation method is supported on non-numeric columns [" %++% missingAttr %++% "].")
        }
      }
      if (imputationMethod[missingAttrsIndex] == "constant" && length(imputationValues) != 0) {
        number_class = c("integer", "double", "numeric")
        if (coltype  %in% number_class) {
          if (!class(imputationValues[missingAttr][[1]]) %in% number_class) {
            r4ml.err (logSource, "The class of the imputationValues must match the column type.")
          }
        } else {
          if (coltype != class(imputationValues[missingAttr][[1]]) ) {
            r4ml.err (logSource, "The class of the imputationValues must match the column type.")
          }
        }
      }
      missingAttrsIndex <- missingAttrsIndex + 1
    }
  }
  
  if (!is.null(omit.na) ) {
    if (any(is.na(match(omit.na, columnNames)))) {
      r4ml.err(logSource, "One or more columns specified in omit.na do not exist in the dataset.")
    }
    if (!is.null(missingAttrs) ) {
      if (any(!is.na(match(omit.na, missingAttrs)))) {
        r4ml.err(logSource, "omit.na and missingAttrs should have an empty intersection.")
      }
    }
  }
  
  if (!is.null(binningAttrs)) {
    if (!is.null(numBins)) {
      rows <- nrow(data)
      if (rows < max(numBins)) {
        r4ml.err(logSource, "Number of bins is larger than the number of rows in the dataset [" %++% rows %++% "].")
      }
    }
    if (any(is.na(match(binningAttrs, columnNames)))) {
      r4ml.err(logSource, "One or more columns specified in binningAttrs do not exist in the dataset.")
    }
  }
  if (!is.null(dummycodeAttrs)) {
    if(any(is.na(match(dummycodeAttrs, columnNames)))) {
      r4ml.err(logSource, "One or more columns specified in dummycodeAttrs do not exist in the dataset.")
    }
    
    #         if(isAnyAttrOfType(hm = data, attrs = dummycodeAttrs, type = "numeric")) {
    #             r4ml.err(logSource, "Numeric attributes are not allowed to be dummy coded.")
    #         }
  }
  if (!is.null(scalingAttrs)) {
    if (any(is.na(match(scalingAttrs, columnNames)))) {
      r4ml.err(logSource, "One or more columns specified in scalingAttrs do not exist in the dataset.")
    }
    if (missing(scalingMethod)) {
      scalingMethod <- rep("mean-subtraction", length(scalingAttrs))
    }
    if (length(scalingAttrs) != length(scalingMethod)) {
      if (length(scalingMethod) == 1) {
        scalingMethod <- rep(scalingMethod, length(scalingAttrs))
      }
      else {
        r4ml.err(logSource, "The number of scaling columns does not match the length of scalingMethod.")
      }
    }
  
    if(isAnyAttrOfType(data, scalingAttrs, "character")) {
      r4ml.err(logSource, "Scaling can not be performed on nominal columns.")
    }
    if (!is.null(recodeAttrs)) {
      if (any(scalingAttrs %in% recodeAttrs)) {
        r4ml.err(logSource, "recoded attributes can not be scaled.")
      }
    }
  }
  if (!is.null(binningAttrs)) {
    if (missing(numBins)) {
      r4ml.err(logSource, "numBins must be specified for each attribute in binningAttrs.")
    }
    if (length(numBins) != length(binningAttrs)) {
      if (length(numBins) == 1) {
        numBins <- rep(numBins, length(binningAttrs))
      }
      else {
        r4ml.err(logSource, "The number of binning columns does not match the length of numBins.")
      }
    }
  }
  
  if (!is.null(numBins)) {
    if (any(numBins < 2)) {
      r4ml.err(logSource, "The number of bins for binning columns must be greater than or equal to 2.")
    }
    if (any(ceiling(numBins) != floor(numBins))) {
      r4ml.err(logSource, "One or more values in the numBins is invalid, expect positive integers.")
    }
  }
  
  # generate the vector of recode columns
  columnTypes <- SparkR::coltypes(data)
  strbools <- (columnTypes == "character" | columnTypes == "logical")
  seedList <- columnNames[strbools]     
  rcdList <- union(seedList, recodeAttrs)
  
  # the intersection between binningAttrs and rcdList should be empty
  if (!is.null(rcdList) && !is.null(binningAttrs) && (length(intersect(rcdList, binningAttrs)) != 0)) {
    r4ml.err(logSource, "The intersection between binningAttrs and the columns to be recoded should be empty.")
  }
  
  # dummy code column is either a recoded or binning column
  binningOrRecodeList <- union(binningAttrs, rcdList)
  if (!is.null(dummycodeAttrs) && any(is.na(match(dummycodeAttrs, binningOrRecodeList)))) {
    r4ml.err(logSource, "One or more columns specified in dummycodeAttrs are not part of the recoded or binning column list.")
  }
  
  # missing imputation can't be done on a recoded column
  #     if (!is.null(rcdList) && !is.null(missingAttrs) && (length(intersect(rcdList, missingAttrs)) != 0)) {
  #         r4ml.err(logSource, "The intersection between missingAttrs and the columns to be recoded should be empty.")
  #     }
  
  # a column can not be both binning and scaling
  if (!is.null(binningAttrs) && !is.null(scalingAttrs) && (length(intersect(binningAttrs, scalingAttrs) != 0))) {
    r4ml.err(logSource, "The intersection between binningAttrs and scalingAttrs should be empty.")
  }
  list("rcdList" = rcdList)
}

isAnyAttrOfType <- function(hm, attrs, type) {
  logSource <- "columnTypes"
  allColumnNames <- SparkR::colnames(hm)
  allColumnTypes <- SparkR::coltypes(hm)
  return(any(allColumnTypes[allColumnNames %in% attrs] == type))
}

