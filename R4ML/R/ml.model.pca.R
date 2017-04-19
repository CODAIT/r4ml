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
# This class represents a Principal Component Analysis model
setClass(
  "r4ml.pca",
  representation(
    k = "numeric",
    eigen.vectors = "data.frame",
    std.deviations = "data.frame",
    eigen.values = "data.frame"
  ),
  contains = "r4ml.model"
)

# @TODO To implement fraction of retained variance
# @TODO Implement predict and summary functions

#' @name r4ml.pca
#' @title Principal Component Analysis (PCA)
#' @description Builds a Principal Component Analysis (PCA) model on a given r4ml.matrix, allows one to project
#' a given dataset into the new feature space.
#' @details r4ml.pca() computes the eigen vectors, eigen values, and square roots of
#' the eigen values on the given dataset.
#' @param data (r4ml.matrix):  The dataset which PCA will be applied to
#' @param k (integer):  The number of dimensions in the new feature space
#' @param center (logical):  A boolean value indicating whether all columns in the data should be centered to have zero mean.
#' Default is set to FALSE.
#' @param scale (logical):  A boolean value indicating whether all columns in the data should be scaled to have variance one.
#' Default is set to FALSE.
#' @param projData (logical):  A boolean value indicating whether the data should be projected in the new feature space. If
#' set to TRUE, a list (r4ml.pca model, r4ml.matrix projData) is returned. If set to FALSE, only the r4ml.pca model is returned.
#' Default value for \code{projData} is TRUE.
#' @param applyPCA (r4ml.pca):  (optional) Currently is not supported. In future this option will import an existing r4ml.pca model. If provided, \code{data} will be projected to the
#' new feature space given the existing eigen vectors. In this case, a new r4ml.matrix will be returned.
#' @export
#' @return r4ml.pca S4 object is returned if projData is set to FALSE:\cr
#' An r4ml.pca S4 object contains the following additional fields:
#'
##' \tabular{rlll}{
##'\tab\code{eigen.vectors}       \tab (data.frame) \tab The eigen vectors. \cr
##'\tab\code{std.deviations}     \tab (data.frame) \tab The square root of the eigen values.\cr
##'\tab\code{eigen.values}     \tab (data.frame) \tab The eigen values.\cr
##'\tab\code{call}          \tab (character) \tab String representation of this method's call,
##'including the parameters and values passed to it.\cr
##'}
#'
#' If projData is set to TRUE a list is returned containing (1) an r4ml.pca model and (2) an r4ml.matrix with the new data
## @TODO add  @seealso link{r4ml.transform}
#'
#' @examples \dontrun{
#'
#' # Load the Iris dataset
#' train <- as.r4ml.matrix(iris[, -5])
#'
#' # Create an r4ml.pca model on Iris and projected data. Then display the new feature set(ie the principal components),
#' # eigen values, eigen vectors, standard deviations (i.e., the square roots of the eigen values)
#' iris_pca <- r4ml.pca(train, center = TRUE, scale = TRUE, k = 2)
#' show(iris_pca$model)
#'
#' # projData set to False: Only the eigen values, eigen vectors and standard deviations are returned.
#' iris_pca <- r4ml.pca(train, center = TRUE, scale = TRUE, k = 2, projData = FALSE)
#' show(iris_pca)
#'
#' }
#'
r4ml.pca <- function(data, k, center, scale, projData, applyPCA) {
  pca <- new(
    "r4ml.pca",
    modelType = "feature-extraction",
    data = data,
    k = k,
    center = center,
    scale = scale,
    projData = projData,
    applyPCA = applyPCA
  )
  
  # If projData is true, return a list containing the model and the projected data
  # Else return only the model
  if (pca@dmlArgs$PROJDATA) {
    return (list(
      model = pca,
      "projData" = pca@dmlOuts$sysml.execute$newA
    ))
    
  } else {
    return(pca)
  }
  
  
  # @TODO Support applyPCA for future. applyPCA allows import of an existing PCA model
  # @NOTE cannot use read.csv since it is not scalable
  # If an existing model is provided, one should only project in the new feature space
  # and return an r4ml.matrix with the new dataset
  
  
}

# overloaded method which checks the training parameters of the pca model
setMethod(
  "r4ml.model.validateTrainingParameters",
  signature = "r4ml.pca",
  definition =
    function(model, args) {
      logSource <- "r4ml.model.validateTrainingParameters.r4ml.pca"
      with(args, {
        .r4ml.checkParameter(logSource, center, "logical", c(T, F), isOptional =
                                 T)
        .r4ml.checkParameter(logSource, scale, "logical", c(T, F), isOptional =
                                 T)
        .r4ml.checkParameter(logSource, k, c("numeric", "integer"), isOptional =
                                 T)
        .r4ml.checkParameter(logSource, projData, "logical", c(T, F), isOptional =
                                 T)
        .r4ml.checkParameter(logSource, applyPCA, "r4ml.pca", isOptional =
                                 T)
        
        if (!missing(k)) {
          if (k < 1) {
            r4ml.err(logSource,
                       "Parameter k must be a positive integer number.")
          }
          if (k >= SparkR::ncol(data)) {
            r4ml.err(logSource,
                       "Parameter k must be less than the original number of features.")
          }
        }
      })
      return(model)
    }
)

# Overwrite the base model's method to build the traning args which will be
# passed to the dml script to run
setMethod(
  "r4ml.model.buildTrainingArgs",
  signature = "r4ml.pca",
  definition =
    function(model, args) {
      logSource <- "r4ml.model.buildTrainingArgs.r4ml.pca"
      dmlArgs <- list()
      dmlPath <-
        file.path(r4ml.env$SYSML_ALGO_ROOT(), r4ml.env$DML_PCA_SCRIPT)
      with(args,  {
        dmlArgs <- list(
          dml = dmlPath,
          "eval_stdev_dominant",
          "eval_dominant",
          "evec_dominant",
          INPUT = data,
          OFMT = "csv"
        )
        
        if (!missing(k)) {
          dmlArgs <- c(dmlArgs, K = k)
        }
        if (!missing(center)) {
          dmlArgs <- c(dmlArgs, CENTER = ifelse(center, 1, 0))
        }
        if (!missing(scale)) {
          dmlArgs <- c(dmlArgs, SCALE = ifelse(scale, 1, 0))
        }
        if (!missing(applyPCA)) {
          # @TODO To add support for applyPCA
          #dmlArgs <- c(dmlArgs, MODEL=applyPCA@modelPath)
        }
        
        if (missing(projData)) {
          projData = 1
        }
        
        if (projData) {
          dmlArgs <- c(dmlArgs, PROJDATA = 1)
          dmlArgs <- c(dmlArgs, "newA")
        } else {
          dmlArgs <- c(dmlArgs, PROJDATA = 0)
        }
        
        model@dmlArgs <- dmlArgs
        return(model)
      })
    }
)

# overwrite the base model's post training function so that one can
# post process the final outputs from the dml scripts
setMethod(
  "r4ml.model.postTraining",
  signature = "r4ml.pca",
  definition =
    function(model) {
      outputs <- model@dmlOuts$sysml.execute
      
      #Converting r4ml to data.frame for consumption
      model@std.deviations <-
        SparkR::as.data.frame(outputs$eval_stdev_dominant)
      colnames(model@std.deviations) <- "StdDev"
      
      model@eigen.vectors <-
        SparkR::as.data.frame(outputs$evec_dominant)
      colnames(model@eigen.vectors) <-
        "PC" %++% seq(1:SparkR::ncol(model@eigen.vectors))
      rownames(model@eigen.vectors) <- model@featureNames
      
      model@eigen.values <-
        SparkR::as.data.frame(outputs$eval_dominant)
      colnames(model@eigen.values) <-
        "EigenValues" %++% seq(1:SparkR::ncol(model@eigen.values))
      
      
      return(model)
    }
)


setMethod(
  f = "show",
  signature = "r4ml.pca",
  definition =
    function(object) {
      logSource <- "show.r4ml.pca"
      callNextMethod()
      
      # Add model specific values to be displayed
      
      cat("\n Standard Deviations : \n")
      print(object@std.deviations)
      
      cat("\n Rotation:\n")
      print(object@eigen.vectors)
      
    }
)
