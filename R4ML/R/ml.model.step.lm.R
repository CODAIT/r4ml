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
#' @include zzz.R
#' @include ml.model.base.R

setClass("hydrar.step.lm",
         representation(selectedFeatureNames = "character"),
         contains = "hydrar.lm"
         )

#' @name hydrar.step.lm
#' @title Step-wise Linear Regression
#' @export
#' @description Fits a linear regression model from a hydrar.matrix in a step-wise fashion or
#'    loads an existing model from HDFS.
#'
#' @param formula (formula) A formula in the form Y ~ ., where Y is the response variable.
#'                The response variable must be of type "scale".
#' @param data (hydrar.matrix) A hydrar.matrix to be fitted.
#' @param intercept (logical) Boolean value indicating if the intercept term should be used for the regression.
#' @param shiftAndRescale (logical) Boolean value indicating if shifting and rescaling X columns to mean = 0, variance = 1 should be performed.
#' @param lambda (numeric) Regularization parameter.
#' @param directory (character) The HDFS path to save the Linear Regression model
#'  if \code{formula} and \code{data} are specified. Otherwise, an HDFS location
#'  with a previously trained model to be loaded.
#' @return An S4 object of class \code{hydrar.step.lm} which contains the arguments above as well
#' as the following additional fields:
#'  \tabular{rlll}{
##'\tab\code{coefficients}  \tab (numeric)   \tab Coefficients of the regression\cr
##'\tab\code{modelPath}     \tab (character) \tab HDFS location where the model files are stored\cr
##'\tab\code{transformPath} \tab (character) \tab HDFS location where the \code{hydrar.transform()}
##'                                               metadata are stored \cr
##'\tab\code{yIdx}          \tab (numeric)   \tab Column id of the response variable\cr
##'\tab\code{numFeatures}   \tab (numeric)   \tab The number of attributes\cr
##'\tab\code{labelColname}  \tab (character) \tab Column name of the response variable \cr
##'\tab\code{call}          \tab (character) \tab String representation of this method's call,
##'                                               including the parameters and values passed to it.\cr
##'
##' }
#'
#' @examples \dontrun{
#'
#'
#' # Convert the iris data set into a HydraR frame. Column #5 is a factor so we will ignore it for now
#' hydra_iris <- as.hydrar.frame(iris[,-5])
#' 
#' # Convert the HydraR frame into a HydraR matrix
#' hydra_iris <- as.hydrar.matrix(hydra_iris)
#'
#'
#'step_lm <- hydrar.step.lm(Sepal_Length ~ . , data = hydra_iris)
#'
#' }
#'
#' @seealso \link{predict.hydrar.lm}
#'


hydrar.step.lm <- function(formula, data, intercept=FALSE, shiftAndRescale=FALSE, threshold=.001, directory="~/"){
  
  new("hydrar.step.lm", modelType="regression",
      formula=formula, data=data, method="direct-solve", intercept=intercept, shiftAndRescale=shiftAndRescale,
      lambda=0, directory=directory, tolerance=hydrar.emptySymbol(), iter.max=hydrar.emptySymbol(), lambda=0, threshold=threshold)
}

setMethod("hydrar.model.validateTrainingParameters", signature="hydrar.step.lm", def = 
  function(model, args) {
    logSource <- "hydrar.model.validateTrainingParameters"
      callNextMethod()
        with(args, {
          .hydrar.checkParameter(logSource, threshold, c("numeric", "integer"), isOptional=TRUE)
        })

        return(model)
    }
  )

setMethod(f = "show", signature = "hydrar.step.lm", definition = 
  function(object) {
    logSource <- "hydrar.step.lm.show"
    callNextMethod()
    
    cat("\nFeature selection order: \n")
    print(object@selectedFeatureNames)
  }
)

setMethod("hydrar.model.buildTrainingArgs", signature="hydrar.step.lm", def = 
  function(model, args) {
    with(args,{
      model@method <- method
      model@intercept <- intercept
      model@shiftAndRescale <- shiftAndRescale

      if (args$intercept == TRUE) {
        model@featureNames <- c("(intercept)", model@featureNames)
      }

      dmlArgs <- list(
        dml = file.path(hydrar.env$SYSML_ALGO_ROOT(), "StepLinearRegDS.dml"),
        X_orig = args$X,
        y = args$Y,
        icpt = ifelse(!args$intercept, 0, ifelse(!args$shiftAndRescale, 1, 2)),
        O = file.path(hydrar.env$WORKSPACE_ROOT("hydrar.step.lm"), "stats.csv"),
        fmt = "csv",
        write_beta = 1,
        'Selected',
        'beta_out'
      )
      
      if (!missing(threshold)) { dmlArgs <- c(dmlArgs, thr = args$threshold) }

      model@dmlArgs <- dmlArgs

      return(model)
    })

  }
)

setMethod("hydrar.model.import", signature="hydrar.step.lm", definition = 
  function(emptyModelForDispatch, directory) {
    warning("hydrar.model.import is not implemented yet")
    #@TODO
    #logSource <- "hydrar.model.import.hydrar.model.step.lm"
    #model <- callNextMethod()
    
    # Import subset of selected column ids
    #colids <- as.vector(t(as.data.frame(as.hydrar.matrix(model@modelPath %++% "/S.csv"))))
    
    # If intercept = TRUE, the first feature is the intercept itself
    #if (model@intercept == TRUE) {
    #  colids <- colids + 1
    #}
    
    #model@selectedFeatureNames <- model@featureNames[colids]
    #return(model)
  }
)
