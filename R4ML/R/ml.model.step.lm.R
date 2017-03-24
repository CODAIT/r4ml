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
#' @include zzz.R
#' @include ml.model.base.R

setClass("r4ml.step.lm",
         representation(selectedFeatureNames = "character"),
         contains = "r4ml.lm"
         )

#' @name r4ml.step.lm
#' @title Step-wise Linear Regression
#' @export
#' @description Fits a linear regression model from a r4ml.matrix in a
#' step-wise fashion or loads an existing model from HDFS.
#' @param formula (formula) A formula in the form Y ~ ., where Y is the response
#' variable. The response variable must be of type "scale".
#' @param data (r4ml.matrix) A r4ml.matrix to be fitted.
#' @param intercept (logical) Boolean value indicating if the intercept term
#' should be used for the regression.
#' @param shiftAndRescale (logical) Boolean value indicating if shifting and
#' rescaling X columns to mean = 0, variance = 1 should be performed.
#' @param threshold Threshold to stop the algorithm: if the decrease in the
#' value of AIC falls below this no further features are being checked and the
#' algorithm stops 
#' @param directory (character) The HDFS path to save the Linear Regression model
#'  if \code{formula} and \code{data} are specified. Otherwise, an HDFS location
#'  with a previously trained model to be loaded.
#' @return An S4 object of class \code{r4ml.step.lm} which contains the arguments above as well
#' as the following additional fields:
#'  \tabular{rlll}{
##'\tab\code{coefficients}  \tab (numeric)   \tab Coefficients of the regression\cr
##'\tab\code{modelPath}     \tab (character) \tab HDFS location where the model files are stored\cr
##'\tab\code{transformPath} \tab (character) \tab HDFS location where the \code{r4ml.transform()}
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
#' # Convert the iris data set into a R4ML frame.
#' # Column #5 is a factor so we will ignore it for now
#' r4ml_iris <- as.r4ml.frame(iris[, -5])
#' 
#' # Convert the R4ML frame into a R4ML matrix
#' r4ml_iris <- as.r4ml.matrix(r4ml_iris)
#'
#'
#' step_lm <- r4ml.step.lm(Sepal_Length ~ ., data = r4ml_iris)
#'
#' }
#'
#' @seealso \link{predict.r4ml.lm}
#'


r4ml.step.lm <- function(formula, data, intercept=FALSE, shiftAndRescale=FALSE, threshold=.001,
                           directory = r4ml.env$WORKSPACE_ROOT("r4ml.step.lm")){
  
  new("r4ml.step.lm", modelType="regression",
      formula=formula, data=data, method="direct-solve", intercept=intercept, shiftAndRescale=shiftAndRescale,
      lambda=0, directory=directory, tolerance=r4ml.emptySymbol(), iter.max=r4ml.emptySymbol(), threshold=threshold)
}

setMethod("r4ml.model.validateTrainingParameters", signature="r4ml.step.lm", def = 
  function(model, args) {
    logSource <- "r4ml.model.validateTrainingParameters"
      callNextMethod()
        with(args, {
          .r4ml.checkParameter(logSource, threshold, c("numeric", "integer"), isOptional=TRUE)
        })

        return(model)
    }
  )

setMethod(f = "show", signature = "r4ml.step.lm", definition = 
  function(object) {
    logSource <- "r4ml.step.lm.show"
    callNextMethod()
    
    cat("\nFeature selection order: \n")
    print(object@selectedFeatureNames)
  }
)

setMethod("r4ml.model.buildTrainingArgs", signature="r4ml.step.lm", def = 
  function(model, args) {
    with(args,{
      model@method <- method
      model@intercept <- intercept
      model@shiftAndRescale <- shiftAndRescale

      if (args$intercept == TRUE) {
        model@featureNames <- c(model@featureNames, r4ml.env$INTERCEPT)
      }

      dmlArgs <- list(
        dml = file.path(r4ml.env$SYSML_ALGO_ROOT(), "StepLinearRegDS.dml"),
        X_orig = args$X,
        y = args$Y,
        icpt = ifelse(!args$intercept, 0, ifelse(!args$shiftAndRescale, 1, 2)),
        O = file.path(r4ml.env$WORKSPACE_ROOT("r4ml.step.lm"), "stats.csv"),
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

setMethod("r4ml.model.import", signature="r4ml.step.lm", definition = 
  function(emptyModelForDispatch, directory) {
    warning("r4ml.model.import is not implemented yet")
    #@TODO
    #logSource <- "r4ml.model.import.r4ml.model.step.lm"
    #model <- callNextMethod()
    
    # Import subset of selected column ids
    #colids <- as.vector(t(as.data.frame(as.r4ml.matrix(model@modelPath %++% "/S.csv"))))
    
    # If intercept = TRUE, the first feature is the intercept itself
    #if (model@intercept == TRUE) {
    #  colids <- colids + 1
    #}
    
    #model@selectedFeatureNames <- model@featureNames[colids]
    #return(model)
  }
)
