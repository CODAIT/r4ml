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

setClass("r4ml.lm",
         slots = c(coefficients = "data.frame",
                   intercept = "logical",
                   shiftAndRescale = "logical",
                   transformPath = "character",
                   labelColumnName = "character",
                   method = "character"),
         contains = "r4ml.model"
)

#' @name r4ml.lm
#' @title Linear Regression
#' @export
#' @description Fits a linear regression model from an r4ml.matrix.
#' @details There are two different implementations to estimate the coefficients
#'  of the Linear Regression. Depending on the size and sparsity of the training
#'  set, one or the other method may be more efficient.
#'
#'  The \emph{direct-solve} method performs better when the number of features
#' \tabular{rlll}{
#'  \tab 1 \tab is relatively small (i.e., less than 1000) and \cr
#'  \tab 2 \tab the training set is either \cr
#'    \tab \tab ---- tall (i.e., number of rows >> number of columns) or \cr
#'    \tab \tab ---- fairly dense (i.e., the proportion of zero/missing values is very small). \cr
#'  }
#'  Otherwise,
#'  the \emph{iterative} method would be preferred. If the number of columns
#'  is above 50,000, \emph{iterative} should be used.
#' @param formula (formula) A formula in the form Y ~ ., where Y is the response variable.
#'                The response variable must be of type "scale".
#' @param data (r4ml.matrix) An r4ml.matrix to be fitted.
#' @param method (character) "direct-solve" or "iterative" (conjugate gradient). Default is "direct-solve".
#' @param intercept (logical) Boolean value indicating if the intercept term should be used for the regression.
#' @param shiftAndRescale (logical) Boolean value indicating if shifting and rescaling X columns to mean = 0, variance = 1 should be performed.
#' @param tolerance (numeric) Epsilon degree of tolerance, used when method is "iterative".
#' @param iter.max (numeric) Number of iterations, used when method is "iterative".
#' @param lambda (numeric) Regularization parameter.
# @TODO remove it
#' @param directory (character) The path to save the Linear Regression model
#' @return An S4 object of class \code{r4ml.lm} which contains the arguments above as well
#' as the following additional fields:
#'  \tabular{rlll}{
##'\tab\code{coefficients}  \tab (numeric)   \tab Coefficients of the regression\cr
##'\tab\code{modelPath}     \tab (character) \tab location where the model files are stored\cr
##'\tab\code{transformPath} \tab (character) \tab location where the \code{r4ml.transform()}
##'                                               metadata is stored \cr
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
#' # Project some relevant columns for modeling / statistical analysis
#' airlineFiltered <- airline[, c("Month", "DayofMonth", "DayOfWeek", "CRSDepTime",
#'                                "Distance", "ArrDelay")]
#'
#' airlineFiltered <- as.r4ml.frame(airlineFiltered)
#'
#' # Apply required transformations for Machine Learning
#' airlineFiltered <- r4ml.ml.preprocess(
#'  data = airlineFiltered,
#'  transformPath = "/tmp",
#'  recodeAttrs = c("DayOfWeek"),
#'  omit.na = c("Distance", "ArrDelay"),
#'  dummycodeAttrs = c("DayOfWeek")
#'  )
#'
#' airlineMatrix <- as.r4ml.matrix(airlineFiltered$data)
#'
#' # Split the data into 70% for training and 30% for testing
#' samples <- r4ml.sample(airlineMatrix, perc=c(0.7, 0.3))
#' train <- samples[[1]]
#' test <- samples[[2]]
#' 
#' train <- cache(train)
#' test <- cache(test)
#'
#' # Create a linear regression model
#' lm <- r4ml.lm(ArrDelay ~ ., data=train, directory = "/tmp")
#'
#' # Get the coefficients of the regression
#' coef(lm)
#'
#' # Calculate predictions for the testing set
#' pred <- predict(lm, test)
#'
#' print(pred)
#' }
#'
#' @seealso {predict.r4ml.lm} #NOTE add link after predict func
r4ml.lm <- function(formula, data, method = "direct-solve", intercept = TRUE,
                    shiftAndRescale = FALSE, tolerance, iter.max, lambda, directory) {
  new("r4ml.lm", modelType = "regression", formula = formula, data = data,
      method = method, intercept = intercept, shiftAndRescale = shiftAndRescale,
      tolerance = tolerance, iter.max = iter.max, lambda = lambda)

}

# overloaded method which checks the training parameters of the linear model
setMethod("r4ml.model.validateTrainingParameters", signature = "r4ml.lm", definition =
  function(model, args) {
    logSource <- "r4ml.model.validateTrainingParameters"
    with(args, {
      .r4ml.checkParameter(logSource, method, "character", c("direct-solve", "iterative"))
      .r4ml.checkParameter(logSource, intercept, "logical", c(TRUE, FALSE))
      .r4ml.checkParameter(logSource, shiftAndRescale, "logical", c(TRUE, FALSE))
      .r4ml.checkParameter(logSource, tolerance, "numeric", isOptional = TRUE)
      .r4ml.checkParameter(logSource, iter.max, "numeric", isOptional = TRUE)
      .r4ml.checkParameter(logSource, lambda, "numeric", isOptional = TRUE)
      if (!missing(iter.max) && (iter.max < 0)) {
        r4ml.err(logSource, "Parameter iter.max must be a natural number.")
      }
      if (!missing(tolerance) && (tolerance <= 0)) {
        r4ml.err(logSource, "Parameter tolerance must be a positive number.")
      }
      if (!missing(lambda) && (lambda < 0)) {
        r4ml.err(logSource, "Parameter lambda must be a non-negative number.")
      }
      if (!intercept & shiftAndRescale) {
        r4ml.err(logSource, "The shiftAndRescale should be FALSE when intercept is FALSE.")
      }
      if ((method == "iterative") && !missing(iter.max)) {
        if ((iter.max < 1) | (round(iter.max) != iter.max) | is.infinite(iter.max)) {
          r4ml.err(logSource, "The iter.max argument has an invalid value.")
        }
      }
      return(model)
    })
  }
)

# Overwrite the base model's method to build the traning args which will be
# passed to the dml script to run
setMethod("r4ml.model.buildTrainingArgs", signature = "r4ml.lm", definition =
  function(model, args) {
    with(args, {
      model@training_data <- args$data
      model@method <- method
      model@intercept <- intercept
      model@shiftAndRescale <- shiftAndRescale

      if (args$intercept == TRUE) {
        model@featureNames <- c(model@featureNames, r4ml.env$INTERCEPT)
      }

      #coefPath <- model@modelPath %++% "/coefficients.csv"
      #statsPath <- model@modelPath %++% "/stats.csv"
      workspace <- r4ml.env$WORKSPACE_ROOT("r4ml.lm")
      statsPath <- file.path(workspace, "stats.csv")
      dmlPath <- file.path(r4ml.env$SYSML_ALGO_ROOT(),
                   ifelse(args$method == "direct-solve",
                     r4ml.env$DML_LM_DS_SCRIPT, r4ml.env$DML_LM_CG_SCRIPT))
      # invoke DML script
      dmlArgs <- list(
        dml = dmlPath,
        X = args$X,
        y = args$Y,
        icpt = ifelse(!args$intercept, 0, ifelse(!args$shiftAndRescale, 1, 2)),
        "beta_out", # output from DML script
        O = statsPath,
        fmt = "csv")
      if (!missing(lambda)) {
        dmlArgs <- c(dmlArgs, reg = args$lambda)
      }
      if (args$method == "iterative") {
        if (!missing(tolerance)) {
          dmlArgs <- c(dmlArgs, tol = args$tolerance)
        }
        if (!missing(iter.max)) {
          dmlArgs <- c(dmlArgs, maxi = args$iter.max)
        }
      }
      model@dmlArgs <- dmlArgs
      return(model)
    })
  }
)

# overwrite the base model's post training function so that one can
# post process the final outputs from the dml scripts
setMethod("r4ml.model.postTraining", signature = "r4ml.lm", definition =
  function(model) {
    outputs <- model@dmlOuts$sysml.execute
    #stats calculation
    statsPath <- model@dmlArgs$O
    statsCsv <- SparkR::as.data.frame(r4ml.read.csv(statsPath, header = FALSE, stringsAsFactors = FALSE))
    stats <- statsCsv[, 2, drop = FALSE]
    row.names(stats) <- statsCsv[, 1]
    colnames(stats) <- "value"
    model@dmlOuts$stats = stats

    outNames <- names(outputs)
    i <- 1
    for (output in outputs) {
      outName <- outNames[i]
      i <- i + 1
      model@dmlOuts[outName] <- output
    }

    model@coefficients <- coef(model)
    model@labelColumnName <- model@yColname
    return(model)
  }
)

#' @title print linear model fitted coefficients
#' @description Print the coefficients of the fitted model
#' @name show
#' @param object (r4ml.lm) The linear regression model
#' @return None
#' @seealso \link{coef}
setMethod(f = "show", signature = "r4ml.lm", definition =
  function(object) {
    logSource <- "r4ml.lm"
      callNextMethod()
      cat("\n\nCoefficients: \n")
      coeff <- coef(object)
      print(coeff)
  }
)

#' @title linear model fitted coefficients
#' @description Get the coefficients of the fitted model
#' @name coef
#' @param object (r4ml.lm) The linear regression model
#' @return A data.frame with the coefficient for the learned model
#' @seealso \link{show}
setMethod("coef", signature = "r4ml.lm", definition =
  function(object) {
    obj <- SparkR::as.data.frame(object@dmlOuts[["beta_out"]])
    rownames(obj) <- object@featureNames
    obj
  }
)

#' @title Get the statistics data of a learned model
#' @description With this method we can get the statistics of a learned model
#'              represented by the object
#' @name stats
#' @param object (r4ml.lm) The linear regression model
#' @return A data.frame with the statistics data for the learned model
setMethod("stats", signature = "r4ml.lm", definition =
  function(object) {
    return(object@dmlOuts$stats)
  }
)

#' @name predict.r4ml.lm
#' @title Predict method for Linear Regression models
#' @description This method allows one to score/test a linear regression model for a given r4ml.matrix. If the testing set
#' is labeled, testing will be done and some statistics will be computed to measure the quality of the model. 
#' Otherwise, scoring will be performed and only the predictions will be computed.
#' @param object (r4ml.lm) The linear regression model
#' @param data (r4ml.matrix) The data to be tested or scored.
#' @return If the testing dataset is not labeled, the result will be an r4ml.matrix with the predictions for each row.
#' Otherwise, the result will be a list with an r4ml.matrix with the predictions for each row (\code{$probabilities}), and
#' (2) a data.frame with goodness-of-fit statistics ($statistics) for each column. Please refer to \link{predict.r4ml.glm} for the
#' definitions of these statistics.
#' @examples \dontrun{
#' 
#'   df <- iris
#'   df$Species <- (as.numeric(df$Species))
#'   iris_df <- as.r4ml.frame(df)
#'   iris_mat <- as.r4ml.matrix(iris_df)
#'   ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 
#'   s <- r4ml.sample(iris_mat, perc=c(0.2, 0.8))
#'   test <- s[[1]]
#'   train <- s[[2]]
#'   y_test <- as.r4ml.matrix(test[, 1])
#'   y_test <- SparkR::as.data.frame(y_test)
#'   test <- as.r4ml.matrix(test[,c(2:5)])
#'   iris_lm <- r4ml.lm(Sepal_Length ~ ., data = train, method = "iterative")
#'   output <- predict(iris_lm, test)
#' }
#' 
#' @export
#' @seealso \link{r4ml.lm}
predict.r4ml.lm <- function(object, data) {
    logSource <- "predict.r4ml.lm"
    
    r4ml.info(logSource, "Predicting labels using given Linear Regression model.")
    model <- object
    .r4ml.checkParameter(logSource, data, inheritsFrom = "r4ml.matrix")

    # Create path for storing goodness-of-fit statistics
    statsPath <- file.path(r4ml.env$WORKSPACE_ROOT("r4ml.lm"), "stats_predict.csv")

    # compare the column name vector with the data's to determine if we're making predictions
    testing <- r4ml.ml.checkModelFeaturesMatchData(coef(object), data, object@intercept, object@labelColumnName, object@yColId)  

    # accumulate arguments for generating statistics if label data is present
    if (testing) {
        # testing
        xAndY <- r4ml.model.splitXY(data, model@yColname)
        testset_x <- xAndY$X
        testset_y <- xAndY$Y   
        args = list(X = testset_x, Y = testset_y)
        args <- c(args, O = statsPath)
        args <- c(args, scoring_only = "no")
    }
    # accumulate argument if no label data is present to make predictions
    else {
        # scoring
        args = list(X = data)
        args <- c(args, scoring_only = "yes")
    }
    # add arguments that are general across testing/scoring
    args <- c(args, dfam = 1)
    args <- c(args, B_full = as.r4ml.matrix(coef(object)))
    args <- c(args, "means")
    args <- c(args, fmt = r4ml.env$CSV)
    args <- c(args, dml = file.path(r4ml.env$SYSML_ALGO_ROOT(), r4ml.env$DML_GLM_TEST_SCRIPT))
    
    # call the predict DML script
    dmlOuts <- do.call("sysml.execute", args)
    preds <- dmlOuts[['means']]
    SparkR::colnames(preds) <- c("preds")

    # orgainze result for presentation to user
    if (testing) {
      stats <- SparkR::as.data.frame(r4ml.read.csv(statsPath, header = FALSE,
                                                   stringsAsFactors = FALSE))
      return(list("predictions" = preds, "statistics" = stats))
    } else {
      return(list("predictions" = preds))
    }
}

r4ml.lm.se <- function(object) {
  logSource <- "r4ml.lm.se"

  df <- object@training_data
  training_nrow <- SparkR::nrow(df)

  if (training_nrow > 60000) {
    # since the dataset has >60k rows we want a sample we can fit on the driver
    frac <- 50000 / training_nrow # we want a sample of ~50k rows
    df <- SparkR::sample(df, withReplacement = FALSE, fraction = frac, seed = 5)
  }

  df <- SparkR::as.data.frame(df)

  colnames(df)[which(colnames(df) == object@labelColumnName)] <- "y"

  if (object@intercept) {
    fit <- lm(y ~ ., data = df )
  } else {
    fit <- lm(y ~ . -1, data = df )
  }

  coefs <- summary(fit)$coefficients
  coefs <- as.data.frame(coefs)

  # rescale the std. error
  coefs$`Std. Error` <- coefs$`Std. Error` * sqrt(nrow(df) / training_nrow)

  coefs$`t value` <- coefs$Estimate / coefs$`Std. Error`

  coefs$`Pr(>|t|)` <- 2 * pt(abs(coefs$`t value`), training_nrow, lower.tail = FALSE)

  return(coefs)
}

#' @name summary.r4ml.lm
#' @title LM Summary
#' @description Summarizes an R4ML LM model \eqn{a + b}
#' @param object (r4ml.lm): An R4ML LM model
#' @return a summary of the model
#' @export
#' @examples \dontrun{
#' summary.r4ml.lm(r4ml_lm_model)
#' }
summary.r4ml.lm <- function(object) {
  cat("Call:\n")
  cat(object@call)
  cat("\n\n")

  cat("Residuals:\n")
  cat("   Mean: ")
  cat(round(x = object@dmlOuts$stats["AVG_RES_Y", "value"], digits = 5))
  cat("   St. Dev.: ")
  cat(round(x = object@dmlOuts$stats["STDEV_RES_Y", "value"], digits = 5))
  cat("\n\n")

  cat("Coefficients:\n")
  coef_df <- object@coefficients
  base::colnames(coef_df) <- c("Estimate")
  coef_df$`Std. Error` <- NA
  coef_df$`t value` <- NA
  coef_df$`Pr(>|t|)` <- NA

  se_df <- r4ml.lm.se(object)

  # need to make sure the order is correct
  se_df <- se_df[order(row.names(se_df)), ]
  coef_df <- coef_df[order(row.names(coef_df)), ] 

  coef_df$`Std. Error` <- round(se_df$`Std. Error`, 7)
  coef_df$`t value` <- round(se_df$`t value`, 3)
  coef_df$`Pr(>|t|)` <- round(se_df$`Pr(>|t|)`, 10)

  cat("\n") # new line here to deal with RStudio handling of SparkR output
  print(coef_df)
  
  cat("\n")
  cat("NOTE: Std. Error, t value, & Pr(>|t|) are estimated")
  cat("\n\n")

  cat("Residual standard deviation: ")
  cat(round(x = object@dmlOuts$stats["STDEV_RES_Y", "value"], digits = 5))
  cat("\n\n")

  cat("Multiple R-squared: ")
  cat(round(x = object@dmlOuts$stats["PLAIN_R2", "value"], digits = 5))
  cat(", Adjusted R-squared: ")
  cat(round(x = object@dmlOuts$stats["ADJUSTED_R2", "value"], digits = 5))
  cat("\n")

  return(invisible(NULL))
}
