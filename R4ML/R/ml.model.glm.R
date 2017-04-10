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
requireNamespace("SparkR")
# This class represents a General Linear Model.
setOldClass("family")

setClass(
  "r4ml.glm",
  representation(
    coefficients = "data.frame",
    family="family",
    neg.binomial.class = "numeric",
    intercept= "logical", 
    shiftAndRescale = "logical",
    lambda = "numeric", 
    tolerance = "numeric", 
    dispersion = "numeric", 
    outer.iter.max = "numeric", 
    inner.iter.max = "numeric"
  ), contains = "r4ml.model"
)

#' @title Generalized Linear Models
#' @description Fits a Generalized Linear Model for a \code{r4ml.matrix}.
#' @param formula (formula) A formula in the form Y ~ ., where Y is the response variable.
#' @param data (r4ml.matrix) Dataset to fit the model.
#' @param family (family) Distribution family and link function. 
#'                Supported families are "binomial", "gaussian", "poisson", "gamma", and "inverse.gaussian".
#'                Supported link functions are "identity", "inverse", "log", "sqrt", "1/mu^2", "logit", "probit", "cloglog", and "cauchit".
#'                gaussian(identity) is used by default.
#' @param neg.binomial.class (numeric) Only required if \code{family} is set to binomial. It is the response value for the "No" label. 
#' If the response variable was recoded, method link{r4ml.recodeMaps}
#' can be used to set \code{neg.binomial.class} accordingly. Values in the response variable that are different than \code{neg.binomial.class} will be considered as the "Yes" class.
#' @param intercept (logical) Boolean value specifying whether the intercept term should be part of the model.
#' @param shiftAndRescale (logical) Boolean value indicating if shifting and rescaling X columns to mean = 0, variance = 1 should be performed.
#' @param lambda (numeric) Regularization parameter for L2 regularization.
#' @param tolerance (numeric) Epsilon degree of tolerance.
#' @param dispersion (numeric) (Over-)dispersion value, or 0.0 to estimate it from data.
#' @param outer.iter.max (integer) Maximum number of outer (Newton / Fisher Scoring) iterations.
#' @param inner.iter.max (integer) Maximum number of inner (Conjugate Gradient) iterations. A value of 0 indicates no maximum.
#' are specified.
#' @return An S4 object of class r4ml.glm which contains the arguments above as well as the following additional fields:
##' \tabular{rlll}{
##'\tab\code{coefficients}          \tab (numeric) \tab Coefficients of the regression \cr
##'\tab\code{transformPath} \tab (character)   \tab location where the \code{r4ml.transform()} metadata is stored \cr
##'\tab\code{yColId}          \tab (numeric) \tab Column id of the response variable\cr
##'\tab\code{yColname}      \tab (character) \tab Column name of the response variable \cr
##'\tab\code{featureNames}      \tab (character) \tab The features used to build the model \cr
##'\tab\code{call}          \tab (character) \tab String representation of this method's call, including the parameters and values passed to it.\cr
##'}
#' 
#' @examples \dontrun{
#' 
#'                                                                           
#' # Project some relevant columns for modeling / statistical analysis
#' airlineFiltered <- airline[, c("Month", "DayofMonth", "DayOfWeek", "CRSDepTime",
#'                                 "Distance", "ArrDelay")]
#'
#' airlineFiltered <- as.r4ml.frame(airlineFiltered)
#'
#'# Apply required transformations for Machine Learning
#' airlineFiltered <- r4ml.ml.preprocess(
#'   hf = airlineFiltered,
#'   transformPath = "/tmp",
#'   recodeAttrs = c("DayOfWeek"),
#'   omit.na = c("Distance", "ArrDelay"),
#'   dummycodeAttrs = c("DayOfWeek")
#' )
#'
#' airlineMatrix <- as.r4ml.matrix(airlineFiltered$data)
#'
#' # Split the data into 70% for training and 30% for testing
#' samples <- r4ml.sample(airlineMatrix, perc=c(0.7, 0.3))
#' train <- samples[[1]]
#' test <- samples[[2]]
#'
#' # Create a generalized linear model
#' glm <- r4ml.glm(ArrDelay ~ ., data=train, family=gaussian(identity))
#'
#' # Get the coefficients of the regression
#' glm
#' 
#' # Calculate predictions for the testing set
#' pred <- predict(glm, test)
#' print(pred)
#' }       
#' 
#' @seealso \link{predict.r4ml.glm}
#' @export
r4ml.glm <- function(formula, data, family=gaussian(link="identity"), neg.binomial.class, intercept = TRUE, shiftAndRescale = FALSE,
                       lambda, tolerance, dispersion, outer.iter.max, inner.iter.max) {
  
  new("r4ml.glm", modelType="regression", formula=formula, data=data, family=family, 
      neg.binomial.class=neg.binomial.class, intercept = intercept, 
      shiftAndRescale = shiftAndRescale, lambda=lambda, tolerance=tolerance, 
      dispersion=dispersion, outer.iter.max=outer.iter.max, inner.iter.max=inner.iter.max)
}

setMethod(
  "r4ml.model.validateTrainingParameters", 
  signature="r4ml.glm", 
  definition = 
    function(model, args) {
      logSource <- "r4ml.model.validateTrainingParameters(r4ml.glm)"
      with(args, {
        .r4ml.checkParameter(logSource, family, c("family"))
        .r4ml.checkParameter(logSource, neg.binomial.class, c("numeric", "integer"), isOptional=T)
        .r4ml.checkParameter(logSource, intercept, c("logical"))
        .r4ml.checkParameter(logSource, shiftAndRescale, c("logical"))    
        .r4ml.checkParameter(logSource, lambda, "numeric", isOptional=T)
        .r4ml.checkParameter(logSource, tolerance, "numeric", isOptional=T)
        .r4ml.checkParameter(logSource, dispersion, "numeric", isOptional=T)
        
        .r4ml.checkParameter(logSource, outer.iter.max, "numeric", isOptional=T)
        .r4ml.checkParameter(logSource, inner.iter.max, "numeric", isOptional=T)
        
        if (family$family == "binomial") {
          if (missing(neg.binomial.class)) {
            r4ml.err(logSource, "When family is set to binomial, parameter neg.binomial.class must be set.")
          }
        } else {
          if (!missing(neg.binomial.class)) {
            r4ml.err(logSource, "Parameter neg.binomial.class can only be set when family is set to binomial.")
          }
        }
        if (!missing(outer.iter.max) && (outer.iter.max <= 0)) {
          r4ml.err(logSource, "Parameter outer.iter.max must be a positive integer.")
        }
        if (!missing(inner.iter.max) && (inner.iter.max < 0)) {
          r4ml.err(logSource, "Parameter inner.iter.max must be a natural number.")
        }
        if (!missing(tolerance) && (tolerance <= 0)) {
          r4ml.err(logSource, "Parameter tolerance must be a positive number.")
        }
        if (!missing(lambda) && (lambda < 0)) {
          r4ml.err(logSource, "Parameter lambda must be a non-negative number.")
        }    
        if (!missing(dispersion) && (dispersion < 0)) {
          r4ml.err(logSource, "Parameter dispersion must be a positive number.")
        }
        if(ncol(data) < 2) {
          r4ml.err(logSource, "Given training dataset must have at least two columns.")
        }
        
        r4ml.debug(logSource, "Parameters checked")
        
        return(model)
      })
    }
)

setMethod(
  "r4ml.model.buildTrainingArgs", 
  signature="r4ml.glm", 
  definition = 
    function(model, args) {
      logSource <- "r4ml.model.buildTrainingArgs.r4ml.glm"                  
      with(args,  {
        # Extract parameters from family: Default = gaussian, identity
        familyInfo <- r4ml.ml.parseFamilyAndLink(family)    
        distFamily <- familyInfo$distFamily
        powerOfVariance <- familyInfo$powerOfVariance
        linkFunction <- familyInfo$linkFunction
        powerOfLink <- familyInfo$powerOfLink
        
        r4ml.debug(logSource, "\ndistFamily: " %++% distFamily)
        r4ml.debug(logSource, "\npowerOfVariance: " %++% powerOfVariance)
        r4ml.debug(logSource, "\nlinkFunction: " %++% linkFunction)
        r4ml.debug(logSource, "\npowerOfLink: " %++% powerOfLink)                      
        
        # Compute the value for parameter icpt, which combines both intercept and shiftAndRescale
        icpt = if(intercept && !shiftAndRescale) {
          1  
        } else if (intercept && shiftAndRescale) {
          2
        } else if (!intercept && !shiftAndRescale) {
          0
        } else {
          r4ml.err(logSource, "Parameter 'intercept' must be TRUE when 'shiftAndRescale' is TRUE")
        }
        
        X <- X
        Y <- Y
        model@family <- family
        model@intercept <- intercept
        model@shiftAndRescale <- shiftAndRescale
        
        if (args$intercept == TRUE) {
          model@featureNames <- c(r4ml.env$INTERCEPT, model@featureNames)
        }
        
        statsPath <- file.path(r4ml.env$WORKSPACE_ROOT("r4ml.glm"), "stats.csv")
        
        ###########################################################
        # Invoke the SystemML script with the following parameters:
        ###########################################################
        # X     String  ---     Location to read the matrix X of feature vectors
        # Y     String  ---     Location to read response matrix Y with either 1 or 2 columns:
        #                       if dfam = 2, Y is 1-column Bernoulli or 2-column Binomial (#pos, #neg)
        # B     String  ---     Location to store estimated regression parameters (the betas)
        # dfam  Int     1       Distribution family code: 1 = Power, 2 = Binomial
        # vpow  Double  0.0     Power for Variance defined as (mean)^power (ignored if dfam != 1):
        #                       0.0 = Gaussian, 1.0 = Poisson, 2.0 = gamma, 3.0 = Inverse Gaussian
        # link  Int     0       Link function code: 0 = canonical (depends on distribution),
        #                       1 = Power, 2 = Logit, 3 = Probit, 4 = Cloglog, 5 = Cauchit
        # lpow  Double  1.0     Power for Link function defined as (mean)^power (ignored if link != 1):
        #                       -2.0 = 1/mu^2, -1.0 = reciprocal, 0.0 = log, 0.5 = sqrt, 1.0 = identity
        # yneg  Double  0.0     Response value for Bernoulli "No" label, usually 0.0 or -1.0
        # icpt  Int     0       Intercept presence, X columns shifting and rescaling:
        #                       0 = no intercept, no shifting, no rescaling;
        #                       1 = add intercept, but neither shift nor rescale X;
        #                       2 = add intercept, shift & rescale X columns to mean = 0, variance = 1
        # reg   Double  0.0     Regularization parameter (lambda) for L2 regularization
        # tol   Double 0.000001 Tolerance (epsilon)
        # disp  Double  0.0     (Over-)dispersion value, or 0.0 to estimate it from data
        # moi   Int     200     Maximum number of outer (Newton / Fisher Scoring) iterations
        # mii   Int     0       Maximum number of inner (Conjugate Gradient) iterations, 0 = no maximum
        dmlArgs <- list(dml=file.path(r4ml.env$SYSML_ALGO_ROOT(),r4ml.env$DML_GLM_SCRIPT), 
                        X=X,
                        Y=Y,
                        "beta_out",
                        O=statsPath,
                        dfam=distFamily,
                        vpow=powerOfVariance,
                        link=linkFunction,
                        lpow=powerOfLink,
                        icpt=icpt,
                        fmt="csv")
        if (!missing(neg.binomial.class)) {
          dmlArgs <- c(dmlArgs, yneg=neg.binomial.class)
        }
        if (!missing(lambda)) {
          dmlArgs <- c(dmlArgs, reg=lambda)
          model@lambda <- lambda
        }
        if (!missing(dispersion)) {
          dmlArgs <- c(dmlArgs, disp=dispersion)
          model@dispersion <- dispersion
        } else {
          model@dispersion <- 0
        }
        if (!missing(tolerance)) {
          dmlArgs <- c(dmlArgs, tol=tolerance)
        }
        if (!missing(outer.iter.max)) {
          dmlArgs <- c(dmlArgs, moi=outer.iter.max)
        }
        if (!missing(inner.iter.max)) {
          dmlArgs <- c(dmlArgs, mii=inner.iter.max)
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
  signature="r4ml.glm", 
  definition =
    function(model) {
      outputs <- model@dmlOuts$sysml.execute
      statsPath <- model@dmlArgs$O
      statsCsv <- SparkR::as.data.frame(r4ml.read.csv(statsPath, header=FALSE, stringsAsFactors=FALSE))
      if (model@dispersion == 0) {
        model@dispersion <- as.numeric(statsCsv[8, 2])
      }
      stats <- statsCsv[, 2, drop=F]
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
      model@coefficients <- .r4ml.glm.buildCoef(model)
      return(model)
    }
)

.r4ml.glm.buildCoef <- function(object) {
  df <- as.data.frame(t(SparkR::as.data.frame(object@dmlOuts[["beta_out"]])))
  if (object@shiftAndRescale) {
    row.names(df) <- c("no-shift-and-rescale", "shift-and-rescale")
  } else {
    row.names(df) <- NULL
  }
  
  # Move the intercept column from the end to the beginning
  if (object@intercept == T) {
    df <- df[, c(ncol(df), seq(1, ncol(df) - 1) )]
  }
  names(df) <- object@featureNames
  return(df)
}

#' @title GLM model fitted coefficients
#' @description Get the coefficients of the fitted model
#' @name coef
#' @param object (r4ml.glm) The linear regression model
#' @return A data.frame with the coefficient for the learned model
#' @seealso \link{show}
#' @export
setMethod(
  "coef", 
  signature="r4ml.glm", 
  definition =
    function(object) {
      object@coefficients
    }
)

setMethod(
  f = "show", 
  signature = "r4ml.glm", 
  definition = 
    function(object) {
      logSource <- "show.r4ml.glm"                  
      callNextMethod()                  
      cat("\n\nFamily: \n" %++% object@family$family)
      cat("\n\nLink: \n" %++% object@family$link)                  
      cat("\n\nCoefficients: \n")
      print(coef(object))
    }
)

#' @title Get the statistics data of a learned model
#' @description With this method we can get the statistics of a learned model
#'              represented by the object
#' @name stats
#' @param object (r4ml.glm) The GLM model
#' @return A data.frame with the statistics data for the learned model
#' @export
setMethod(
  "stats", 
  signature="r4ml.glm", 
  definition =
    function(object) {
      return(object@dmlOuts$stats)
    }
)

# @TODO: Add validations for inconsistent combinations of link/families
r4ml.ml.parseFamilyAndLink <- function(family) {
  logSource <- "r4ml.ml.parseFamilyAndLink"
  
  distFamily <- 0
  powerOfVariance <- 0
  linkFunction <- 0
  powerOfLink <- 0
  
  if (family$family == "gaussian") {    
    distFamily <- 1
    powerOfVariance <- 0
  } else if (family$family == "poisson") {
    distFamily <- 1
    powerOfVariance <- 1
  } else if (family$family == "gamma") {
    distFamily <- 1
    powerOfVariance <- 2
  } else if (family$family == "inverse.gaussian") {
    distFamily <- 1
    powerOfVariance <- 3
  } else if (family$family == "binomial") {
    distFamily <- 2
    powerOfVariance <- 0 # Default
  } else {
    r4ml.err(logSource, "Invalid family: " %++% family$family)
  }
  
  if (family$link == "identity") { 
    linkFunction <- 1
    powerOfLink <- 1
  } else if (family$link == "inverse") {
    linkFunction <- 1
    powerOfLink <- -1
  } else if (family$link == "log") {
    linkFunction <- 1
    powerOfLink <- 0
  } else if (family$link == "sqrt") {
    linkFunction <- 1
    powerOfLink <- 0.5
  } else if (family$link == "1/mu^2") {
    linkFunction <- 1
    powerOfLink <- -2
  } else if (family$link == "logit") {
    linkFunction <- 2
    powerOfLink <- 1 # Default
  } else if (family$link == "probit") {
    linkFunction <- 3
    powerOfLink <- 1 # Default
  } else if (family$link == "cloglog") {
    linkFunction <- 4
    powerOfLink <- 1 # Default
  } else if (family$link == "cauchit") {
    linkFunction <- 5
    powerOfLink <- 1 # Default
  } else {
    r4ml.err(logSource, "Invalid link function: " %++% family$link)
  }    
  list("distFamily"=distFamily, "powerOfVariance"=powerOfVariance, "linkFunction"=linkFunction, "powerOfLink"=powerOfLink)
}

#' @title Predict method for Generalized Linear Models
#' @name predict.r4ml.glm
#' @description This method allows to score/test a GLM model for a given r4ml.matrix. If the testing set is labeled,
#' testing will be done and some statistics will be computed to measure the quality of the model. Otherwise, scoring will be performed
#' and only the predictions will be computed.
#' @param object (r4ml.glm) a GLM model built by Big R
#' @param data (r4ml.matrix) the testing set
#' @param family (family) the distribution family and link function. Supported families are "gaussian", "poisson", "gamma", and "inverse.gaussian".
#' Supported link functions are "identity", "inverse", "log", "sqrt", "1/mu^2", "logit" "probit", "cloglog", and "cauchit"
#' @param dispersion (numeric) (Over-) dispersion value. If not provided, the dispersion provided/estimated in the training phase will be used.
#' @return If scoring (i.e., testing dataset is unlabeled), the result will be a r4ml.matrix with the predictions.
#' In the case of testing (i.e., testing dataset is labeled), the result will be a list with two elements: (1) the
#' predictions as a r4ml.matrix, and (2) a data.frame with some goodness-of-fit statistics. These statistics are as follows:
#' 
#' \tabular{rlll}{
#' \tab LOGLHOOD Z \tab Log-likelihood Z-score (in st. dev.'s from the mean)\cr
#' \tab LOGLHOOD Z PVAL\tab Log-likelihood Z-score p-value, two-sided\cr
#' \tab PEARSON X2\tab Pearson residual chi-square statistic\cr
#' \tab PEARSON X2 BY DF\tab Pearson chi-square divided by degrees of freedom\cr
#' \tab PEARSON_X2_BY_PVAL\tab Pearson chi-square p-value\cr
#' \tab DEVIANCE_G2\tab Deviance from the saturated model G2-statistic\cr
#' \tab DEVIANCE_G2_BY_DF\tab Deviance G2-statistic divided by degrees of freedom\cr
#' \tab DEVIANCE_G2_PVAL\tab Deviance G2-statistic p-value\cr
#' \tab AVG_TOT_Y\tab Y-column average for an individual response value\cr
#' \tab STDEV_TOT_Y\tab Y-column st. dev. for an individual response value\cr
#' \tab AVG_RES_Y\tab Y-column residual average of Y - pred: mean(Y|X)\cr
#' \tab STDEV_RES_Y\tab Y-column residual st. dev. of Y - pred:mean(Y|X)\cr
#' \tab PRED_STDEV_RES\tab Model-predicted Y-column residual st. deviation\cr
#' \tab PLAIN_R2\tab Plain R2 of Y-column residual with bias included\cr
#' \tab ADJUSTED_R2\tab Adjusted R2 of Y-column residual w. bias included\cr
#' \tab PLAIN_R2_NOBIAS\tab Plain R2 of Y-column residual, bias subtracted\cr
#' \tab ADJUSTED_R2_NOBIAS\tab Adjusted R2 of Y-column residual, bias subtracted\cr
#' }
#' 
#' @examples \dontrun{
#' 
#' # Project some relevant columns for modeling / statistical analysis
#' airlineFiltered <- airline[, c("Month", "DayofMonth", "DayOfWeek", "CRSDepTime",
#'                                 "Distance", "ArrDelay")]
#'
#' airlineFiltered <- as.r4ml.frame(airlineFiltered)
#'
#'# Apply required transformations for Machine Learning
#' airlineFiltered <- r4ml.ml.preprocess(
#'   hf = airlineFiltered,
#'   transformPath = "/tmp",
#'   recodeAttrs = c("DayOfWeek"),
#'   omit.na = c("Distance", "ArrDelay"),
#'   dummycodeAttrs = c("DayOfWeek")
#' )
#'
#' airlineMatrix <- as.r4ml.matrix(airlineFiltered$data)
#'
#' # Split the data into 70% for training and 30% for testing
#' samples <- r4ml.sample(airlineMatrix, perc=c(0.7, 0.3))
#' train <- samples[[1]]
#' test <- samples[[2]]
#'
#' # Create a generalized linear model
#' glm <- r4ml.glm(ArrDelay ~ ., data=train, family=gaussian(identity))
#'
#' # Get the coefficients of the regression
#' glm
#' 
#' # Calculate predictions for the testing set
#' pred <- predict(glm, test)
#' print(pred)
#' }       
#' 
#' @seealso \link{r4ml.glm}
#' @export
predict.r4ml.glm <- function(object, data, family, dispersion) {
  logSource <- "r4ml.predict.glm"
  
  r4ml.info(logSource, "Predicting labels using given GLM model on data")
  glm <- object
  
  .r4ml.checkParameter(logSource, data, inheritsFrom="r4ml.matrix")
  .r4ml.checkParameter(logSource, family, "family", isOptional=T)
  .r4ml.checkParameter(logSource, dispersion, c("numeric", "integer"), isOptional=T)
  
  if (!missing(dispersion) && (dispersion < 0)) {
    r4ml.err(logSource, "Parameter dispersion must be a positive number.")
  }
  
  # Extract parameters from family: Default values are obtained from the model.
  if (missing(family)) {
    family <- object@family
  }
  
  if (missing(dispersion)) {
    dispersion <- object@dispersion
  }    
  
  familyInfo <- r4ml.ml.parseFamilyAndLink(family)
  
  distFamily <- familyInfo$distFamily
  powerOfVariance <- familyInfo$powerOfVariance
  linkFunction <- familyInfo$linkFunction
  powerOfLink <- familyInfo$powerOfLink    
  
  r4ml.info(logSource, "\ndistFamily: " %++% distFamily)
  r4ml.info(logSource, "\npowerOfVariance: " %++% powerOfVariance)
  r4ml.info(logSource, "\nlinkFunction: " %++% linkFunction)
  r4ml.info(logSource, "\npowerOfLink: " %++% powerOfLink)    
  
  # Check if scoring or testing should be done:
  # if the data has the label column, then it is a testing request, otherwise, it is a scoring request
  # get the featureNames from the coefficients, column #1 is always "(Intercept)" so exclude it.
  coefcn <- base::colnames(glm@coefficients)
  
  # compare the column name vector with the data's
  cn <- SparkR::colnames(data)
  
  # Skip the very last coefficient if intecrept=TRUE
  if (glm@intercept == TRUE) {
    coefcn <- coefcn[-1] 
  }
  
  # Clean up old results
  statsPath <- file.path(r4ml.env$WORKSPACE_ROOT("r4ml.glm"), "stats_predict.csv")
  
  # TEMP: build the coefficients.csv file
  args <- list(dml=file.path(r4ml.env$SYSML_ALGO_ROOT(), r4ml.env$DML_GLM_TEST_SCRIPT),
               B_full = as.r4ml.matrix(as.r4ml.frame(object@dmlOuts[['beta_out']], repartition = FALSE)),
               "means", # this is output $M
               O=statsPath,
               dfam=distFamily,
               vpow=powerOfVariance,
               link=linkFunction,
               lpow=powerOfLink,
               fmt="csv"
  )
  dmlOuts <- NULL
  
  # identify testing or scoring
  ycn <- glm@yColname
  matching <- match(ycn, cn)
  if (is.na(matching)) {
    # check whether this is for scoring
    if (identical(cn, coefcn)) {
      testing <- F
    }
    else {
      r4ml.err(logSource, "Invalid dataset is passed in.")
    }
  }
  else {
    cn <- cn[-matching]
    # check whether this is for testing
    if (identical(cn, coefcn)) {
      testing <- T
    }
    else {
      r4ml.err(logSource, "Invalid dataset is passed in.")
    }
  }
  
  if(testing) {
    XY <- r4ml.model.splitXY(data, glm@yColname)        
    testset_x <- XY$X
    testset_y <- XY$Y
    
    # INPUT PARAMETERS:
    # ---------------------------------------------------------------------------------------------
    # NAME  TYPE   DEFAULT  MEANING
    # ---------------------------------------------------------------------------------------------
    # X     String  ---     Location to read the matrix X of records (feature vectors)
    # B     String  ---     Location to read GLM regression parameters (the betas), with dimensions
    #                           ncol(X)   x k: do not add intercept
    #                           ncol(X)+1 x k: add intercept as given by the last B-row
    #                           if k > 1, use only B[, 1] unless it is Multinomial Logit (dfam=3)
    # M     String  " "     Location to write the matrix of predicted response means/probabilities:
    #                           nrow(X) x 1  : for Power-type distributions (dfam=1)
    #                           nrow(X) x 2  : for Binomial distribution (dfam=2), column 2 is "No"
    #                           nrow(X) x k+1: for Multinomial Logit (dfam=3), col# k+1 is baseline
    # Y     String  " "     Location to read response matrix Y, with the following dimensions:
    #                           nrow(X) x 1  : for all distributions (dfam=1 or 2 or 3)
    #                           nrow(X) x 2  : for Binomial (dfam=2) given by (#pos, #neg) counts
    #                           nrow(X) x k+1: for Multinomial (dfam=3) given by category counts
    # O     String  " "     Location to write the printed statistics; by default is standard output
    # dfam  Int     1       GLM distribution family: 1 = Power, 2 = Binomial, 3 = Multinomial Logit
    # vpow  Double  0.0     Power for Variance defined as (mean)^power (ignored if dfam != 1):
    #                       0.0 = Gaussian, 1.0 = Poisson, 2.0 = gamma, 3.0 = Inverse Gaussian
    # link  Int     0       Link function code: 0 = canonical (depends on distribution), 1 = Power,
    #                       2 = Logit, 3 = Probit, 4 = Cloglog, 5 = Cauchit; ignored if Multinomial
    # lpow  Double  1.0     Power for Link function defined as (mean)^power (ignored if link != 1):
    #                       -2.0 = 1/mu^2, -1.0 = reciprocal, 0.0 = log, 0.5 = sqrt, 1.0 = identity
    # disp  Double  1.0     Dispersion value, when available
    # fmt   String "text"   Matrix output format, usually "text" or "csv" (for matrices only)
    args <- c(args, 
              X=testset_x,
              Y=testset_y)
    
    if (!is.na(dispersion)) {
      args <- c(args, disp=dispersion)
    }
    args <- c(args, scoring_only="no")
    dmlOuts <- do.call("sysml.execute", args)  
  } else { #only scoring (no Y is passed)
    args <- c(args, 
              X=data)
    if (!is.na(dispersion)) {
      args <- c(args, disp=dispersion)
    }
    args <- c(args, scoring_only="yes")
    dmlOuts <- do.call("sysml.execute", args)
  }
  
  # If family is binomial, probabilities will be returned (two columns). Otherwise,
  # predictions themselves will be output
  colnames <- if (glm@family$family == "binomial") {
    c("class_0", "class_1")
  } else {
    glm@yColname
  }
  
  preds <- as.r4ml.matrix(as.r4ml.frame(dmlOuts[['means']], repartition = FALSE))
  # Output probabilities/predictions accordingly
  output <- if (glm@family$family == "binomial") {
    list("probabilities" = preds)
  } else {
    list("predictions" = preds)
  }
  
  # Add stats
  if (testing) {
    statsCsv <- SparkR::as.data.frame(r4ml.read.csv(statsPath, header=FALSE, stringsAsFactors=FALSE))
    colnames(statsCsv) <- c("name", "column", "scaled", "value")
  }
  else {
    statsCsv <- data.frame()
  }
  output <- c(output, list("statistics"=statsCsv))
  
  return(output)
}
#'@export
setMethod("predict", "r4ml.glm", predict.r4ml.glm)
