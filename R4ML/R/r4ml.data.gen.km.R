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


#' @name hydrar.data.gen.km
#' @title Kaplan-Meier Data Generation
#' @export
#' @param N (numeric) number of observations
#' @param P (numeric) number of groups
#' @param weibA (numeric) Weibull shape parameter
#' @param weibB (numeric) Weibull scale parameter
#' @param IVeff (numeric) effects of factor levels (1 -> reference level)
#' @param obsLen (numeric) length of observation time
#' 
#' @return Generated Dataset
#' 
#' @examples \dontrun{
#' 
#' # Create Dataset
#' surv <- hydrar.data.gen.km(30)
#' 
#' # Transform data to hydrar matrix
#' survMatrix <- as.hydrar.matrix(surv)
#' 
#' # Establish formula for parsing
#' survFormula <- Surv(Timestamp, Censor) ~ Age
#' 
#' # Run kaplan meier on generated data
#' km <- hydrar.kaplan.meier(survFormula, data=survMatrix,
#'                           test=1, rho="none")
#'                           
#' # Produce Summary                          
#' summary <- summary(km)
#' 
#' # Compute Test Statistics
#' test = hydrar.kaplan.meier.test(km)
#' 
#' }
hydrar.data.gen.km <- function(N, P=3, weibA=1.5, weibB=100,
  IVeff=c(0, -1, 1.5), obsLen=120) {
  # N - number of observations
  # P - number of groups
  # weibA - Weibull shape parameter
  # weibB - Weibull scale parameter
  # IVeff - effects of factor levels (1 -> reference level)
  # obsLen - length of observation time
  
  # N mod P must be 0
  if (N %% P != 0){
    warning("N mod P must be 0 - reverting to floor of N/P * P.")
    N <- floor(N/P)*P
  }
  
  # Generate data for sex.
  sex <- factor(base::sample(c("f", "m"), N, replace=TRUE))
  
  # Generate continuous covariate
  X <- rnorm(N, 0, 1)
  
  # Generate factor covariate
  IV <- factor(rep(LETTERS[1:P], each=N/P))
  
  Xbeta  <- 0.7*X + IVeff[unclass(IV)] + rnorm(N, 0, 2)
  
  # Generate uniformly distributed random variable
  U <- runif(N, 0, 1)
  
  # Generate simulated event
  eventT <- ceiling((-log(U)*weibB*exp(-Xbeta))^(1/weibA))
  
  # Censoring time = end of study
  censT <- rep(obsLen, N)
  
  # observed censored event times
  obsT <- pmin(eventT, censT)
  
  # has evenet occured?
  status <- eventT <= censT
  
  # Construct dataframe
  dfSurv <- data.frame(obsT, status, sex, X, IV)
  dfSurv$Censor=as.numeric(status)
  dfSurv$sex = as.numeric(dfSurv$sex)
  dfSurv$IV = as.numeric(dfSurv$IV)
  dfSurv$status = as.numeric(dfSurv$status)
  names(dfSurv)[1] <- "Timestamp"
  dfSurv$Age <- ceiling(runif(N, 49, 52))
  dfSurv$Timestamp <- 1:N
  dfSurv$Race <- ceiling(runif(N, -1, 2))
  dfSurv$Origin <- ceiling(runif(N, -1, 2))
  
  # Return dataset
  as.hydrar.frame(dfSurv)
}


