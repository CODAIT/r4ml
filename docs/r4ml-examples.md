# <img src="../HydraR/inst/images/hydraR-logo.png" alt="HydraR Logo"/>

## __**How to Use HydraR**__

   Congratulations, you just installed HydraR, now you are ready to do scalable
   data analysis and machine learning.

   1) We have the comprehensive examples at [examples folder](../HydraR/inst/examples)

   2) Also we have many vignettes for user to use at [vignettes](../HydraR/vignettes)
 
   3) Our unit-test also provide great examples at [unittests](../HydraR/tests/testthat)

   4) Here is very simple example of running the linear model by R script

   ```
    # In this example, we're going to use iris data set, and we'll use linear regression
    # model to predict Sepal Length using the rest of the fearures as predictors.

    # load HydraR library
    library(HydraR)

    hydrar.session(master = "local[*]", sparkHome = "/path/to/apache/spark")

    # create hydrar dataframe from iris data
    iris.df <- as.hydrar.frame(iris)

    # We need to dummycode Species for regression model using preprocessing
    # this returns a list containing $data $metadata
    pp <- hydrar.ml.preprocess(iris.df, transformPath = "/tmp",
                               dummycodeAttrs = c("Species"))

    # convert preprocessed data to hydrar.matrix
    iris.mat <- as.hydrar.matrix(pp$data)

    # create train/test split
    ml.coltypes(iris.mat) <- c("scale", "scale", "scale", "scale",
                               "nominal", "nominal", "nominal")
    s <- hydrar.sample(iris.mat, perc=c(0.2,0.8))
    test <- s[[1]]
    train <- s[[2]]

    # fit linear regression model.
    iris.model <- hydrar.lm(Sepal_Length ~ . , data = train, intercept = TRUE)

    # we can check the coefficients of model by using coef function
    coef(iris.model)

    # we can now make predictions using test data set
    preds <- predict(iris.model, test)

    # the predict function calculates several statitics just like hydrar.lm.
    preds$statistics

    hydrar.session.stop()
   ```
