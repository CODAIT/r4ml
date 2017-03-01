# <img src="../HydraR/inst/images/hydraR-logo.png" alt="HydraR Logo"/>

## __**How to Use HydraR**__

   Congratulations, you just installed HydraR, now you are ready to do scalable
   data analysis and machine learning.

   1) We have the comprehensive examples at [examples folder](../HydraR/inst/examples)

   2) Also we have many vignettes for user to use at [vignettes](../HydraR/vignettes)
 
   3) Our unit-test also provide great examples at [unittests](../HydraR/tests/testthat)

   4) Here is very simple example of running the linear model by R script

   ```
    # load HydraR library
    library(HydraR)
    
    sparkR.session(master = "local[*]", sparkHome = "/path/to/apache/spark")

    # data cleanup and pre-processing
    df <- iris
    df <- hydrar.ml.preprocess(as.hydrar.frame(df),
             transformPath = "/tmp",
             recodeAttrs="Species")$data
    iris_df <- as.hydrar.frame(df)
    iris_mat <- as.hydrar.matrix(iris_df)
    ml.coltypes(iris_mat) <- c("scale", "scale", "scale", "scale", "nominal") 

    # split into train and test data set
    s <- hydrar.sample(iris_mat, perc=c(0.2,0.8))
    test <- s[[1]]
    train <- s[[2]]
    y_test <- as.hydrar.matrix(test[, 1])
    y_test = SparkR:::as.data.frame(y_test)
    test <- as.hydrar.matrix(test[, c(2:5)])

    # create the linear model
    iris_lm <- hydrar.lm(Sepal_Length ~ . , data = train, method ="iterative")

    # check for the model accuracy by predicting on the test data set
    preds <- predict(iris_lm, test)
    preds
    
    sparkR.session.stop()
   ```

  
