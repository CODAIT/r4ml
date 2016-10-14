# <img src="HydraR/inst/images/hydraR-logo.png" alt="IMAGE ALT TEXT HERE" width="340" height="250" />

# Table of Contents

-[How to install](#how-to-install)

&nbsp;&nbsp;-[Install Dependencies](#install-dependencies)

&nbsp;&nbsp;&nbsp;&nbsp;-[SparkR](#sparkr)

&nbsp;&nbsp;&nbsp;&nbsp;-[R packages](#r-packages)

&nbsp;&nbsp;-[Install HydraR](#install-hydrar)

&nbsp;&nbsp;&nbsp;&nbsp;-[HydraR development and usages](#hydrar-development-and_usages)

&nbsp;&nbsp;&nbsp;&nbsp;-[HydraR usages](#hydrar-usages)

-[How to Use HydraR](#how-to-use-hydrar)


## How to install

### Install Dependencies

#### SparkR

   1) If not installed, run the following scripts

   ```
   mkdir -p $HOME/apache
   pushd $HOME/apache
   # 2.0 maintenance branch with stability fixes on top of Spark 2.0.0
   git clone git://github.com/apache/spark.git -b branch-2.0
   cd spark
   mvn -DskipTests -Pnetlib-lgpl -Psparkr clean package
   cd ..
   popd
   ```

   2) first get the existing system library path by running on the terminal
   ```
    Rscript -e "paste(.libPaths(), collapse=':')"
   ```
   
   3) Add the following lines in `$HOME/.Renviron` (note: replace `<OUTPUT_FROM_PREVIOUS_CODE>` appropriately
   ```
   R_LIBS=<OUTPUT_FROM_PREVIOUS_CODE>:<HOME>/apache/spark/R/lib:$R_LIBS
   ```

   4) set the spark home environment in the `$HOME/.Renviron` file by adding the following line
   ```
   SPARK_HOME=$HOME/apache/spark
   ```
   5) Define `SPARKR_SUBMIT_ARGS` and `HYDRAR_CLIENT` in you R enviroment
   
   5.1) If you a running in local modeter also add the following to `$HOME/.Renviron` 
   ```
   SPARKR_SUBMIT_ARGS=" sparkr-shell"
   # 
   HYDRAR_CLIENT="local[*]" # optional
   ```
   
  5.2) if you a running on a cluster also add the following to `$HOME/.Renviron` (replace `<num_exe>` with the number of executors in your cluster)
   ```
   SPARKR_SUBMIT_ARGS=" --master yarn-client --num-executors <num_exe> sparkr-shell"
   HYDRAR_CLIENT="yarn-client"
   ```

#### R packages

   Install all the package which have dependencies and suggestion in the DESCRIPTION file using the 

   `install.packages(pkgname)` command in R
   One can see the content as follows
   ```
   cat HydraR/DESCRIPTION
   ```
  

### Install HydraR
One can follow one of the following instructions...

#### HydraR development and usages
  1) clone github and load it without installing

   ```
   mkdir -p $HOME/apache
   git clone https://github.com/SparkTC/spark-hydrar
   cd spark-hydrar
   bin/install-all.sh
   cd ..
   ```

  2) Add the following path to the R_LIBS in the `$HOME/.Renviron` file

   ```
  R_LIBS=<PREVIOUS_PATH_FOR_R_LIBRARY>:<PATH_FOR_SPARKR_LIB>:<HOME>/apache/spark-hydrar/lib:$R_LIBS 
   ```


#### HydraR usages

   1) Direct download as explained in the previous steps

    Same as the previous instruction step 1 i.e run the following command

   ```
   mkdir -p $HOME/apache
   git clone https://github.com/SparkTC/spark-hydrar
   pushd spark-hydrar
   bin/install-all.sh
   popd
   ```

   After that one can use the install packages to install into library

   ```
   pushd spark-hydrar
   Rscript -e 'install.packages("./lib/HydraR",repos=NULL, type="source")'
   popd
   ```
    


   2) github install (Currently not recommended as it will not install all the docs)

    Note that this will install in the standard R installation folder 

    ```
    library(devtools)
    install_github("sparkTC/spark-hydrar", subdir="HydraR")
    ```


## How to install

   1) We have the comprehensive examples at [examples folder](./HydraR/inst/examples)

   2) Here is very simple example of running the linear model by R script

   ```
    # load HydraR library
    library(HydraR)

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
    y_test = as.hydrar.matrix(as.hydrar.frame(test[,1]))
    y_test = SparkR:::as.data.frame(y_test)
    test = as.hydrar.matrix(as.hydrar.frame(test[,c(2:5)]))

    # create the linear model
    iris_lm <- hydrar.lm(Sepal_Length ~ . , data = train, method ="iterative")

    # check for the model accuracy by predicting on the test data set
    preds <- predict(iris_lm, test)
    preds
   ```

  
