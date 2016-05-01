# <img src="HydraR/inst/images/hydraR-logo.png" alt="IMAGE ALT TEXT HERE" width="340" height="250" />

# Table of Contents

-[How to install](#how-to-install)

&nbsp;&nbsp;-[Install Dependencies](#install-dependencies)

&nbsp;&nbsp;&nbsp;&nbsp;-[SparkR](#sparkr)

&nbsp;&nbsp;&nbsp;&nbsp;-[R packages](#r-packages)

&nbsp;&nbsp;-[Install HydraR](#install-hydrar)

&nbsp;&nbsp;&nbsp;&nbsp;-[HydraR development and usages](#hydrar-development-and_usages)

&nbsp;&nbsp;&nbsp;&nbsp;-[HydraR usages](#hydrar-usages)



## How to install

### Install Dependencies

#### SparkR

   1) If not installed, run the following scripts

   ```
   mkdir -p $HOME/apache
   pushd $HOME/apache
   git clone https://github.com/apache/spark
   cd spark
   mvn -DskipTests -Pnetlib-lgpl -Psparkr clean package
   cd ..
   popd
   ```

   2) first get the existing system library path by running on the terminal
   ```
    Rscript -e "paste(.libPaths(), collapse=':')"
   ```
   
   3)Add the following lines in $HOME/.Renviron (note: replace <OUTPUT_FROM_PREVIOUS_CODE> appropriately
   ```
   R_LIBS=<OUTPUT_FROM_PREVIOUS_CODE>:$HOME/apache/spark/R/lib:$R_LIBS
   ```
    

   4) set the spark home environment in the `$HOME/.Renviron` file by adding the following line

   ```
   SPARK_HOME=$HOME/apache/spark
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
  R_LIBS=$HOME/apache/spark/R/lib:$HOME/apache/spark-hydrar/lib:$R_LIBS 
   ```


#### HydraR usages

   1) Direct download as explained in the previous steps

    Same as the previous instruction step 1 i.e run the following command

   ```
   mkdir -p $HOME/apache
   git clone https://github.com/SparkTC/spark-hydrar
   cd spark-hydrar
   bin/install-all.sh
   cd ..
   ```

   After that one can use the install packages to install into library

   ```
   Rscript -e 'install.packages("./lib/HydraR",repos=NULL, type="source")'
   ```
    


   2) github install (Currently not recommended)

    Note that this will install in the standard R installation folder 

    ```
    library(devtools)
    install_github("sparkTC/spark-hydrar", subdir="HydraR")
    ```
