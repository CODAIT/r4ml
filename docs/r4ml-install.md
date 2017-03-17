#<img src="../R4ML/inst/images/r4ml-logo.png" alt="R4ML Logo"/>

## __**How to install**__

### 1.0 __**Install Dependencies**__

#### 1.1 __**Install Core R**__

   Install R as per https://cloud.r-project.org/

#### 2.0 __**Install SparkR**__

##### 2.1 __**Install core SparkR**__

 You can follow one of the following steps 

  - 2.1.1: download prebuilt binary
  - 2.1.2: build your own or if you already have spark source code

##### 2.1.1 __**Direct SparkR download**__

   Download directly from spark resources. http://spark.apache.org/downloads.html

   Issue the following shell commands
   ```
   # assumming that the downloaded file is at /tmp/spark-2.1.0-bin-hadoop2.7.tgz
   mkdir -p ~/sparktc
   pushd ~/sparktc
   mv /tmp/spark-2.1.0-bin-hadoop2.7.tgz .

   # extract the spark binary 
   tar -xvzf spark-2.1.0-bin-hadoop2.7.tgz

   # rename/remove the appropriately
   mv spark-2.1.0-bin-hadoop2.7 spark; \rm -rf spark-2.1.0-bin-hadoop2.7.tgz

   popd
   ```
   

##### 2.1.2 __**Download Source and build it**__

   Execute the following shell script

   ```
   mkdir -p ~/sparktc
   pushd ~/sparktc

   # 2.1 maintenance branch with stability fixes on top of Spark 2.1.0
   git clone git://github.com/apache/spark.git -b branch-2.1

   # build the sparkr
   pushd spark; mvn -DskipTests -Pnetlib-lgpl -Psparkr clean package; popd

   popd
   ```

##### 2.2 __**Set up the environment path in the .Renviron**__

   A) first get the existing system library path by running on the terminal
   ```
    Rscript -e "paste(.libPaths(), collapse=':')"
   ```
   
   B) Add the following lines in `~/.Renviron` (note: replace `<OUTPUT_FROM_PREVIOUS_CODE>` appropriately
   ```
   # change <VAR> to the appropriate value
   R_LIBS=<OUTPUT_FROM_PREVIOUS_CODE>:~/sparktc/spark/R/lib:$R_LIBS
   ```

   C) Add the SPARK_HOME env variable in ~/.Renviron
   ```
   # SPARK_HOME=<SPARK_INSTALL_DIR>/spark
   SPARK_HOME=~/sparktc/spark
   ```

#### 3.0 __**Install R4ML's dependencies (R packages)**__

   Install all the as per the DESCRIPTION file of R4ML using the install.packages R cmd
   Here is the R shell command to achieve the above
   ```
   # start the R shell in the OS prompt
   bash>R
   # now you have the R shell, run the following R cmds
   R>

   # must have the following packages
   R>r4ml_mandatory_pkgs <- c("uuid", "R6", "survival")
   R>install.packages(r4ml_mandatory_pkgs)

   # the following packages are needed for dev, testing and documentations
   R>r4ml_dev_pkgs <- c("testthat", "knitr", "devtools", "roxygen2")
   R>install.packages(r4ml_dev_pkgs)

   R>quit("no")
   ```
  

### 4.0 __**Install R4ML**__

  A) clone github and install R4ML.

   ```
   mkdir -p ~/sparktc
   pushd ~/sparktc
   git clone https://github.com/SparkTC/R4ML R4ML
   pushd R4ML
   bin/install-all.sh
   popd
   popd
   ```

  B) Add the following path to the R_LIBS in the `$HOME/.Renviron` file

   ```
  # note: replace <VAR> with the appropriate value for your environment
  R_LIBS=<PREVIOUS_PATH_FOR_R_LIBRARY>:<PATH_FOR_SPARKR_LIB>:~/sparktc/R4ML/lib:$R_LIBS 
   ```
