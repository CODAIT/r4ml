# HydraR
<img src="HydraR/inst/images/hydraR-logo.png" 
alt="IMAGE ALT TEXT HERE" width="240" height="180" />

## How to install

### Install Dependencies
#### 1) SparkR
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

   2)Add the following lines in $HOME/.Renviron
   ```
   R_LIBS=$HOME/apache/spark/R/lib:$R_LIBS
   ```

#### 2) R packages
   Install all the package which have dependencies and suggestion in the DESCRIPTION file using the 
   `install.packages(pkgname)` command in R
   One can see the content as follows
   ```
   cat HydraR/DESCRIPTION
   ```
### Install Instruction (follow one of the following instruction)

#### For development. clone github and load it without installing
  1)
   ```
   mkdir -p $HOME/apache
   git clone https://github.com/SparkTC/spark-hydrar
   cd spark-hydrar
   bin/install-all.sh
   cd ..
   ```
  2) Add the following path to the R_LIBS
   ```
  R_LIBS=$HOME/apache/spark/R/lib:$HOME/apache/spark-hydrar/lib/HydraR:$R_LIBS 
   ```

#### for user 

   1) Direct download as explained in the previous steps

    After step 1, one can install using install.packages command in R

   2) github install
    Note that this will install in the standard R installation folder
    ```R
    library(devtools)
    install_github("sparkTC/spark-hydrar", subdir="HydraR")
    ```


