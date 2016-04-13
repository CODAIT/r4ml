# HydraR

## How to install

### Dependencies
#### SparkR
   1) If not installed, run the following scripts

   mkdir -p $HOME/apache
   pushd $HOME/apache
   git clone https://github.com/apache/spark
   cd spark
   mvn -DskipTests -Pnetlib-lgpl -Psparkr clean package
   cd ..
   popd

   2)Add the following lines in $HOME/.Renviron
   R_LIBS=$HOME/apache/spark/R/lib:$R_LIBS

### Install Instruction (follow one of the following instruction)

#### For development. clone github and load it without installing
  1)
   mkdir -p $HOME/apache
   git clone https://github.com/SparkTC/spark-hydrar
   cd spark-hydrar
   bin/install-all.sh
   cd ..
  2) Add the following path to the R_LIBS
  R_LIBS=$HOME/apache/spark/R/lib:$HOME/apache/spark-hydrar/lib/HydraR:$R_LIBS 

#### for user
After step 1, one can install using install.packages.

  
