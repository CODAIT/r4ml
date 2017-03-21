# Install IOP
To setup your cluster & install IOP follow the directions found here:

https://developer.ibm.com/hadoop/2016/09/06/ibm-open-platform-with-apache-hadoop-and-biginsights-4-3-technical-preview/

**Important:** pay close attention to the known issues especially the point about **disabling YARN Timeline server**

# Install R on each node
Run the following commands on each node in your cluster.

1. `yum install epel-release`
2. `yum install libcurl-devel openssl-devel blas-devel lapack-devel texinfo-tex libicu-devel`
3. `yum install R`


If you are installing on a cluster with many nodes consider using the `pssh` package to speed up the process. (https://linux.die.net/man/1/pssh)

# Install RStudio Server (optional)
To make running R more user friendly consider install RStudio server on your root node.

https://www.rstudio.com/products/rstudio/download-server/

# Setup R on your root node
Follow the instructions found here to setup R on your root node:

https://github.com/SparkTC/spark-hydrar#how-to-install

**Note:** `R` needs to be installed on each node however you only need to follow the above steps on the node you will be using to run jobs.

# Examples

Here is an example shell script that can be run from your root node
```
### Install R ###
yum install epel-release
yum install libcurl-devel openssl-devel blas-devel lapack-devel texinfo-tex libicu-devel
yum install R

### Install RStudio Server ###
wget https://download2.rstudio.org/rstudio-server-rhel-0.99.903-x86_64.rpm
yum install --nogpgcheck rstudio-server-rhel-0.99.903-x86_64.rpm
rm rstudio-server-rhel-0.99.903-x86_64.rpm

### Install Ambari ###
wget http://birepo-build.svl.ibm.com/repos/Ambari/RHEL6/x86_64/2.2.0_IOP-4.3.0.0-TP/build.latest/ambari.repo
mv ambari.repo /etc/yum.repos.d/
yum install ambari-server             

ambari-server setup
ambari-server start

# To continue setup login to ambari on port 8080 with user: admin / password: admin

### Setup R on all the nodes ###
yum install pssh
# create a text file containing a list of all the nodes in your cluster named nodes

pssh -h nodes yum install epel-release -y
pssh -h nodes yum install libcurl-devel openssl-devel blas-devel lapack-devel texinfo-tex libicu-devel -y
pssh -h nodes yum install R -y
```

Sample .Renviron
```
R_LIBS=/usr/lib64/R/library:/usr/share/R/library:/usr/iop/current/spark2-client/R/lib:/home/data-scientist/lib:$R_LIBS
SPARK_HOME=/usr/iop/current/spark2-client
SPARK_HOME_VERSION="2.0.0"
HYDRAR_CLIENT="yarn-client"
SPARK_DRIVER_MEMORY="4G"
SPARKR_SUBMIT_ARGS="--master yarn --deploy-mode client --driver-memory 4G --executor-memory 5G --num-executors 48 --executor-cores 1 --conf spark.yarn.executor.memoryOverhead=512 sparkr-shell"
```

