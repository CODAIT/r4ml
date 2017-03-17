#!/bin/bash

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

# Script to create API docs for R4ML
# This requires `devtools` and `knitr` to be installed on the machine.

# After running this script the html docs can be found in 
# $R4ML_HOME/R4ML/html


set -o pipefail
set -e

# Figure out where the script is
export FWDIR="$(cd "`dirname "$0"`/../"; pwd)"
pushd $FWDIR

# Install the package (this will also generate the Rd files)
$FWDIR/bin/create-docs.sh


# run all the relevant test cases
$FWDIR/bin/test-all.sh

# now finally re-install the package so as to have all the docs and other info
$FWDIR/bin/install-dev.sh

# currently, the html folder is not copied to the lib dir. So we manually do it
echo "Coping the html folder to lib"
\cp -rf $FWDIR/R4ML/html/* $FWDIR/lib/R4ML/html
