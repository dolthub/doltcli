#!/bin/bash

set -xeou pipefail

DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
BASE=$DIR/..

USERNAME=Dolthub
PASSWORD=

poetry config http-basic.pypi $USERNAME $PASSWORD

#cd $BASE
#poetry build
#poetry publish -r doltcli $@
