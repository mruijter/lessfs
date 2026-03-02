#!/bin/bash

die_err() {
	echo "$0 ERROR: $@"
	exit 1
}

if [ n$1 = n ]; then
	die_err "Missing required argument lessfs mountpoint."
fi

pushd .
git clone https://github.com/pjd/pjdfstest.git || die_err "Failed to clone pjd-fstests"
cd pjdfstest || die_err "Failed to find pjdfstest after cloning"
autoreconf -fi .
./configure
make || die_err "Failed to compile pjd-fstests"
cd /$1 || exit 2
mkdir -p prove
cd prove
prove -r /devel/peak/repos/lessfs/regression/pjdfstest || die_err "One or more tests failed"
popd
[ -d pjdfstest ] && rm -rf pjdfstest
