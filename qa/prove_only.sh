#!/bin/bash
die_err() {
	echo "$0 ERROR: $*"
	exit 1
}

jdfsinstall_path() {
	install_path=$(pwd)
	filename=$(basename "$0")
	install_path=$(echo "${BASH_SOURCE[0]}" | sed "s/\/qa/~/g" | cut -d'~' -f1 | sed "s/$filename//g")
	if [ "$install_path" = "." ] || [ "$install_path" = "./" ]; then
		install_path=$(pwd)
	else
		install_path="$install_path/qa"
	fi
}

jdfsinstall_path
if [ n"$1" = n ]; then
	die_err "Missing required argument lessfs mountpoint."
fi

cd "$install_path" || die_err "chdir $install_path"
[ ! -d pjdfstest ] && git clone https://github.com/pjd/pjdfstest.git
cd pjdfstest || die_err "Failed to find pjdfstest after cloning"
autoreconf -fi .
./configure
make || die_err "Failed to compile pjd-fstests"
cd /"$1" || exit 2
mkdir -p prove
cd prove || die_err "chdir prove"
prove -r "$install_path/pjdfstest/tests" || die_err "One or more tests failed"
