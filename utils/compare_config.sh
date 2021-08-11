#! /bin/bash

if [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]] || [[ "$#" -ne 2 ]] ; then
	echo "This script is used to compare different KeyDB configuration files."
	echo ""
	echo "	Usage:  compare_config.sh [keydb1.conf] [keydb2.conf]"
	echo ""
	echo "Output: a side by side sorted list of all active parameters, followed by a summary of the differences."
	exit 0
fi

conf_1=$(mktemp)
conf_2=$(mktemp)

echo "----------------------------------------------------"
echo "--- display all active parameters in config files---"
echo "----------------------------------------------------"
echo ""
echo "--- $1 ---" > $conf_1
echo "" >> $conf_1
grep -ve "^#" -ve "^$" $1 | sort >> $conf_1
echo "--- $2 ---" >> $conf_2
echo "" >> $conf_2
grep -ve "^#" -ve "^$" $2 | sort >> $conf_2

pr -T --merge $conf_1 $conf_2

echo ""
echo ""
echo "--------------------------------------------"
echo "--- display config file differences only ---"
echo "--------------------------------------------"
echo ""

sdiff --suppress-common-lines $conf_1 $conf_2

rm $conf_1
rm $conf_2

exit 0
