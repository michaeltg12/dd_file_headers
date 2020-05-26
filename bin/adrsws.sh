#!/bin/sh
VERSION="20181115.0"
TMP_BASE="/tmp"
ADRSWS="https://adc.arm.gov/cgi-bin/adrsws.py"
ADHZIP="https://adc.arm.gov/docs/arm-download-helper.zip"

ACCESS_KEY="zovi7wXTsRvhjuAx"

# Subroutines
usage()
{
    echo
    echo "adrsws.sh -- Version: ${VERSION}"
    echo "Usage:"
    echo "  $0 "
    echo "    [-u <arch user ID> "
    echo "      [-t <request type>] "
    echo "      [-d <datastream name> -s <start date> -e <end date>] "
    echo "      [-v] [-n] [<input file>]"
    echo "      [-p]"
    echo "      [-g <download destination directory>]"
    echo "    ] | [-a <email address> -t userid]"
    echo
    echo "  Options:"
    echo "    a - email address of user "
    echo "    d - complete data stream name"
    echo "    g - \"get\" files automatically into dest. directory"
    echo "    n - no cleanup; Temp dir containing JSON object not removed"
    echo "    p - plain text output, instead of JSON"
    echo "        (Note: applies only to -t dslist and -t flist)"
    echo "    t - Request types: dslist, flist, order, userid"
    echo "        (Note: flist requires -d, -s and -e, with YYYY-mm-dd date"
    echo "         formats; If -t not specified, \"order\" is default.)"
    echo "    v - Keep the version designations on the filename(s)"
    echo
    echo "  Examples: "
    echo "    $0 -a wwallace@ornl.gov -t userid"
    echo "    $0 -u wwallace files-list.txt"
    echo "    $0 -u wwallace -v files-list.txt"
    echo "    cat files-list.txt | $0 -u wwallace"
    echo "    $0 -u wwallace -t dslist"
    echo "    $0 -u wwallace -t flist -d enamplpolfsC1.b1 -s 2015-01-01 -e 2016-03-31 -v"
    echo "    $0 -u wwallace -t flist -d enamplpolfsC1.b1 -s 2015-01-01 -e 2016-03-31 -p | $0 -u wwallace -g /home/wallace/arm-downloads"
    echo
    echo "More infomration: https://adc.arm.gov/docs/adrsws.html"
    echo
}

cleanup_exit()
{
    exit_code=$1

    # Remove lock dir, if it belongs to this process
    prg=$(basename $0 | cut -d\. -f1)
    tmpd="${TMP_BASE}/${prg}.$$"
    if [ -d "${tmpd}" ]; then
        rm -rf ${tmpd}
    fi
    exit ${exit_code}
}

downloadData()
{
    local auid=$1; shift
    local oid=$1; shift
    local dldir=$1; shift
    local tdir=$1; shift

    echo "Setting up automatic download of files to ${dldir} ..."

    # Get the arm-download-helper.sh script
    echo "Downloading arm-download-helper zip file ..."
    curl -k -s -o ${tmp_dir}/arm-download-helper.zip ${ADHZIP}
    if [ $? -ne 0 ]; then
        echo
        echo "ERROR"
        echo "  Failed to get ${ADHZIP}"
        echo "  Please download manually!"
    fi

    echo "Unzipping arm-download-helper.zip ..."
    (cd ${tmp_dir}; unzip arm-download-helper.zip)

    echo "Invoking arm-download-helper.sh script ..."
    if [ -d ${dldir} ]; then
        ${tmp_dir}/arm-download-helper.sh ${auid} ${oid} ${dldir}
        if [ $? -ne 0 ]; then
            return 1
        fi
    else
        echo "ERROR"
        echo "  ${dldir} does not exist!"
        return 1
    fi

    return 0
}


#
# Main
#
if [ $# -eq 0 ]; then
    usage
    exit 0
fi

# Initialize some of the flags and variables
ret_verf="n"
debug=0
plaintxt=0
prg=$(basename $0 | cut -d\. -f1)
tmp_dir="${TMP_BASE}/${prg}.$$"

while getopts "a:d:e:g:pt:s:nu:v" o
do
    case $o in
        a)  email=$OPTARG
            ;;
        d)  dsname=$OPTARG
            ;;
        e)  edate=$OPTARG
            ;;
        g)  dldir=$OPTARG
            ;;
        p)  plaintxt=1
            ;;
        n)  debug=1
            ;;
        s)  sdate=$OPTARG
            ;;
        t)  reqty=$OPTARG
            ;;
        u)  armuser=$OPTARG
            ;;
        v)  ret_verf="y"
            ;;
        *)  usage
            exit 1
    esac
done

cnt=1
while [ $cnt -le $OPTIND ]
do
    if [ $cnt -eq $OPTIND ]; then
        inp_file=$1
        break
    fi
    shift
    cnt=`expr $cnt + 1`
done

if [ "${armuser:-xxx}" = "xxx" ]; then
    if [ "${email:-xxx}" != "xxx" -a "${reqty}" = "userid" ]; then
        echo > /dev/null
    else
        echo
        echo "ERROR"
        echo "  Required argument missing: -u <arch user ID>"
        usage
        exit 1
    fi
else
    armuser=$(echo $armuser | tr [:upper:] [:lower:])
fi

if [ "${reqty:-xxx}" = "xxx" ]; then
    reqty="order"
else
    if [ "${reqty}" = "flist" ]; then
        if [ "${dsname:-xxx}" = "xxx" ]; then
            echo
            echo "ERROR"
            echo "  Required argument missing: -d <datastream name>"
            usage
            exit 1
        fi
        if [ "${sdate:-xxx}" = "xxx" ]; then
            echo
            echo "ERROR"
            echo "  Required argument missing: -s <start date>"
            usage
            exit 1
        fi
        if [ "${edate:-xxx}" = "xxx" ]; then
            echo
            echo "ERROR"
            echo "  Required argument missing: -e <end date>"
            usage
            exit 1
        fi
    fi

    if [ "${reqty}" != "order" ]; then
        inp_file="/dev/null"
    fi
fi


# Check if the program "curl" exists
which curl > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo
    echo "ERROR"
    echo "  \"curl\" program not installed or accessible via current PATH definition!"
    echo
    exit 1
fi

# Clean up, if interrupted
trap "cleanup_exit 1" INT QUIT KILL TERM

# Make temporary directory
mkdir ${tmp_dir}

if [ "${inp_file:-xxx}" = "xxx" ]; then
    # Read from stdin
    inp_file=${tmp_dir}/input.txt
    while read line
    do
        echo $line >> ${inp_file}
    done
fi

if [ ! -e "${inp_file}" -a "${inp_file}" != "/dev/null" ]; then
    echo
    echo "ERROR"
    echo "  ${inp_file}: No such file!"
    echo
    cleanup_exit 1
fi

cat - > ${tmp_dir}/request.json << EOF
{
  "accesskey": "${ACCESS_KEY}",
EOF

if [ -f "${inp_file}" ]; then
    echo "  \"userid\": \"${armuser}\"," >> ${tmp_dir}/request.json
    echo "  \"withvers\": \"${ret_verf}\"," >> ${tmp_dir}/request.json
    echo "  \"reqtype\": \"${reqty}\"," >> ${tmp_dir}/request.json
    echo "  \"fileslist\": [" >> ${tmp_dir}/request.json

    while read line
    do
    	/bin/echo -n ",\"${line}\"" >> ${tmp_dir}/listelems.txt
    done < ${inp_file}

    sed -i 's/^,//' ${tmp_dir}/listelems.txt
    cat ${tmp_dir}/listelems.txt >> ${tmp_dir}/request.json
    echo "]" >> ${tmp_dir}/request.json
elif [ "${reqty}" = "flist" ]; then
    echo "  \"userid\": \"${armuser}\"," >> ${tmp_dir}/request.json
    echo "  \"withvers\": \"${ret_verf}\"," >> ${tmp_dir}/request.json
    echo "  \"reqtype\": \"${reqty}\"," >> ${tmp_dir}/request.json
    echo "  \"datastream\": \"${dsname}\"," >> ${tmp_dir}/request.json
    echo "  \"startdate\": \"${sdate}\"," >> ${tmp_dir}/request.json
    echo "  \"enddate\": \"${edate}\"" >> ${tmp_dir}/request.json
elif [ "${reqty}" = "dslist" ]; then
    echo "  \"userid\": \"${armuser}\"," >> ${tmp_dir}/request.json
    echo "  \"withvers\": \"${ret_verf}\"," >> ${tmp_dir}/request.json
    echo "  \"reqtype\": \"${reqty}\"" >> ${tmp_dir}/request.json
elif [ "${reqty}" = "userid" ]; then
    echo "  \"reqtype\": \"${reqty}\"," >> ${tmp_dir}/request.json
    echo "  \"email\": \"${email}\"" >> ${tmp_dir}/request.json
fi

echo "}" >> ${tmp_dir}/request.json

# Call curl
cmd_str="curl -s -k --data-urlencode reqjson@${tmp_dir}/request.json ${ADRSWS}"
if [ $reqty = "dslist" -a $plaintxt -eq 1 ]; then
    $cmd_str | sed 's/^.*"datastreams": \[\(.*\)\].*/\1/' | sed -e 's/", "/\n/g' -e 's/"//g' -e '/^$/d'
    rc=$?
elif [ $reqty = "flist" -a $plaintxt -eq 1 ]; then
    $cmd_str | sed 's/^.*"files": \[\(.*\)\].*/\1/' | sed -e 's/", "/\n/g' -e 's/"//g' -e '/^$/d'
    rc=$?
else
    $cmd_str > ${tmp_dir}/result.json
    rc=$?
    cat ${tmp_dir}/result.json

    if [ $reqty = "order" ]; then
        oid=$(cat ${tmp_dir}/result.json | tr -d '{} ' | \
              awk -F, 'BEGIN {id=-1} {for (i=1; i<=NF; i++) {split($i, a, ":"); \
                       if (a[1] ~ /orderid/) {id=a[2]}}} END {printf("%s", id)}' |\
                       tr -d '"')
        echo
        echo "Order successfully placed. Order ID: ${oid}"
    fi

    # Check and see if "-g" flag was supplied to automatically
    # invoke arm-download-helper.sh script.
    if [ "X${dldir}" != "X" ]; then
        downloadData ${armuser} ${oid} "${dldir}" "${tmp_dir}"
        if [ $? -eq 0 ]; then
            echo "Successfully downloaded all files."
        else
            echo "ERROR"
            echo "  Failed to download data. Please download manually!"
        fi
    fi
fi

if [ $rc -ne 0 ]; then
    echo
    echo "ERROR"
    echo "  Failed executing curl on ${ADRSWS}!"
    echo
    cleanup_exit 1
fi

if [ $debug -eq 0 -a -d ${tmp_dir} ]; then
    rm -rf ${tmp_dir}
fi

exit 0
