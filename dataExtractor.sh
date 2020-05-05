# /*
# * Copyright 2020 American Express Travel Related Services Company, Inc.
# *
# * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# * in compliance with the License. You may obtain a copy of the License at
# *
# * http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software distributed under the License
# * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# * or implied. See the License for the specific language governing permissions and limitations under
# * the License.
# *
# */

##**************************************************************************
#!/bin/bash -
#title          :dataExtractor.sh
#description    :Extract Data from any Big Data table in multiple formats
#developer      :Sheet Pangasa, Rajesh Munjal, Mayank Sharma
#Maintainer     :Sheet Pangasa, Rajesh Munjal, Swadhin (Dean) Jain
#date           :20190128
#version        :1.0.0
#usage          :sh DataExtractor.sh
##**************************************************************************

#Source config variable
configFile=$(dirname $0)/config.cfg

if [ -f "$configFile" ]; then
    . "$configFile"
else
    echo "Can't source file $configFile. Exiting Process"
    exit 1
fi


##**************************************************************************
## Read Input Arguments
##**************************************************************************

while [[ $# -gt 1 ]]; do
    key="$1"

    case "$key" in
    -s | --schema)
        schemaName="$2"
        shift
        ;;
    -t | --tablename)
        tableName="$2"
        shift
        ;;
    -md | --moveoutputdirectory)
        moveoutputDirectory="$2"
        shift
        ;;
    -f | --filename)
        filename="$2"
        shift
        ;;
    -c | --filtercondition)
        conditionStatement="$2"
        shift
        ;;
    -ex | --excludecolumn)
        excludeColumn="$2"
        shift
        ;;
    -m | --mergefile)
        mergeFile="$2"
        shift
        ;;
    -hr | --generateheader)
        generateHeader="$2"
        shift
        ;;
    -tr | --generatetrailer)
        generateTrailer="$2"
        shift
        ;;
    -dl | --delimiter)
        fileDelimiter="$2"
        shift
        ;;
    -hdl | --headerdelimiter)
        fileDelimiterHeader="$2"
        shift
        ;;
    -hd | --headerdate)
        addDateHeader="$2"
        shift
        ;;
    --default)
        DEFAULT=YES
        ;;
    *)
        # unknown option'
        ;;
    esac
    shift # past argument or value
done

tempFilesDir=$basePath/tempfiles

mkdir -p $tempFilesDir

dataGeneratorQueries=$tempFilesDir/dataGeneratorQueries
mkdir -p $dataGeneratorQueries

outputDirectory=$basePath/datageneratoroutput/$(date +%Y%m%d)/$tableName
mkdir -p $outputDirectory

echo "Arguments..."
echo "$separator"
echo
echo "Schema Name: $schemaName"
echo "Table Name: $tableName"
echo "Output Directory: $outputDirectory"
echo "Move Output Directory: $moveoutputDirectory"
echo "Condition Statement: $conditionStatement"
echo "Excluded Columns: $excludeColumn"
echo "Merge Files: $mergeFile"
echo "Generate Headers: $generateHeader"
echo "Generate Trailers: $generateTrailer"
echo "Data Delimiter: $fileDelimiter"
echo "Header Delimiter $fileDelimiterHeader"
echo
echo "$separator"

##**************************************************************************
## Check if mandatory variables have been provided
##**************************************************************************

if [ -z "$schemaName" ] || [ -z "$tableName" ] || [ -z "$mergeFile" ] || [ -z "$moveoutputDirectory" ]; then
    echo "Provide all the mandatory arguments"
    exit 1
fi

##**************************************************************************
##Check if the table exists in schema
##**************************************************************************
echo "Checking for availability of $tableName in $schemaName"
hive --silent -e "DESC "$schemaName"."$tableName"" >>/dev/null 2>&1
err=$?
if [ ! $err -eq 0 ]; then
    echo "Table "$tableName" does not exists in "$schemaName". Please check and rerun with correct table"
    exit $err
else
    echo "Table "$tableName" exists in "$schemaName""
fi

##**************************************************************************
##replace "{DATE}{TIME}" in outputDirectory with current date/time. strip off spaces/tabs(just in case).
##**************************************************************************

outputDirectory=$(sed "s/\s//g;s/{DATE}/$(date +%Y%m%d)/g;s/{TIME}/$(date +%H%M%S)/g" <<< $outputDirectory)

##**************************************************************************
##Check if output directory has an absolute path
##**************************************************************************
if [[ $outputDirectory == /* ]]; then
    echo "Absolute Ouput Directory Path found"
else
    echo "Provide absolute directory path for the output files"
    exit 1
fi

##**************************************************************************
##Check if the output directory already exists and is empty
##**************************************************************************
if [ -d "$outputDirectory" ]; then
    echo "$outputDirectory exists"
    ls -lhd "$outputDirectory"
    if [ ! "$(ls -A $outputDirectory)" ]; then
        echo "$outputDirectory is empty"
    else
        if [ "$moveoutputDirectory" == "Y" ]; then
            outputDirectoryBkp=${outputDirectory}_$(date +%Y%m%d%H%M%S)
            echo "Moving output directory $outputDirectory to $outputDirectoryBkp"
            mv $outputDirectory $outputDirectoryBkp

            if [ $? -ne 0 ]; then
                echo "Failed to move output directory $outputDirectory to $outputDirectoryBkp"
                exit 1
            fi
        else
            echo "$outputDirectory is not empty. The process overwrites any existing directories hence exiting the process. Please make sure the directory is empty"
            exit 1
        fi
    fi
fi



##**************************************************************************
##Check if filename is given when mergeFile="Y"
##**************************************************************************
if [ "$mergeFile" == "Y" ] && [ -z "$filename" ]; then
    echo "Filename required when merge is enabled"
    exit 1
fi


##**************************************************************************
##Check if header needs to be added
##**************************************************************************
if [ "$generateHeader" == "Y" ]; then
    echo "Header Output is Enabled"
    headerCommand='set hive.cli.print.header=true;'
else
    echo "Header Output is Disabled"
    headerCommand='set hive.cli.print.header=false;'
fi

##**************************************************************************
##Check the file data delimiter
##**************************************************************************
if [ -z "$fileDelimiter" ]; then
    fileDelimiter='\001'
    echo "Data Delimiter is set to default: ^A"
else
    fileDelimiter=$fileDelimiter
fi

##**************************************************************************
##Check the header delimiter
##**************************************************************************
if [ -z "$fileDelimiterHeader" ]; then
    fileDelimiterHeader='\x01'
    echo "Header Delimiter is set to default: ^A"
else
    fileDelimiterHeader=$fileDelimiterHeader
fi

echo "Delimiter is set to: $fileDelimiter"

##**************************************************************************
##calculate date & time
##**************************************************************************
dateTime=$(date +%Y%m%d)

##**************************************************************************
##if filename is passed, replace "{DATE}{TIME}" in filename with current date/time. strip off spaces/tabs(just in case).
##**************************************************************************
if [ -n "$filename" ]; then
    filename=$(sed "s/\s//g;s/{DATE}/$(date +%Y%m%d)/g;s/{TIME}/$(date +%H%M%S)/g" <<< $filename)
fi

##**************************************************************************
##Generate query templates
##**************************************************************************
templateInsertQuery="INSERT OVERWRITE LOCAL DIRECTORY '"$outputDirectory"' ROW FORMAT DELIMITED FIELDS TERMINATED BY '"$fileDelimiter"' NULL DEFINED AS '"''"' SELECT "

##**************************************************************************
##Get List of Columns from the Table
##**************************************************************************
columnList=$(hive --silent -e "SHOW COLUMNS in "$schemaName"."$tableName"")
err=$?
if [ ! $err -eq 0 ]; then
    echo "Failed to fetch column list from $schemaName.$tableName"
    exit $err
fi

##**************************************************************************
##Replace pipe with new line in excludeColumns
##**************************************************************************
excludeColumn=$(echo $excludeColumn | sed 's/|/\n/g')
err=$?
if [ ! $err -eq 0 ]; then
    echo "Failed to fetch exclude column list"
    exit $err
fi

echo "Following attributes will be excluded from the data extract: "$excludeColumn""

##**************************************************************************
##Replace exclude columns from the list of columns
##**************************************************************************
if [ -z "$excludeColumn" ]
then
    columnList=$(echo "$columnList" | sed 's/$/,/g' | sed '$s/,$//')
    err=$?
    if [ ! $err -eq 0 ]; then
        echo "An Error has occurred in step ColumnList"
        exit $err
    fi
else
    columnList=$(echo "$columnList" | egrep -v -w -i "$excludeColumn" | sed 's/$/,/g' | sed '$s/,$//')
    err=$?
    if [ ! $err -eq 0 ]; then
        echo "An Error has occurred in step ColumnList"
        exit $err
    fi
fi

echo "Final Column list is "$columnList""
fileHeaders=$(echo $columnList | sed "s^,^$fileDelimiterHeader^g" | sed 's/ //g')
err=$?
if [ ! $err -eq 0 ]; then
    echo "Failed to fetch file headers"
    exit $err
fi
echo "file headers: "$fileHeaders""

##**************************************************************************
##CSV Logic
##**************************************************************************
if [ "$fileDelimiter" == "," ]; then
    mkdir -p $outputDirectory
    extractionDB=$extractionDB

    #Create a temporary table for CSV type
    epochDate=$(date +%s)

    tempCsvTable="$extractionDB"."$tableName"_csv_"$epochDate"
    tempCsvTableRelativeLocation="$tableName"_csv_"$epochDate"
    extractionLocation="$extractionLocation"/"$tempCsvTableRelativeLocation/"
    csvTableQuery="CREATE TABLE "$tempCsvTable" ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (\"separatorChar\" = \",\",\"quoteChar\" = \"\\\"\") STORED AS TEXTFILE LOCATION '"$extractionLocation"' as SELECT $columnList from $schemaName.$tableName $conditionStatement"
    queryFile="$tempFilesDir/dataGeneratorQueries/$(date +%s)_$$.hql"
    echo "$csvTableQuery" >>"$queryFile"
    # Execute Hive Query Here
    hive --hiveconf $headerCommand --hiveconf mapred.job.queue.name=$QUEUE_NAME -f $queryFile
    err=$?
    if [ ! $err -eq 0 ]; then
        echo "Failed to execute $queryFile via executor"
        exit $err
    fi
    # Move the data to output location
    sleep 20
    cp -v "$extractionLocation"* "$outputDirectory/"
    err=$?
    if [ ! $err -eq 0 ]; then
        echo "Failed to copy data to $outputDirectory"
        exit $err
    fi
    echo "Calling Merge Header Trailer Function"
    echo "Checking if file merge is selected"
    if [ "$mergeFile" == "Y" ]; then
        echo "File Merge: Enabled"
        cat "$outputDirectory"/* >>"$outputDirectory"/"$filename"
        err=$?
        if [ ! $err -eq 0 ]; then
            echo "Failed to merge files"
            exit $err
        fi
        rm --verbose -f "$outputDirectory"/0*
        err=$?
        if [ ! $err -eq 0 ]; then
            echo "Failed to remove files from $outputDirectory"
            exit $err
        fi

        #check if header & trailer are enabled
        if [ "$generateHeader" == "Y" ] && [ "$generateTrailer" == "Y" ]; then
            echo "Header and Trailers are enable"
            recordCount=$(wc -l <$outputDirectory/"$filename")
            echo "Record count is "$recordCount""
            trailerValue=$(printf "%010d\n" "$recordCount")
            echo "Trailer Value is "$trailerValue""
            #Add header
            # sed -i '1s/^/<added text>\n /' x
            fileHeaders=$(echo $fileHeaders | sed 's/^/"/g' | sed 's/$/"/g' | sed 's/,/","/g')
            sed -i "1s/^/$fileHeaders\n/" "$outputDirectory"/"$filename"
            err=$?
            if [ ! $err -eq 0 ]; then
                echo "Failed to add header columns to the output file"
                exit $err
            fi

            if [ "$addDateHeader" == 'Y' ]; then
                sed -i "1s/^/HDR$dateTime\n/" "$outputDirectory"/"$filename"
            fi
            err=$?
            if [ ! $err -eq 0 ]; then
                echo "Failed to add header date to the output file"
                exit $err
            fi
            #Append trailer to the output file
            echo "TR$trailerValue" >>"$outputDirectory"/"$filename"
            err=$?
            if [ ! $err -eq 0 ]; then
                echo "Failed to add trailer to the output file"
                exit $err
            fi
            #set generate header and trailer to null
            generateHeader=''
            generateTrailer=''
        fi

        #check if header is enabled
        if [ "$generateHeader" == "Y" ]; then
            echo "Header is enabled"
            sed -i "1s/^/$fileHeaders\n/" "$outputDirectory"/"$filename"
            err=$?
            if [ ! $err -eq 0 ]; then
                echo "Failed to add header columns to the output file"
                exit $err
            fi
            if [ "$addDateHeader" == 'Y' ]; then
                sed -i "1s/^/HDR$dateTime\n/" "$outputDirectory"/"$filename"
            fi
            err=$?
            if [ ! $err -eq 0 ]; then
                echo "Failed to add header date to the output file"
                exit $err
            fi
        fi

        #check if trailer is enabled
        if [ "$generateTrailer" == "Y" ]; then
            echo "Trailer is enabled"
            recordCount=$(wc -l <$outputDirectory/"$filename")
            echo "Record count is "$recordCount""
            trailerValue=$(printf "%010d\n" "$recordCount")
            echo "Trailer Value is "$trailerValue""
            #Append trailer to the output file
            echo "TR$trailerValue" >>"$outputDirectory"/"$filename"
            err=$?
            if [ ! $err -eq 0 ]; then
                echo "Failed to add trailer to the output file"
                exit $err
            fi
        fi
    else
        echo "File Merge: Not Enabled"
    fi
    echo "Data Generation is Complete. Data is located at "$outputDirectory"" | mail -s "Data Generation Completed" "$email"
    exit
fi

##**************************************************************************
##Generate Final Insert Overwrite Query
##**************************************************************************
finalInsertQuery="$templateInsertQuery $columnList from $schemaName.$tableName $conditionStatement"
echo "Final Query is: "$finalInsertQuery""

##**************************************************************************
## Generate Query File from the final query
##**************************************************************************
queryFile="$tempFilesDir/dataGeneratorQueries/$(date +%s)_$$.hql"
echo "Generating Final Query File in: $queryFile"
echo "$finalInsertQuery" >"$queryFile"


##**************************************************************************
## Execute the query to generate data, Call hive executor to perform the step
##**************************************************************************
echo "Calling hiveExecutor to run the query"
hive --hiveconf $headerCommand --hiveconf mapred.job.queue.name=$QUEUE_NAME -f $queryFile
err=$?
if [ ! $err -eq 0 ]; then
    echo "An Error has occurred. Exiting now"
    exit $err
fi


##**************************************************************************
## Delete CRC files from the output directory
##**************************************************************************
rm --verbose -f "$outputDirectory"/.0*.crc
echo "CRC files have been removed"

echo "Checking if file merge is selected"
if [ "$mergeFile" == "Y" ]; then
    echo "File Merge: Enabled"
    cat "$outputDirectory"/* >>"$outputDirectory"/"$filename"
    rm --verbose -f "$outputDirectory"/0*

    #check if header & trailer are enabled
    if [ "$generateHeader" == "Y" ] && [ "$generateTrailer" == "Y" ]; then
        echo "Header and Trailers are enable"
        recordCount=$(wc -l <$outputDirectory/"$filename")
        echo "Record count is "$recordCount""
        trailerValue=$(printf "%010d\n" "$recordCount")
        echo "Trailer Value is "$trailerValue""
        #Add header
        sed -i "1s/^/$fileHeaders\n/" "$outputDirectory"/"$filename"
        err=$?
        if [ ! $err -eq 0 ]; then
            echo "Failed to add header columns to the output file"
            exit $err
        fi
        if [ "$addDateHeader" == 'Y' ]; then
            sed -i "1s/^/HDR$dateTime\n/" "$outputDirectory"/"$filename"
        fi
        err=$?
        if [ ! $err -eq 0 ]; then
            echo "Failed to add header date to the output file"
            exit $err
        fi
        #Append trailer to the output file
        echo "TR$trailerValue" >>"$outputDirectory"/"$filename"
        err=$?
        if [ ! $err -eq 0 ]; then
            echo "Failed to add trailer to the output file"
            exit $err
        fi
        #set generate header and trailer to null
        generateHeader=''
        generateTrailer=''
    fi

    #check if header is enabled
    if [ "$generateHeader" == "Y" ]; then
        echo "Header is enabled"
        sed -i "1s/^/$fileHeaders\n/" "$outputDirectory"/"$filename"
        err=$?
        if [ ! $err -eq 0 ]; then
            echo "Failed to add header columns to the output file"
            exit $err
        fi
        if [ "$addDateHeader" == 'Y' ]; then
            sed -i "1s/^/HDR$dateTime\n/" "$outputDirectory"/"$filename"
        fi
        err=$?
        if [ ! $err -eq 0 ]; then
            echo "Failed to add header date to the output file"
            exit $err
        fi
    fi

    #check if trailer is enabled
    if [ "$generateTrailer" == "Y" ]; then
        echo "Trailer is enabled"
        recordCount=$(wc -l <$outputDirectory/"$filename")
        echo "Record count is "$recordCount""
        trailerValue=$(printf "%010d\n" "$recordCount")
        echo "Trailer Value is "$trailerValue""
        #Append trailer to the output file
        echo "TR$trailerValue" >>"$outputDirectory"/"$filename"
        err=$?
        if [ ! $err -eq 0 ]; then
            echo "Failed to add trailer to the output file"
            exit $err
        fi
    fi
else
    echo "File Merge: Not Enabled"
fi

##**************************************************************************
## Send Notification
##**************************************************************************
echo "Data Generation is Complete. Data is located at "$outputDirectory"" | mail -s "Data Generation Completed" "$email"
echo "Exiting from function"
exit


