# HiDE - Hive Data Extractor

![HiDE](images/HiDE.png)

## Description

A Reusable Big Data Platform component which can generate data extracts in various formats from any Bigdata Hive Tables. Supports custom data delimiters, ability to provide extraction conditions and attribute selection for creating the extract.

## Features

- No code data extraction from Hive
- Completely config driven
- Generate data from any table with option to exclude specific columns and required filter conditions.
- Provides the flexibility of defining your own paths where you want the data to be loaded for your application.
- Works on very high-volume data (Multi Millions of rows)
- Email Notification about status of your request which includes path for the data requested.

## Prerequisite

- Big-Data Cluster
- Hive
- Bash

## Getting Started

- Fork the Repo
- Clone it into you local / server where you want to run this Bigdata Data extraction
- Run the script dataExtractor.sh

## Configuration

- Modify the configuration file - config.cfg
- Update the value of `basePath` variable to the path where the code will be stored
- Update `QUEUE_NAME` to your use-case's queue name, ex: `export QUEUE_NAME=root.testQueue`
- Update the `email` address. Notifications will be sent to the ids mentioned in this variable
- Update the `extractionDB` for ',' delimiter the data is loaded to a table mentioned in extractionDB. 
- Update the `extractionLoaction` for ',' delimiter path of the db where the data needs to be stored

## Command line arguments

- The script requires atleast 4 inputs that needs to be passed as below:
  - `-s | --schema` : `Mandatory` :  Hive database where your table is located  - String  DB Name
  - `-t | --tablename` : `Mandatory` : Table name in hive database from which the data will be generated - String Table  Name
  - `-f | --filename` : `Mandatory` :  Output filename containing the data generated. Works with --mergefile=Y  - String file Name
  - `-md | --moveoutputdirectory` : `Mandatory` : Y/N - Take backup of existing extract directory - String File Path
  - `-c | --filtercondition` : `Optional` : Filter/Where condition. Must be properly quoted - String Filter
  - `-ex | --excludecolumn` : `Optional` : Columns that needs to be excluded during the process - String pipe separated column names
  - `-m | --mergefile` : `Mandatory` : Y/N - Indicates if part files need to be merged. --filename works when then is set to Y - String mergefile
  - `-hr | --generateheader` : `Optional` : Y/N - Generate header for the extract - String generateheader
  - `-tr | --generatetrailer` : `Optional` : Y/N - Generates trailer in the form TXXXXXXXXXX is the record count - String generatetrailer
  - `-dl | --delimiter` : `Optional`: Default ^A - Specific data delimiter request is passed to this variable - Delimiter in quotes ''
  - `-hdl | --headerdelimiter` : `Optional` : Default ^A - Specific header delimiter request is passed to this variable - Delimiter in quotes ''
  - `-hd | --headerdate` : `Optional` : Add date as an additional header record. Format HDR%Y%m%d

## Execution Examples

```bash
sh dataExtractor.sh -s testschema -t testtable -f GenDataFile.dat -m Y -md Y
```

### with delimiter

```bash
sh dataExtractor.sh -s testschema -t testtable -f GenDataFile.dat -m Y -md Y -d ','
```

### with filter condition

```bash
sh dataExtractor.sh -s testschema -t testtable -f GenDataFile.dat -m Y -md Y -c 'WHERE ID=2'
```

### with exclude column

```bash
sh dataExtractor.sh -s testschema -t testtable -f GenDataFile.dat -m Y -md Y -ex 'col1|col2'
```

### with mergefile, generateheader trailer

```bash
sh dataExtractor.sh -s testschema -t testtable -f GenDataFile.dat -m Y -md Y -hr Y - tr Y
```

## Tests

You can run tests using [Bats](https://github.com/sstephenson/bats)

## Contributing

We welcome Your interest in the American Express Open Source Community on Github. Any Contributor to
any Open Source Project managed by the American Express Open Source Community must accept and sign
an Agreement indicating agreement to the terms below. Except for the rights granted in this 
Agreement to American Express and to recipients of software distributed by American Express, You
reserve all right, title, and interest, if any, in and to Your Contributions. Please
[fill out the Agreement](https://cla-assistant.io/americanexpress/hide).

## License

Any contributions made under this project will be governed by the
[Apache License 2.0](./LICENSE.txt).

## Code of Conduct

This project adheres to the [American Express Community Guidelines](./CODE_OF_CONDUCT.md).
By participating, you are expected to honor these guidelines.
