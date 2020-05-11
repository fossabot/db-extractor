# db-extractor

## Code quality analysis and Build Status
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/danielgp/db-extractor/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/danielgp/db-extractor/?branch=master)
[![Build Status](https://scrutinizer-ci.com/g/danielgp/db-extractor/badges/build.png?b=master)](https://scrutinizer-ci.com/g/danielgp/db-extractor/build-status/master)

## What is this repository for?

Extract information from databases (MySQL, MariaDB, SAP HANA to start with, other will be implemented later) using a combination of:
* extraction sequences file (JSON format) that is easy enough to create and maintain but also provide very complex features to be set;
* source system file (JSON format) to keep a central list of servers and/or databases to connect to that can be shared between people;
* user settings file (JSON format) to keep a central list of credentials that is not to be shared with anyone or maybe with a small group of people; 

## Features implemented

* Ability to extract from a single source system or multiple using 1 JSON extraction sequence file;
* Ability to extract a single or multiple query for each source system using same JSON extraction sequence file;
* Ability to extract a single or multiple files using sessions for each query where parameters can be specified (currently on CSV and Excel file format are supported, other will follow);
* Multi-language (English, Italian, Romanian);
* Enhance behaviour choices so that besides existing 'skip-if-output-file-exists' and 'overwrite-if-output-file-exists' to have the option to specify to overwrite but only if the file is older than any choice of a CalculatedDate expression is given, as this is very useful when extracting large amount of data over VPN in small pieces and VPN drops (could mean already extracted pieces would be already skipped as not older than threshold imposed);

## Supported File Types/Formats

* Comma Separated Values (with ability to specify a custom separator of your preference)
* Excel 2013+
* JSON
* Pickle (compression algorithms: bz2, gzip, xz, zip to choose from)

## Who do I talk to?

Repository owner is: [Daniel Popiniuc](mailto:danielpopiniuc@gmail.com)


## Installation

Installation can be completed in few steps as follows:
* Ensure you have git available to your system:
```
    $ git --version
```
> If you get an error depending on your system you need to install it.
>> For Windows you can do so from [Git for Windows](https://github.com/git-for-windows/git/releases/);
* Download this project from Github:
```
    $ git clone https://github.com/danielgp/db-extractor <local_path_of_this_package>
```
> conventions used:
>> <content_within_html_tags> = variables to be replaced with user values relevant strings
* Create a Python Virtual Environment using following command executed from project root folder:
```
    $ python(.exe) -m venv <local_folder_on_your_computer_for_this_package>/virtual_environment/
```
* Upgrade pip (PIP is a package manager for Python packages) and SetupTools using following command executed from newly created virtual environment and Scripts sub-folder:
```
    $ <local_path_of_this_package>/virtual_environment/Scripts/python(.exe) -m pip install --upgrade pip
    $ <local_path_of_this_package>/virtual_environment/Scripts/pip(.exe) install --upgrade setuptools
```
* Install project prerequisites using following command executed from project root folder:
```
    $ <local_path_of_this_package>/virtual_environment/Scripts/python(.exe) <local_path_of_this_package>/setup.py install
```
* Ensure all localization source files are compile properly in order for the package to work properly
```
    $ <local_path_of_this_package>/virtual_environment/Scripts/python(.exe) <local_path_of_this_package>/sources/localizations_compile.py
```

## Maintaining local package up-to-date

Once the package is installed is quite important to keep up with latest releases as such are addressing important code improvements and potential security issues, and this can be achieved by following command:
```
    $ git --work-tree=<local_path_of_this_package> --git-dir=<local_path_of_this_package>/.git/ --no-pager pull origin master
```
- conventions used:
    - <content_within_html_tags> = variables to be replaced with user values relevant strings


## Usage

```
    $ python <local_path_of_this_package>/sources/extractor.py --input-source-system-file <input_source_system_file_name> --input-credentials-file <input_credentials_file_name> --input-extracting-sequence-file <input_extracting_sequence_file_name> (--output-log-file <full_path_and_file_name_to_log_running_details>)
```
> conventions used:
>> (content_within_round_parenthesis) = optional
>> <content_within_html_tags> = variables to be replaced with user values relevant strings
>> single vertical pipeline = separator for alternative options

### Example of usage
```
    $ python sources/extractor.py --input-source-system-file samples/sample---server-config.json --input-credentials-file samples/sample---user-settings.json --input-extracting-sequence-file samples/sample---list-of-fields.json --output-log-file samples/sample---list-of-fields.log
```

## Code of conduct

Use [CODE_OF_CONDUCT.md](.github/CODE_OF_CONDUCT.md)

## Features already raised

* Implement ability to store extracted result-set into HTML format file;

## Features to request template

Use [feature_request.md](.github/ISSUE_TEMPLATE/feature_request.md)

## Bug report template

Use [bug_report.md](.github/ISSUE_TEMPLATE/bug_report.md)

## Required software/drivers/configurations

see [readme_software.md](readme_software.md)

## Used references

see [readme_reference.md](readme_reference.md)
