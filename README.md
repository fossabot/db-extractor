# db-extractor


## What is this repository for?

Extract information from databases (MySQL, SAP HANA to start with, other will be implemented later)

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
    $ git clone https://github.com/danielgp/tableau-hyper-management <local_folder_on_your_computer>
```
> conventions used:
>> <content_within_html_tags> = variables to be replaced with user values relevant strings
* Create a Python Virtual Environment using following command executed from project root folder:
```
    $ python -m venv virtual_environment/
```
* Upgrade pip (PIP is a package manager for Python packages) and SetupTools using following command executed from newly created virtual environment and Scripts sub-folder:
```
    $ python -m pip install --upgrade pip
    $ pip install --upgrade setuptools
```
* Install project prerequisites using following command executed from project root folder:
```
    $ python setup.py install
```


## Maintaining local package up-to-date

Once the package is installed is quite important to keep up with latest releases as such are addressing important code improvements and potential security issues, and this can be achieved by following command:
```
    $ git --work-tree=<local_folder_on_your_computer> --git-dir=<local_folder_on_your_computer>/.git/ --no-pager pull origin master
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

## Features to request template

Use [feature_request.md](.github/ISSUE_TEMPLATE/feature_request.md)

## Bug report template

Use [bug_report.md](.github/ISSUE_TEMPLATE/bug_report.md)

## Code quality analysis
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/danielgp/db-extractor/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/danielgp/db-extractor/?branch=master)

## Build Status
[![Build Status](https://scrutinizer-ci.com/g/danielgp/db-extractor/badges/build.png?b=master)](https://scrutinizer-ci.com/g/danielgp/db-extractor/build-status/master)


