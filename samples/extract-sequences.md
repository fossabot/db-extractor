# Extract Sequence file and related ones

## Why JSON format?
As JSON allows an hierarchical representation of any kind of data but still keep a structured level that's easy to digest both programmatically and humans this standard has been chosen.

## Why separate Source Systems file?
* Main reason for choosing a separate JSON file for Source Systems configurations is to have a centralized, therefore easier to control (create and adjust whenever architecture changes dictates so), structure that can be shared between team members;
* Another good reason is the ability to use various Extract Sequence in conjunction with same Source System file (many o one relationship).

## Why separate User Secrets file?
Having a separate JSON file for USer Secrets configurations allows any team member / user to be in fully control, respecting privacy, of included highly sensitive username and password to be used for various connections.

## Structure of the Extract Sequence file explained
### Main block
* To be able to support multiple sequences for extraction, a list type is to be used to sustains main block, and that is represented by a squared parenthesis block;
* each extract sequence is to be enclosed into curly brackets which has following keys:

| Attribute Name  | Attribute Type | Values Type | Accepted values | Description |
|:----------------|:---------------|:------------|:----------------|:------------|
| server-vendor | Mandatory | String | Oracle, MariaDB Foundation, SAP | Vendor as the official company currently holding the developing decision power |
| server-type | Mandatory | String | MySQL, MariaDB, HANA | Type as product generic name |
| server-group | Mandatory | String | any | Groups as a category of servers, usually given by geographical location |
| server-layer | Mandatory | String | any | Layer as a category of servers, usually to differentiate between Development, Quality and Production |
> above 4 mentioned attributes within extracting sequence are expected to be hierarchical attributes within Source Systems file, as in below example:
>> Extract Sequence 1st example:
```
[
    {
        "server-vendor": "Oracle",
        "server-type":   "MySQL",
        "server-group":  "Your Company Server Group Name",
        "server-layer":  "Local",
        ...
    }
]
```
>> Source Systems 1st example (correlated with Extract Sequence 1st example)
```
{
    "Systems": {
        "Oracle": {
            "MySQL": {
                "Server": {
                    "Your Company Server Group Name": {
                        "Local": {
                            "FriendlyName": "Local Oracle MySQL",
                            "ServerName":   "localost",
                            "ServerPort":   3306
                        },
                        "Production": {
                            "FriendlyName": "Production Oracle MySQL",
                            "ServerName":   "prod",
                            "ServerPort":   3306
                        }
                    }
                }
            }
        }
    }
}
```
| Attribute Name  | Attribute Type | Values Type | Accepted values | Description |
|:----------------|:---------------|:------------|:----------------|:------------|
| account-label | Mandatory | String | any | just a convention to differentiate potential multiple accounts available |
>> Extract Sequence 2nd example:
```
[
    {
        ...
        "account-label":  "Default",
        ...
    }
]
```
>> User Settings 1st example (correlated with Extract Sequence 2nd example)
```
{
    "Credentials": {
        "Oracle": {
            "MySQL": {
                "Your Company Server Group Name": {
                    "Local": {
                        "Admin": {
                            "Name": "Administrator Full Name Here",
                            "Password": "your_admin_password_goes_here",
                            "Username": "your_admin_username_goes_here"
                        },
                        "Default": {
                            "Name": "Your Full Name Here",
                            "Password": "your_password_goes_here",
                            "Username": "your_username_goes_here"
                        }
                    }
                }
            }
        }
    }
}
```
