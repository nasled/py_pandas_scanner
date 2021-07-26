
## Transactions scanner

The service was built using python3, pipenv, sqlite, sqlachemy, pandas.

Results could be found in the /data folder.

* Tested Without Normalization/ErrorLogging

```
Total time: 62.44595670700073
``` 

* Tested With Normalization Without ErrorLogging
```
Total time  71.52450966835022
```

* Tested With Normalization/ErrorLogging
```
Total time  604.1574554443359
```

### Installation

* Copy the project to a directory
```
git clone https://github.com/nasled/transactions_scanner.git
```
* Place the CSV data-file to the next folder
```
./data/transactions.csv
```
* Activate virtualenv (install pipenv if necessary)
```
pipenv shell
```
* Install the required dependencies
```
pipenv install 
```
* Update the required dependencies
```
pipenv update
```
* Set permissions
```
chmod 0777 scanner.py
```

### Usage
* Display help
```
./scanner.py help
```
* Remove database files
```
./scanner.py clean
```
* Run import step
```
./scanner.py import
```
* Run export step
```
./scanner.py export
```

