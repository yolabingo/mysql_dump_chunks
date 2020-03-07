# MySQL DumpChunks
Creates a mysqldump shell script that breaks up a large database table into smaller chunks


### Usage
```
usage: dump_chunks.py [-h] [-c DUMPFILE_COUNT] [-i DB_PRIMARY_KEY]
                      [-o OUTPUT_DIR] [-v] [-m DB_MAX_ID]
                      server database table user password

create mysqldump commands to dump a large database table in chunks

positional arguments:
  server                mysql server hostname or ip address
  database              mysql database name
  table                 mysql table name to be dumped
  user                  mysql user
  password              mysql password

optional arguments:
  -h, --help            show this help message and exit
  -c DUMPFILE_COUNT, --dumpfile-count DUMPFILE_COUNT
                        number of .sql dump files to create, default=10
  -i DB_PRIMARY_KEY, --db-primary-key DB_PRIMARY_KEY
                        table primary key column - can be introspected
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        output dir for .sql files, default='./'
  -v, --verbose         verbose output
  -m DB_MAX_ID, --db-max-id DB_MAX_ID
                        approx value of the max primary key, 0==introspect
```
#### Examples
Save wp_options table

``` mysql-dump-chunks db1.example.com wp_db wp_options db_user sw0rdfish ```

Save wp_options table as 5 separate dump files

``` mysql-dump-chunks db1.example.com wp_db wp_options db_user sw0rdfish --dumpfile-count 5 ```

Save wp_options table, use /tmp/dump/ as dumpfile destination directory

``` mysql-dump-chunks db1.example.com wp_db wp_options db_user sw0rdfish --output-dir /tmp/dump ```

Running without Python Mysql support? 

Just specify both `db-primaryokey` and `db-max-id`

It won't be able to divide up the dumpfiles into equal number of rows though.

Save to a file

``` mysql-dump-chunks db1.example.com wp_db wp_options db_user sw0rdfish > dump.sh ```

Or pipe to shell to run immediately

``` mysql-dump-chunks db1.example.com wp_db wp_options db_user sw0rdfish | sh ```

### Prerequisites

python3.6 or greater


### Installing

Maybe it just works? 

```python3 mysql-dump-chunks```
or
```./mysql-dump-chunks```

If you need to install Python MySQL driver, you can do so system-wide or in virtual environment

#### Installing Python Mysql system-wide
Debian/Ubuntu 

``` apt-get install python3-mysqldb```


#### Installing Python Mysql in a virtual environment
Install header files so python can build its Mysql client

Debian/Ubuntu

``` apt-get install python3-dev default-libmysqlclient-dev ```

macOS Homebrew

``` brew install mysql-client ```

Create a new Python3 virtualenv

```bash
python3 -m venv /var/tmp/mysqlpy
```

Then, either activate the virtualenv, which prepends /var/tmp/mysqlpy/bin to your path:

```bash
source /var/tmp/mysqlpy/bin/activate
# install mysqlclient in virtualenv with python package manager
pip install mysqlclient
python3 mysql-dump-chunks
deactivate  # exists the virtualenv
```
Or invoke those binaries directly

```bash
# install mysqlclient in the virtualenv
/var/tmp/mysqlpy/bin/pip install mysqlclient
# run mysql-dump-chunks via the python interprete in the virtualenv
/var/tmp/mysqlpy/bin/python3 mysql-dump-chunks
```

## Authors

* **toddj@swcp.com** - (https://gitlab.swcp.com/toddj/mysql-dump-chunks)
