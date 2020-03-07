#!/usr/bin/env python3

# create mysqldump commands to dump multiple sql files for a single (large) table
# specify dump_file_count for desired number of files

# requires Python 3.6 or greater


import argparse
import logging
import os
import sys

try:
    import MySQLdb
except ModuleNotFoundError:
    print("Unable to load python MySQLdb module\n")
    print("It is not required, but it allows database introspection for convenience.")
    print("More importantly, it creates equal-sized dump files\n")
    print("It may be installed (depending on the environment) with")  
    print("  'pip install mysqlclient' or ")
    print("  'pip install --user mysqlclient' or \n")


class MysqlChunkException(Exception):
    pass


class MysqlChunks():
    """create mysqldump commands to fetch a large database table in smaller chunks"""

    def __init__(self,
             host,
             user,
             db,
             password,
             table,
             pk="",
             chunk_count=10,
             output_dir="",
             mysqldump_script_file="",
             verbose=False,
             db_max_id=0,
         ):
        """
        host: mysql server hostname
        user: mysql user
        db:   mysql db name
        password: mysql password
        table: mysql table to dump
        pk: primary key column name for table
        chunk_count: number of sql dump files to create
        output_dir: directory to save *.sql dump files
        verbose:  verbose output
        db_max_id: approx max id of pk, only needed if no mysql support
        """
        assert(host)
        self.host = host
        self.user = user
        self.db = db
        self.password = password
        self.table = table
        self.chunk_count = chunk_count
        self.db_max_id = db_max_id

        if not output_dir: 
            output_dir = "./"
        self.output_dir = os.path.abspath(output_dir)

        if not mysqldump_script_file:
            mysqldump_script_file = f"mysqldump-{host}-{db}-{table}"
        self.mysqldump_script_file = os.path.abspath(mysqldump_script_file)

        self._log(verbose)
        self._validate_params()
        self._get_pk(pk)
        chunks = self._get_chunks() 
        self._mysqldump_template(chunks)


    def _mysqldump_template(self, chunks):
        """writes mysqldump commands to file""" 
        output = "#!/bin/bash\n"
        mysqldump = f"mysqldump --opt --order-by-primary --compress -h {self.host} -u {self.user} -p'{self.password}'"
        no_create = ""
        file_count = 1
        start = 1
        end = None
        for pk in chunks[1:]:
            end = pk
            sql_file = os.path.join(self.output_dir, f"{self.table}.{str(file_count).zfill(3)}.sql")
            id_range = f'-w"{self.table}.{self.pk}>={start} AND {self.table}.{self.pk}<{end}"'
            output += f"{mysqldump} {no_create} {id_range} -r {sql_file} {self.db} {self.table} \n"
            output += f"echo 'dumped {sql_file}' \n"
            output += "sleep 1 \n"
            file_count += 1
            no_create = "--skip-add-drop-table --no-create-info"
            start = end
        sql_file = os.path.join(self.output_dir, f"{self.table}.{str(file_count).zfill(3)}.sql")
        id_range = f'-w"{self.table}.{self.pk}>={end}"'
        output += f"{mysqldump} {no_create} {id_range} -r {sql_file} {self.db} {self.table} \n"
        output += f"echo 'dumped {sql_file}' \n"
        with open(self.mysqldump_script_file, "w") as f:
            f.write(output)
        os.chmod(self.mysqldump_script_file, 0o755)
        print("mysqldump file ready:")
        print(f"  {self.mysqldump_script_file}")


    def _get_chunks(self):
        """ find primary keys for chunk boundries

        for example [1, 1900, 4532, 7222]
        """
        try:
            try:
                cursor = self._db_connect().cursor()
                cursor.execute(f"SELECT {self.pk} FROM {self.table} ORDER BY {self.pk}")
                keys = [i[0] for i in cursor.fetchall()]
                logging.info(f"{len(keys)} rows in table '{self.table}'")
            except (MySQLdb._exceptions.OperationalError,
                    MySQLdb._exceptions.ProgrammingError,) as e: 
                logging.info(f"unable to query database: {e}")
                logging.info(f"so using db_max_id provided")
        except NameError:
            # mysqlclient is not installed
            keys = [i for i in range(self.db_max_id)]

        if len(keys) < 10000:
            logging.warning(f"db table only has {len(keys)} rows - perhaps this won't be useful")
        if len(keys) <= (self.chunk_count):
            raise MysqlChunkException("chunk_count must be greater than total table rows so bailing...")
        # divide the list of keys into equal-sized chunks
        chunk_size = int(len(keys) / self.chunk_count)
        chunks = keys[::chunk_size]
        chunks[0] = 1
        logging.info(f"total rows: {len(keys)}")
        logging.info(f"chunk_size: {chunk_size}")
        logging.info(f"chunks: {chunks}")
        return chunks
            

    def _get_pk(self, pk):
        """query db to find primary key column if it wasn't provided as a parameter"""
        if pk:
            self.pk = pk
        else:
            logging.info("primary key not specified so querying the db for it")
            try:
                db = self._db_connect()
                db.query(f"SHOW KEYS FROM {self.table} WHERE Key_name = 'PRIMARY'")
                result = db.store_result().fetch_row(how=1)
                self.pk = result[0]["Column_name"]
                logging.info(f"found table primary key '{self.pk}' for '{self.table}'")
            except (MySQLdb._exceptions.OperationalError,
                    MySQLdb._exceptions.ProgrammingError,) as e: 
                raise MysqlChunkException(f"mysql error {e}")
        if not self.pk:
            raise MysqlChunkException("db table primary key required, bailing...")


    def _db_connect(self):
        return MySQLdb.connect(host=self.host, db=self.db, user=self.user, passwd=self.password)


    def _validate_params(self):
        """required paramaters"""
        for i in [self.host, self.user, self.db, self.password, self.table]:
            if not i:
                msg = "These paramaters are required, one was missing\n"
                msg += f"self.host: {self.host}\n"
                msg += f"self.user: {self.user}\n"
                msg += f"self.db: {self.db}\n"
                msg += f"self.password: {self.password}\n"
                msg += f"self.table: {self.table}\n"
                raise MysqlChunkException(msg)


    def _log(self, verbose, debug=False):
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        elif verbose:
            logging.basicConfig(level=logging.INFO)
        logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create mysqldump commands to dump a large database table in chunks")
    parser.add_argument("server", help="mysql server hostname or ip address")
    parser.add_argument("database", help="mysql database name")
    parser.add_argument("table", help="mysql table name to be dumped")
    parser.add_argument("user", help="mysql user")
    parser.add_argument("password", help="mysql password")
    parser.add_argument("-c", "--dumpfile-count", default=10, type=int, help="number of .sql dump files to create, default=10")
    parser.add_argument("-i", "--db-primary-key", default=None, help="table primary key column - can be introspected")
    parser.add_argument("-o", "--output-dir", default="./", help="output dir for .sql files, default='./'")
    parser.add_argument("-f", "--mysqldump-script-file", default="", help="file to save mysqldump commands")
    parser.add_argument("-v", "--verbose", default=False, action="store_true", help="verbose output")
    # max_id = 0 means we query the DB and generate equal-size sql files
    # specify max ID to avoid MySQL query, but sql files may vary in size 
    # it should be reasonably close to max PRIMARY KEY value but need not be exact 
    parser.add_argument("-m", "--db-max-id", default=0, type=int, help="approx value of the max primary key, 0==introspect ")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)
    logging.getLogger(__name__)

    try:
        chunk = MysqlChunks(
                     host=args.server,
                     user=args.user,
                     db=args.database,
                     password=args.password,
                     table=args.table,
                     pk=args.db_primary_key,
                     chunk_count=args.dumpfile_count,
                     output_dir=args.output_dir,
                     verbose=args.verbose,
                     mysqldump_script_file=args.mysqldump_script_file,
                     db_max_id=args.db_max_id,
                 )
     
    except MysqlChunkException as e:
        logging.error(e)
        sys.exit(1)
