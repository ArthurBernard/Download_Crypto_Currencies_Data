#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-07-26 11:54:55
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-14 18:57:21

""" Tools and object to load, append and save differnet kind of database.

"""

# Built-in packages
from os import makedirs
import os.path
import time
import sqlite3
from pickle import Pickler, Unpickler

# Third-party packages
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

# Local packages


__all__ = ['IODataBase', 'get_df', 'save_df']


class IODataBase:
    """ Object to save a pd.DataFrame into different kind/format of database.

    Parameters
    ----------
    path : str, optional
        Path of the database, default is './' (current directory).
    method : {'DataFrame', 'SQLite', 'CSV', 'Excel', 'PostgreSQL', 'Oracle',\
              'MSSQL', 'MySQL'}
        Format of database, default is CSV.

    Attributes
    ----------
    path : str
        Path of the database.
    method : {'DataFrame', 'SQLite', 'CSV', 'Excel', 'PostgreSQL', 'Oracle',\
              'MSSQL', 'MySQL'}
        Kind/format of the database.
    parser : dict
        Values are function to corresponding to `method`.

    Methods
    -------
    save_as_dataframe
    save_as_sql
    save_as_sqlite
    save_as_csv
    save_as_excel
    __call__

    """
    # TODO:
    # - Add InfluxDB method
    # - Add output methods
    # - Add unitest/doctest

    def __init__(self, path='./', method='csv'):
        # Verify path exist
        makedirs(path, exist_ok=True)

        # Set init attribute
        self.path = path
        self.method = method.lower()
        self.parser = {
            'dataframe': self.save_as_dataframe,
            'sqlite': self.save_as_sqlite,
            'csv': self.save_as_csv,
            'excel': self.save_as_excel,
            'postgresql': self.save_as_sql,
            'mysql': self.save_as_sql,
            'oracle': self.save_as_sql,
            'mssql': self.save_as_sql,
        }

        # Verify method
        if self.method not in self.parser.keys():

            raise NotImplementedError(
                "`method` should be DataFrame, SQLite, CSV or Excel"
            )

    def __call__(self, new_data, **kwargs):
        """ Append and save `new_data` in database as `method` format.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        kwargs : dict, optional
            Cf parameters of corresponding `method`.

        """
        return self.parser[self.method](new_data, **kwargs)

    def save_as_dataframe(self, new_data, name=None, ext='.dat'):
        """ Append and save `new_data` as pd.DataFrame binary object.

        With pickle save as binary pd.DataFrame object, if `name` database
        already exists, load it, append `new_data` and save it, else create a
        new database.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        name : str, optional
            Name of the database, default is the current date.
        ext : str, optional
            Extension of the database, default is '.dat'.

        """
        if name is None:
            name = time.strftime('%y-%m-%d', time.gmtime(time.time()))

        # Load data
        database = get_df(self.path, name, ext=ext)
        # Append new data
        database = database.append(new_data, sort=False)
        # Save new data
        save_df(database, self.path, name, ext=ext)

    def get_from_dataframe(self, name, ext='.dat'):
        """ Get data from pd.DataFrame binary object.

        With pickle get as binary pd.DataFrame object.

        Parameters
        ----------
        name : str
            Name of the database.
        ext : str, optional
            Extension of the database, default is '.dat'.

        """
        return get_df(self.path, name, ext=ext)

    def save_as_sqlite(self, new_data, table='main_table', name=None,
                       ext='.db', index=True, index_label=None):
        """ Append and save `new_data` in SQLite database.

        With sqlite, if `name` database already exists append `new_data`, else
        create a new data base.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        table : str, optional
            Name of the table, default is 'main_table'.
        name : str, optional
            Name of the database, default is the current year.
        ext : str, optional
            Extension of the database, default is '.db'.
        index : bool, optional
            Write pd.DataFrame index as a column. Uses index_label as the
            column name in the table. Default is True.
        index_label : string or sequence, optional
            Column label for index column(s). If None is given (default) and
            index is True, then the index names are used. A sequence should
            be given if the pd.DataFrame uses pd.MultiIndex.

        """
        if name is None:
            name = time.strftime('%y', time.gmtime(time.time()))

        # Open connection with database
        conn = sqlite3.connect(self.path + name + ext)
        # Append data
        new_data.to_sql(table, con=conn, if_exists='append', index=index,
                        index_label=index_label)
        # Close connection
        conn.close()

    def get_from_sqlite(self, name, table='main_table', ext='.db'):
        """ Get data from SQLite database.

        Parameters
        ----------
        name : str, optional
            Name of the database, default is the current year.
        table : str, optional
            Name of the table, default is 'main_table'.
        ext : str, optional
            Extension of the database, default is '.db'.
        index : bool, optional
            Write pd.DataFrame index as a column. Uses index_label as the
            column name in the table. Default is True.
        index_label : string or sequence, optional
            Column label for index column(s). If None is given (default) and
            index is True, then the index names are used. A sequence should be
            given if the pd.DataFrame uses pd.MultiIndex.

        """
        # TODO : to finish !

        # Open connection with database
        conn = sqlite3.connect(self.path + name + ext)
        # Append data
        df = pd.read_sql(table, con=conn)
        # Close connection
        conn.close()

        return df

    def save_as_sql(self, new_data, table='main_table', name=None,
                    ext='', index=True, index_label=None, driver=None,
                    username=None, password=None, host=None, port=None,
                    **kwargs):
        """ Append and save `new_data` in SQL database.

        SQL database as `method={'PostgreSQL', 'Oracle', 'MSSQL', 'MySQL'}`.
        If `name` already exists append `new_data`, else create a new
        database. See SQLAlchemy documentation for more details [1]_.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        table : str, optional
            Name of the table, default is 'main_table'.
        name : str, optional
            Name of the database, default is the current year.
        ext : str, optional
            Extension of the database, default is '.db'.
        index : bool, optional
            Write pd.DataFrame index as a column. Uses index_label as the
            column name in the table. Default is True.
        index_label : string or sequence, optional
            Column label for index column(s). If None is given (default) and
            index is True, then the index names are used. A sequence should be
            given if the pd.DataFrame uses pd.MultiIndex.
        driver : {'psycopg2', 'pg8000', 'mysqlclient', pymysql', 'cx_oracle',\
                  'pyodbc', 'pymssql'}
            The name of the DBAPI to be used to connect to the database using
            all lowercase letters. If not specified, a default DBAPI will be
            imported if available - this default is typically the most widely
            known driver available for that backend.
        username, password : str
            Username and password to connect to the SQL database.
        host : str, optional
            Host to connect, default is 'localhost'.
        port : str, optional
            The port number, default is None.
        kwargs : dict, optional
            A dictionary of options to be passed to the dialect and/or the
            DBAPI upon connect.

        References
        ----------
        .. [1] https://docs.sqlalchemy.org/en/13/core/engines.html

        """
        if name is None:
            name = time.strftime('%y', time.gmtime(time.time()))

        if driver is None:
            driver = self.method
        else:
            driver = self.method + '+' + driver

        # Open connection with database
        url = URL(
            driver, username=username, password=password, host=host,
            port=port, database=self.path + name + ext, query=kwargs,
        )
        conn = create_engine(url)
        # Append data
        new_data.to_sql(table, con=conn, if_exists='append', index=index,
                        index_label=index_label)
        # Close connection
        # conn.close()

    def save_as_csv(self, new_data, name=None, ext='.csv', index=True,
                    index_label=None):
        """ Append and save `new_data` in database as CSV format.

        With pickle save as binary pd.DataFrame object, if `name` database
        already exists append `new_data`, otherwise create a new file.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        name : str, optional
            Name of the database, default is the current year.
        ext : str, optional
            Extension of the database, default is '.csv'.
        index : bool, optional
            Write row names (index), default is True.
        index_label : str or sequence, optional
            Column label for index column(s) if desired. If not specified
            (default is None), and index are True, then the index names are
            used. A sequence should be given if the DataFrame uses MultiIndex.
            If False do not print fields for index names. Use
            `index_label=False` for easier importing in R.

        """
        if name is None:
            name = time.strftime('%y', time.gmtime(time.time()))

        # Create database and write the header
        if os.path.exists(self.path + name + ext):
            new_data.to_csv(self.path + name + ext, mode='w', header=True,
                            index=index, index_label=index_label)

        # Append data to database without header
        else:
            new_data.to_csv(self.path + name + ext, mode='a', header=False,
                            index=index, index_label=index_label)

    def save_as_excel(self, new_data, name=None, sheet_name='Sheet1',
                      ext='.xlsx', index=True, index_label=None):
        """ Append and save `new_data` in database as Excel format.

        With pickle save as binary pd.DataFrame object, if `name` database
        already exists append `new_data`, else create a new file.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        name : str, optional
            Name of the database, default is the current date.
        sheet_name : str, optional
            Name of sheet which will contain `new_data`, default is 'Sheet1'.
        ext : str, optional
            Extension of the database, default is '.xlsx'.
        index : bool, optional
            Write row names (index), default is True.
        index_label : str or sequence, optional
            Column label for index column(s) if desired. If not specified
            (default is None), and index are True, then the index names are
            used. A sequence should be given if the DataFrame uses MultiIndex.

        Warnings
        --------
        Slow method, not recommanded for large database.

        """
        if name is None:
            name = time.strftime('%y-%m-%d', time.gmtime(time.time()))

        path = self.path + name + ext

        # Append data to database if exist
        try:
            with pd.ExcelWriter(path, engine='openpyxl', mode='a') as w:
                new_data.to_excel(w, sheet_name=sheet_name, merge_cells=False,
                                  index=index, index_label=index_label)
        # Create a new database
        except FileNotFoundError:
            new_data.to_excel(path, sheet_name=sheet_name, merge_cells=False,
                              index=index, index_label=index_label)


def get_df(path, name, ext=''):
    """ Load a dataframe as binnary file.

    Parameters
    ----------
    path, name, ext : str
        Path to the file, name of the file and the extension of the file.

    Returns
    -------
    pandas.DataFrame
        A dataframe, if file not find return an empty dataframe.

    """
    if path[-1] != '/' and name[0] != '/':
        path += '/'

    if len(ext) > 0 and ext[0] != '.':
        ext = '.' + ext

    try:
        with open(path + name + ext, 'rb') as f:
            df = Unpickler(f).load()

            return df

    except FileNotFoundError:

        return pd.DataFrame()


def save_df(df, path, name, ext=''):
    """ Save a dataframe as a binnary file.

    Parameters
    ----------
    df : pandas.DataFrame
        A dataframe to save as binnary file.
    path, name, ext : str
        Path to the file, name of the file and the extension of the file.

    """
    if path[-1] != '/' and name[0] != '/':
        path += '/'

    if len(ext) > 0 and ext[0] != '.':
        ext = '.' + ext

    try:
        with open(path + name + ext, 'wb') as f:
            Pickler(f).dump(df)

    except FileNotFoundError:
        makedirs(path, exist_ok=True)

        with open(path + name + ext, 'wb') as f:
            Pickler(f).dump(df)


if __name__ == '__main__':

    import doctest

    doctest.testmod()
