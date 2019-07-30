#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-07-26 11:54:55
# @Last modified by: ArthurBernard
# @Last modified time: 2019-07-30 17:18:17

""" Tools and object to load, append and save differnet kind of database. """

# Built-in packages
from os import makedirs
import os.path
import time
import sqlite3
from pickle import Pickler, Unpickler
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

# External packages
import pandas as pd

# Local packages


__all__ = ['IODataBase', 'get_df', 'save_df']


class IODataBase:
    """ Object to save a pd.DataFrame into different kind of database.

    Attributes
    ----------
    path : str
        Path of the database.
    method : str {'DataFrame', 'SQLite', 'CSV', 'Excel', 'SQL'}
        Kind of database.
    parser : dict
        Values are function to corresponding to `method`.

    Methods
    -------
    save_as_dataframe(new_data, name=None, ext='.dat')
        Append and save `new_data` in database as pd.DataFrame binary object.
    save_as_sql(new_data, table='main_table', name=None, ext='.db',
                index=True, index_label=None)
        Append and save `new_data` in SQLite database.
    save_as_csv(new_data, name=None, ext='.csv', index=True, index_label=None)
        Append and save `new_data` in database as CSV format.
    save_as_excel(new_data, name=None, sheet_name='Sheet1', ext='.xlsx',
                  index=True, index_label=None)
        Append and save `new_data` in database as Excel format.
    __call__(new_data, **kwargs)
        Append and save `new_data` in database as `method` format.

    TODO:
    - Add SQL method
    - Add InfluxDB method
    - Add output methods

    """

    def __init__(self, path='./', method='csv'):
        """ Initialize saver object.

        Parameters
        ----------
        path : str, optional
            Path of the database, default is `'./'` (current directory).
        method : str {'DataFrame', 'SQLite', 'CSV', 'Excel'}, optional
            Format of database, default is CSV.

        """
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

        With pickle save as binary pd.DataFrame object, if `file_name` exists
        append to it `new_data` and save it, else save `new_data`.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        name : str, optional
            Name of the database, default is the current date.
        ext : str, optional
            Extension of the database, default is `'.dat'`.

        """
        if name is None:
            name = time.strftime('%y-%m-%d', time.time())

        # Load data
        database = get_df(self.path, name, ext='.dat')
        # Append new data
        database = database.append(new_data, sort=False)
        # Save new data
        save_df(database, self.path, name, ext='.dat')

    def save_as_sqlite(self, new_data, table='main_table', name=None,
                       ext='.db', index=True, index_label=None):
        """ Append and save `new_data` in SQL(ite) database.

        With sqlite, if `database` exists append to it `new_data`, else create
        a new data base.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        table : str, optional
            Name of the table, default is `'main_table'`.
        name : str, optional
            Name of the database, default is the current year.
        ext : str, optional
            Extension of the database, default is `'.db'`.
        index : bool, optional
            Write pd.DataFrame index as a column. Uses index_label as the
            column name in the table. Default is `True`.
        index_label : string or sequence, optional
            Column label for index column(s). If `None` is given (default) and
            index is `True`, then the index names are used. A sequence should
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

    def save_as_sql(self, new_data, table='main_table', name=None,
                    ext='.db', index=True, index_label=None, driver=None,
                    username=None, password=None, host=None, port=None):
        """ Append and save `new_data` in SQL(ite) database.

        With sqlite, if `database` exists append to it `new_data`, else create
        a new data base.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        table : str, optional
            Name of the table, default is `'main_table'`.
        name : str, optional
            Name of the database, default is the current year.
        ext : str, optional
            Extension of the database, default is `'.db'`.
        index : bool, optional
            Write pd.DataFrame index as a column. Uses index_label as the
            column name in the table. Default is `True`.
        index_label : string or sequence, optional
            Column label for index column(s). If `None` is given (default) and
            index is `True`, then the index names are used. A sequence should
            be given if the pd.DataFrame uses pd.MultiIndex.
        driver : str {'psycopg2', 'pg8000', 'mysqlclient', pymysql',
                      'cx_oracle', 'pyodbc', 'pymssql'}, optional
            The name of the DBAPI to be used to connect to the database using
            all lowercase letters. If not specified, a default DBAPI will be
            imported if available - this default is typically the most widely
            known driver available for that backend.
        username, password : str
            Username and password to connect to the SQL database.
        host : str, optional
            Host to connect, default is `'localhost'`.
        port : str, optional
            The port number, default is `None`.


        """
        if name is None:
            name = time.strftime('%y', time.gmtime(time.time()))

        # Open connection with database
        conn = create_engine(URL(
            self.method, username=username, password=password, host=host,
            port=port, database=self.path + name + ext,
        ))
        # Append data
        new_data.to_sql(table, con=conn, if_exists='append', index=index,
                        index_label=index_label)
        # Close connection
        conn.close()

    def save_as_csv(self, new_data, name=None, ext='.csv', index=True,
                    index_label=None):
        """ Append and save `new_data` in database as CSV format.

        With pickle save as binary pd.DataFrame object, if `file_name` exists
        append to it `new_data` and save it, else save `new_data`.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        name : str, optional
            Name of the database, default is the current year.
        ext : str, optional
            Extension of the database, default is `'.csv`'.
        index : bool, optional
            Write row names (index), default is `True`.
        index_label : str or sequence, optional
            Column label for index column(s) if desired. If not specified
            (default is `None`), and index are True, then the index names are
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

        With pickle save as binary pd.DataFrame object, if `file_name` exists
        append to it `new_data` and save it, else save `new_data`.

        Parameters
        ----------
        new_data : pd.DataFrame
            Data to append to the database.
        name : str, optional
            Name of the database, default is the current date.
        sheet_name : str, optional
            Name of sheet which will contain `new_data`, default is `'Sheet1'`.
        ext : str, optional
            Extension of the database, default is `'.xlsx'`.
        index : bool, optional
            Write row names (index), default is `True`.
        index_label : str or sequence, optional
            Column label for index column(s) if desired. If not specified
            (default is `None`), and index are `True`, then the index names are
            used. A sequence should be given if the DataFrame uses MultiIndex.

        Warning
        -------
        Slow method, not recommanded for large database.

        """
        path = self.path + name + ext

        if name is None:
            name = time.strftime('%y-%m-%d', time.gmtime(time.time()))

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
