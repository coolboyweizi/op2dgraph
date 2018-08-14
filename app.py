#!/usr/bin/env python

from module.oplogs import Oplogs
from config import db_host,db_cols,db_name


if __name__ == '__main__':

    oplogs = Oplogs(db=db_name,
                    host=db_host,
                    collections=db_cols)
    oplogs.out()
