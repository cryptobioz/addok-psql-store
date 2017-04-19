# Addok-psql-store

Store your documents into a postgresql database to save Redis RAM usage.


## Install

    pip install git+https://github.com/addok/addok-psql-store


## Configuration

The plugin will register itself when installed, by setting the correct
`DOCUMENT_STORE_PYPATH`. You can override this setting in your local
configuration file.

Other settings:

    PG_CONFIG = 'dbname=addok user=addok host=localhost password=addok'
    PG_TABLE = 'addok'
