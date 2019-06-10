import pandas as pd
import pyodbc, os, configparser, logging
from .settings import BASE_DIR
import sqlalchemy, urllib

# logger config
def error_logging():
    try:
        # logger = error_logging()
        logging.config.fileConfig(os.path.join(BASE_DIR, 'logging_config.ini'))
        logger = logging.getLogger()
    except Exception as e:
        return e
    return logger


# reading config file
def read_config_file():
    try:
        logger = error_logging()
        config = configparser.ConfigParser()
        config.read(os.path.join(BASE_DIR, 'config.ini'))
    except Exception as e:
        logger.error(e)
        return e
    return config


def connectDB():
    try:
        config = read_config_file()
        logger = error_logging()
        driver = str(config['DATABASE']['driver'])
        server = str(config['DATABASE']['server'])
        database = str(config['DATABASE']['database_name'])
        username = str(config['DATABASE']['uid'])
        password = str(config['DATABASE']['pwd'])
        params = urllib.parse.quote("DRIVER={" + driver + "};SERVER=" + server + ";DATABASE=" + database + ";UID=" + username + ";pwd=" + password)
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        # cnxn = pyodbc.connect(
        #     'DRIVER={' + driver + '};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    except Exception as e:
        logger.error(e)
        return e
    return engine


def insert_into_db(df: pd.DataFrame, chunk_size: int, table_name: str)-> str:
    try:
        logger = error_logging()
        engine = connectDB()
        start_index = 0
        end_index = chunk_size if chunk_size < len(df) else len(df)
        while start_index != end_index:
            print(start_index, end_index)
            df.iloc[start_index:end_index, :].to_sql(str(table_name), con=engine, if_exists='append', index=False)
            print("batch inserted from ", str(start_index), " to ", str(end_index))
            start_index = min(start_index + chunk_size, len(df))
            end_index = min(end_index + chunk_size, len(df))
    except Exception as e:
        logger.error(str(e))
        return str(e)
    return "Data Pushed"
