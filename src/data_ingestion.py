import pandas as pd
import os
from sklearn.model_selection import train_test_split
import logging

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger("data_ingestion")
logger.setLevel("DEBUG")

console_handler = logging.StreamHandler()
console_handler.setLevel("DEBUG")

log_file_path = os.path.join(log_dir,'data_ingestion.log')
file_handler = logging.FileHandler()
file_handler.setLevel("DEBUG")

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

def load_data(data_url):
    """ This functionn loads data from a given url 

    Args:
        data_url (string): Url for the data to be loaded
    """
    try:
        df=pd.read_csv(data_url)
        logger.debug(f"Data loaded sucessfully from {data_url}")
        return df
    except Exception as e:
        logger.error(f"Exception {e} has occurred while loading data from url")
        
def preprocess(df):
    """Preprocesses the datafram

    Args:
        df (Pandas Dataframe): Dataframe containing Data
    """
    try:
        df.drop(columns = ['unnamed: 2', 'unnamed: 3', 'unnamed: 4'], inplace = True)
        df.rename(columns = {'v1' : 'target', 'v2' : 'text'},inplace = True)
        logger.debug(f" Data Preprocessed Successfully")
        return df
    except Exception as e:
        logger.error(f" Exception {e} occured while preprocessing dataframe df")

def save_data(train_data, test_data, data_path):
    """Saves the train and test data

    Args:
        train_data (Pandas Dataframe): Training Data
        test_data (Pandas Dataframe): Testing  Data
        data_path (str): Location where the Dataframes have to be stored
    """
    try:
        raw_data_path = os.path.join(data_path,"raw")
        os.makedirs(raw_data_path, exist_ok=True)
        train_data.to_csv(os.path.join(raw_data_path,"train"))
        test_data.to_csv(os.path.join(raw_data_path,"test"))
        logger.debug(f"Train and test dataframes stored to {raw_data_path}")
    except Exception as e:
        logger.error(f" Exception {e} has occured while saving the Dataframes")
        
def main():
     