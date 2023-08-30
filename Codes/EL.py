'''EL: Extract in the CSV raw data to Load in the postgresql database: "data_sources"'''

import pandas as pd
import numpy as np
import random
import os
from sqlalchemy import create_engine
# from database_connection import database_connect



def save_table(df_table, table_name, db_name='data_sources'):
    """
    function to save a data table in the postgresql database

    param: df_table (the dataframe table)
    param: table_name (the table name, how it wwill be stored in the postgresql database)
    
    return None
    """

    engine = create_engine(f'postgresql://postgres:postgres@localhost:5432/{db_name}')
    df_table.to_sql(table_name, engine, index=False, if_exists="replace")




def   Adding_specific_columns_with_randomly_duplicated_or_unique_values(dataset,columns_to_add):
    """
    function to Add specific columns with randomly duplicated or unique values
    
    return None
    """
    for column_name, values in columns_to_add.items():
        if isinstance(values, list):
            values_to_add = [random.choice(values) for _ in range(len(dataset))]
        else:
            values_to_add = values
        dataset[column_name] = values_to_add

    return dataset




def load_data():
    """
    function to load data

    return: df_training, df_test
    """
    data1 = pd.read_csv(os.path.join(os.getcwd(),"UIZ.CARE_Data_Structure_for_Training ML_Phase_2.csv"), sep = ";", header=0)

    data2 = pd.read_csv(os.path.join(os.getcwd(),"UIZ.CARE_Test_Data_August_2023-Vs2.csv"), sep = ";")
    
    return data1, data2




def data1_preprocessing(data1):
    """
    function to preprocess data on the data file UIZ.CARE_Data_Structure_for_Training ML_Phase_2.csv

    return: data1 (represents the data file UIZ.CARE_Data_Structure_for_Training ML_Phase_2.csv)
    """

    # Specify the column from which you want to replace the values
    starting_column = 'Cough'

    # Retrieve the index of the starting column
    start_column_index = data1.columns.get_loc(starting_column)

    # Replace values in first row with column names from start column
    data1.iloc[0, start_column_index:] = data1.columns[start_column_index:]

    # Set the column names to the values in the first row
    data1.columns = data1.iloc[0]

    # Drop the first row (containing old column names)
    data1 = data1[1:].reset_index(drop=True)

    # Change the name of the last column
    new_column_name = 'Disease'  # Replace this with the new name you want
    data1.rename(columns={data1.columns[-1]: new_column_name}, inplace=True)
    
    
    # Update column names to lowercase
    data1.columns = data1.columns.str.lower()

    return data1




# Transformation in the second dataframe
def transformation_2nd_df(dataframe):
    dataframe.columns = dataframe.columns.str.lower()
    return dataframe




def combining_the_two_datasets(data1, data2):
    """
    function to combine the two loaded dataset

    param: data1 (data file representing the file UIZ.CARE_Data_Structure_for_Training ML_Phase_2.csv)
    param: data2 (data file representing teh file file UIZ.CARE_Data_Structure_for_Training ML_Phase_2.csv)
    
    return df (final dataframe result)
    """
    # Define the column names to merge on
    columns_to_merge = ['id', 'first_name', 'last_name', 'email', 'gender', 'ip_address',
        'adress', 'gp first_name', 'gp last_name', 'country', 'gp address',
        'gp postal_code', 'gp country', 'gp phone_number']
   
    
    data2 = transformation_2nd_df(data2)

    
    # Convert 'GP postal_code' column to object type in the second DataFrame df_test
    data2['gp postal_code'] = data2['gp postal_code'].astype(data1['gp postal_code'].dtype)
    
    # print(data2['gp postal_code'].dtype)
    # print(data1['gp postal_code'].dtype)

    # Concatenating DataFrames horizontally
    df = pd.concat([data1, data2], axis=1)
    
    
    # Removing duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    
    return df
    

def symptoms_table_creation_and_saving():
    """
    function to create and save the stymtoms table into the postgresql
    
    return None
    """
    symptoms_data_to_keep = ['id', 'cough',
       'difficulty breathing ', 'fatigue', 'fever', 'headaches', 'body pain',
       'loss of tates and smell', 'runny nose ', 'sneezing ', 'wheezing',
       'vomitting ', 'sore throat ', 'disease']
    
    df_symptoms = df[symptoms_data_to_keep].copy()
    # Adding a 'timestamp' column
    df_symptoms['timestamp'] = '18/08/2023'


    # Define the target table name
    table_name = "symptoms"
    save_table(df_symptoms, table_name)
    
    
    
    
def wearable_table_creation():
    """
    function to create and save the wearable table into the postgresql
    
    return None
    """
    wearable_data_to_keep = ['id', 'heart_rate','blood_pressure', 'intensity', 'body_temperature',
                            'respiratory_rate', 'stress_level', 'blood_oxygen_level','steps', \
                                'calories_burned','trigger', 'sleep_duration']
    
    columns_to_add = {
        'model': ['serie 8', 'serie7', 'ultra'],
        'device': ['fitbit', 'apple'],
        'timestamp': '18/08/2023'
    }
    
    df_wearable = df[wearable_data_to_keep].copy()

    # Adding specific columns with randomly duplicated or unique values
    df_wearable = Adding_specific_columns_with_randomly_duplicated_or_unique_values(df_wearable,columns_to_add)

    table_name = "wearable"
    save_table(df_wearable, table_name)
    
def emotional_table_creation_saving():
    """
    function to create and save the wearable table into the postgresql
    
    return None
    """
    
    emotional_data_to_keep = ['id', 'emotion_id', 'heart_rate','emotion_type', 'emotion_date',
        'emotion_time', 'emotion_duration', 'trigger', 'mood']
    
    df_emotional = df[emotional_data_to_keep].copy()

    # Dictionary of columns to add with their respective values to duplicate
    columns_to_add = {
        'model': ['serie 8', 'serie7', 'ultra'],
        'device': ['fitbit', 'apple'],
        'timestamp': '18/08/2023'
    }

    # Adding specific columns with randomly duplicated or unique values
    df_emotional = Adding_specific_columns_with_randomly_duplicated_or_unique_values(df_emotional,columns_to_add)

    table_name = "emotional"
    save_table(df_emotional, table_name)
    
    
def GenZ_table_creation_saving():
    """
    function to create and save the GenZ table into the postgresql
    
    return None
    """
    GenZ_data_to_keep = ['id', 'first_name', 'last_name', 'email', 'gender', 'ip_address',
        'adress']
    df_GenZ = df[GenZ_data_to_keep].copy()

    # Dictionary of columns to add with their respective values to duplicate
    columns_to_add = {
        'age': [16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
        'timestamp': '18/08/2023'
    }

    # Adding specific columns with randomly duplicated or unique values
    df_GenZ = Adding_specific_columns_with_randomly_duplicated_or_unique_values(df_GenZ,columns_to_add)

    table_name = "genz"
    save_table(df_GenZ, table_name)



def GP_table_creation_saving():
    """
    function to create and save the GP table into the postgresql
    
    return None
    """

    GP_data_to_keep = ['id', 'gp first_name', 'gp last_name', 'country', 'gp address',
                    'gp postal_code', 'gp country', 'gp phone_number']
    df_GP = df[GP_data_to_keep].copy()

    # Dictionary of columns to add with their respective values to duplicate
    columns_to_add = {'timestamp': '18/08/2023'}
    df_GP = Adding_specific_columns_with_randomly_duplicated_or_unique_values(df_GP,columns_to_add)

    table_name = "gp"
    save_table(df_GP, table_name)




data1, data2 = load_data()
df = data1_preprocessing(data1)
df = combining_the_two_datasets(df, data2)

#symptoms_table_creation_and_saving()
#wearable_table_creation()



#emotional_table_creation_saving()




#GenZ_table_creation_saving()



#GP_table_creation_saving()
