import configparser
import sqlite3
import pandas as pd
import utils
from datetime import date

# Read configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Read patient's data and specify some columns to be string for preventing python to convert values
data_patients = pd.read_excel(config['SOURCES']['FILEPATH_PATIENT'], dtype={
                              'BIRTH_DATE': str, 'HOSPITAL_PATIENT_ID': str, "PHONE_NUMBER": str, "DEATH_DATE": str})

# Get attributes of table DWH_PATIENT for checking that we have the right column names in the excel file
# because we will use these attributes as standard key to treat the datas
allows_header = utils.get_table_dwh_patient_attributes()
# Get all invalid column names
bad_headers = [
    col for col in data_patients.columns if col not in allows_header]
# Stop program if invalid column names
if len(bad_headers) > 0:
    print("Please change column names in the excel file with the used in table DWH_PATIENT")
    quit()

# Initialize variable
UPLOAD_ID = 0
UPDATE_DATE = date.today()

# Stocking excel rows which won't be inserted in the table DWH_PATIENT
columns_name = data_patients.columns.tolist()
# add column identifying why the row was not inserted
columns_name.append('ANOMALY')
not_clean_datas = pd.DataFrame(columns=columns_name)


#  Change missing data to empty string and remove duplicates
data_patients = data_patients.drop_duplicates().fillna("")
# Get all columns which will be filled in the table DWH_PATIENT according to the excel file
columns_to_insert = data_patients.columns.tolist()
columns_to_insert.remove('HOSPITAL_PATIENT_ID')


# Read configuration file
config = configparser.ConfigParser()
config.read('config.ini')

print('Integration starting...')
# Read patient's data and specify some columns to be string for preventing python to convert values
print("Reading file...")
data_patients = pd.read_excel(config['SOURCES']['FILEPATH_PATIENT'], dtype={
                              'BIRTH_DATE': str, 'HOSPITAL_PATIENT_ID': str, "PHONE_NUMBER": str, "DEATH_DATE": str})


# Get attributes of table DWH_PATIENT for checking that we have the right column names in the excel file
# because we will use these attributes as standard key to treat the datas
print("Checking headers...")
allows_header = utils.get_table_dwh_patient_attributes()
# Get all invalid column names
bad_headers = [
    col for col in data_patients.columns if col not in allows_header]
# Stop program if invalid column names
if len(bad_headers) > 0:
    print("Please change column names in the excel file with the used in table DWH_PATIENT")
    quit()

# Initialize variable
UPLOAD_ID = 0
UPDATE_DATE = date.today()

# Stocking excel rows which won't be inserted in the table DWH_PATIENT
columns_name = data_patients.columns.tolist()
# add column identifying why the row was not inserted
columns_name.append('ANOMALY')
not_clean_datas = pd.DataFrame(columns=columns_name)

print("Clean data")
#  Change missing data to empty string and remove duplicates
data_patients = data_patients.drop_duplicates().fillna("")

# Get all columns which will be filled in the table DWH_PATIENT according to the excel file
columns_to_insert = data_patients.columns.tolist()
columns_to_insert.remove('HOSPITAL_PATIENT_ID')

# Preparing queries
queries_update = "UPDATE DWH_PATIENT SET "
for column in columns_to_insert:
    queries_update += column + " = ? , "
queries_update += "UPLOAD_ID = ? , UPDATE_DATE = ? WHERE PATIENT_NUM = ?"

columns_to_insert.extend(["UPLOAD_ID", "UPDATE_DATE"])
values_parameters = ["?"] * len(columns_to_insert)
params_to_str = ",".join(values_parameters)
columns_to_str = ",".join(columns_to_insert)

queries_insert = f"INSERT INTO  DWH_PATIENT ({columns_to_str}) values ({params_to_str})"


# Use some columns for checking if same patient
queries_search_duplicates = """SELECT PATIENT_NUM, 
    case when LASTNAME like ? then 1 else 0 end + 
    case when FIRSTNAME like ? then 1 else 0 end +
    case when BIRTH_DATE like ? then 1 else 0 end +
    case when PHONE_NUMBER like ? then 1 else 0 end +
    case when MAIDEN_NAME like ? then 1 else 0 end +
    case when RESIDENCE_CITY like ? then 1 else 0 end +
    case when RESIDENCE_COUNTRY like ? then 1 else 0 end +
    case when RESIDENCE_ADDRESS like ? then 1 else 0 end 
    AS count FROM DWH_PATIENT where count >= ?
"""


def get_last_upload_id(cursor):
    """
    Return new upload id for setting in DWH_PATIENT and DWH_IPP_HIST 
    depending on the last upload id inserted in table DWH_PATIENT

    Parameters:
       cursor (sqlite Cursor): from connection opened outside of the function

    Returns:
      result (int): The upload id 

    """
    queries = "SELECT MAX(UPLOAD_ID) FROM DWH_PATIENT"
    cursor.execute(queries)
    result = cursor.fetchone()[0]
    if result is None:
        result = 0
    else:
        result += 1
    return result


def insert_patient(row, cursor):
    """
    Inserting new patient

    Parameters:
       row (pandas series): one row of the dataframe having patient datas
       cursor (sqlite Cursor): from connection opened outside of the function

    Returns:
      (bool): True if row inserted

    """

    IPP = row["HOSPITAL_PATIENT_ID"]
    values_to_insert = row.drop(labels=['HOSPITAL_PATIENT_ID'])
    values_to_list = values_to_insert.values.tolist()
    # search if patient already exist
    cursor.execute(queries_search_duplicates, (row["LASTNAME"], row["FIRSTNAME"], row["BIRTH_DATE"], row["PHONE_NUMBER"],
                   row["MAIDEN_NAME"], row["RESIDENCE_CITY"], row["RESIDENCE_COUNTRY"], row["RESIDENCE_ADDRESS"], int(config['DUPLICATION']['MATCHING_COUNT'])))
    duplicates = cursor.fetchall()
    # If patient exist
    if len(duplicates) > 0:
        patient_num = duplicates[0][0]
        values_to_list.extend([UPLOAD_ID, UPDATE_DATE, patient_num])
        # Update patient's informations
        cursor.execute(queries_update, values_to_list)
    else:
        values_to_list.extend([UPLOAD_ID, UPDATE_DATE])
        # Insert new patient
        cursor.execute(queries_insert, values_to_list)
        patient_num = cursor.lastrowid
    insert_patient_ipp_hist(cursor, patient_num, IPP)

    return True


def insert_patient_ipp_hist(cursor, patient_num, IPP):
    """
    Inserting new patient ipp
    Using trigger CHANGE_MASTER_PATIENT_ID for automatic change of MASTER_PATIENT_ID

    Parameters:
       cursor (sqlite Cursor): from connection opened outside of the function
       patient_num (int): PATIENT_NUM of patient
       IPP (str): HOSPITAL_PATIENT_ID of the patient 

    Returns:
      (bool): True if row inserted

    """
    queries_add_ipp_hist = "INSERT INTO DWH_PATIENT_IPPHIST VALUES (?,?,?,?,?)"
    cursor.execute(queries_add_ipp_hist, (patient_num, IPP,
                   config['SOURCES']['ORIGIN_PATIENT_ID'], config['CONSTANT']['LAST_MASTER_PATIENT_ID'], UPLOAD_ID))
    return True


# ETL Pipeline
try:
    connection = sqlite3.connect(config['SOURCES']['DATABASE_PATH'])
    print("Connection opened")
    cursor = connection.cursor()
    UPLOAD_ID = get_last_upload_id(cursor)
    print("Check and insert per row")
    for index, row in data_patients.iterrows():
        anomalies = ""
        # ---------------------- LASTNAME CHECK ------------------------
        if not utils.validate_varchar_no_digit(row["LASTNAME"]):
            anomalies += "LASTNAME "
        # ------------------------ FIRSTNAME CHECK ------------------------
        if not utils.validate_varchar_no_digit(row["FIRSTNAME"]):
            anomalies += "FIRSTNAME "
        # ----------------------- BIRTH DATE CHECK -----------------------
        if not utils.valid_date(row["BIRTH_DATE"], config['DATE']['FORMAT'], config['DATE']['SEPARATOR']):
            anomalies += "BIRTH_DATE "
        # ----------------------- HOPITAL PATIENT ID CHECK -------------------
        if not row["HOSPITAL_PATIENT_ID"]:
            anomalies += "HOSPITAL_PATIENT_ID "
        # ----------------------- SEX CHECK -----------------------
        if row["SEX"] != "F" and row["SEX"] != "M":
            anomalies += "SEX "
        # ----------------------- RESIDENCE_ADDRESS CHECK -----------------------
        if not utils.valid_address(row["RESIDENCE_ADDRESS"]):
            anomalies += "RESIDENCE_ADDRESS "
        # ----------------------- PHONE_NUMBER CHECK -----------------------
        if not utils.valid_phone_number(row["PHONE_NUMBER"]):
            anomalies += "PHONE_NUMBER "
        # ----------------------- RESIDENCE CITY CHECK -----------------------
        if not utils.has_no_repeating_word(row["RESIDENCE_CITY"]):
            anomalies += "RESIDENCE_CITY "
        # ----------------------- RESIDENCE_COUNTRY CHECK -----------------------
        if not utils.validate_place_name(row["RESIDENCE_COUNTRY"]):
            anomalies += "RESIDENCE_COUNTRY "
        # ----------------------- DEATH DATE CHECK -----------------------
        if row["DEATH_DATE"]:
            if not utils.valid_date(row["DEATH_DATE"], config['DATE']['FORMAT'], config['DATE']['SEPARATOR']):
                anomalies += "DEATH_DATE "
        # Checking if row has not clean values
        if anomalies:
            row["ANOMALY"] = anomalies.strip().replace(" ", ",")
            not_clean_datas = pd.concat([not_clean_datas, row.to_frame().T])
        else:
            insert_patient(row, cursor)
        connection.commit()
    print("Patients data integration finished...")

except Exception as error:
    print("An error occured !", error)
finally:
    if cursor:
        cursor.close()
        print("The Cursor is closed")
    if connection:
        connection.close()
        print("The SQLite connection is closed")

# Export invalid datas
not_clean_datas.to_excel(config["OUTPUT"]["FILEPATH_ERROR"] +
                         "Error_integration"+UPDATE_DATE.strftime('%Y-%m-%d')+".xlsx")
