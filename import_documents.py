import os
import sqlite3
from datetime import date
import utils
import re
import configparser

print("Integration started")
# Read configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# get all files in the folder
files = os.listdir(config['SOURCES']['FILEPATH_DOCUMENTS'])

# preparing queries
colums_name = ["PATIENT_NUM", "TITLE", "DOCUMENT_ORIGIN_CODE", "DOCUMENT_DATE",
               "ID_DOC_SOURCE", "DOCUMENT_TYPE", "DISPLAYED_TEXT", "AUTHOR", "UPDATE_DATE", "UPLOAD_ID"]
columns_to_str = ",".join(colums_name)
values_parameters = ["?"] * len(colums_name)
params_to_str = ",".join(values_parameters)
queries_insert = f"INSERT INTO DWH_DOCUMENT ({columns_to_str}) VALUES ({params_to_str})"


def get_last_upload_id(cursor):
    """
    Return new upload id for setting in DWH_DOCUMENT
    depending on the last upload id inserted in table DWH_DOCUMENT

    Parameters:
       cursor (sqlite Cursor): from connection opened outside of the function

    Returns:
      result (int): The upload id 

    """
    queries = "SELECT MAX(UPLOAD_ID) FROM DWH_DOCUMENT"
    cursor.execute(queries)
    result = cursor.fetchone()[0]
    if result is None:
        result = 0
    else:
        result += 1
    return result


def get_patient_num(IPP, cursor):
    """
    Return PATIENT_NUM corresponding on the HOSPITAL_PATIENT_ID

    Parameters:
        IPP (str): HOSPITAL_PATIENT_ID
       cursor (sqlite Cursor): from connection opened outside of the function

    Returns:
      result (int): PATIENT_NUM if found else None

    """
    queries_patient_num = "SELECT PATIENT_NUM FROM DWH_PATIENT_IPPHIST where HOSPITAL_PATIENT_ID = ?"
    cursor.execute(queries_patient_num, [IPP])
    result = cursor.fetchone()
    if result is None:
        return None
    return result[0]


def extract_document_informations(file_property, file_details, cursor):
    """
    Return all found informations of the document

    Parameters:
        file_property (tuple): stocking filename and extension ('filename','extension')
        file_details (list): stocking the patient IPP and DOC_ID [IPP,DOC_ID]
        cursor (sqlite Cursor): from connection opened outside of the function

    Returns:
      document (dict): document informations

    """
    # content of the file
    document = {
        "PATIENT_NUM": "",
        "TITLE": "",
        "DOCUMENT_ORIGIN_CODE": "",
        "DOCUMENT_DATE": "",
        "ID_DOC_SOURCE": "",
        "DOCUMENT_TYPE": "",
        "DISPLAYED_TEXT": "",
        "AUTHOR": ""
    }
    IPP = file_details[0]
    document["PATIENT_NUM"] = get_patient_num(IPP, cursor)
    document["ID_DOC_SOURCE"] = file_details[1]
    if file_property[1] == ".pdf":
        document["DOCUMENT_TYPE"] = "PDF"
        document["DOCUMENT_ORIGIN_CODE"] = "DOSSIER_PATIENT"
        document["DISPLAYED_TEXT"] = utils.extract_text_pdf(
            config["SOURCES"]["FILEPATH_DOCUMENTS"]+"/"+file).strip()
    elif file_property[1] == ".docx":
        document["DOCUMENT_TYPE"] = "DOCX"
        document["DOCUMENT_ORIGIN_CODE"] = "RADIOLOGIE_SOFTWARE"
        document["DISPLAYED_TEXT"] = utils.extract_text_docx(
            config["SOURCES"]["FILEPATH_DOCUMENTS"]+"/"+file).strip()

    # ------------------------ TITLE -------------------------------
    # Trying to find title if it's like "Compte rendu..."
    title_find = re.findall(r"(?<=Compte).*?(?=\r\n|\r|\n)",
                            document["DISPLAYED_TEXT"], re.IGNORECASE)
    # If finding line starting with "Compte"
    if len(title_find):
        for value in title_find:
            # Find if there's "rendu" inside the line
            if re.search(r'rendu', value, re.IGNORECASE):
                document["TITLE"] = "Compte"+value
                break
        # find document_date in the title
        # format dd-MM-YYYY or dd/MM/YYYY
        search_date_format_dmy = re.search(
            r'\d{2}(-|/)\d{2}(-|/)\d{4}', document["TITLE"])
        if search_date_format_dmy is not None:
            document["DOCUMENT_DATE"] = search_date_format_dmy.group()
        # format YYYY-MM-dd or YYYY/MM/dd
        else:
            search_date_format_ymd = re.search(
                r'\d{4}(-|/)\d{2}(-|/)\d{2}', document["TITLE"])
            if search_date_format_ymd is not None:
                document["DOCUMENT_DATE"] = search_date_format_ymd.group()
    # Find title "Ordonnance"
    else:
        title_find = re.findall(
            r"(?<=Ordonnance).*?(?=\r\n|\r|\n)", document["DISPLAYED_TEXT"], re.IGNORECASE)
        if len(title_find):
            document["TITLE"] = "Ordonnance "+title_find[0].strip()
            # find date in the header "Paris le XXXXX,""
            search_date_format_dmy = re.search(
                r'(?<=le\s)\d{2}(-|/)\d{2}(-|/)\d{4}?(?=\,)', document["DISPLAYED_TEXT"], re.IGNORECASE)
            if search_date_format_dmy is not None:
                document["DOCUMENT_DATE"] = search_date_format_dmy.group()
            else:
                search_date_format_ymd = re.search(
                    r'(?<=le\s)\d{4}(-|/)\d{2}(-|/)\d{2}?(?=\,)', document["DISPLAYED_TEXT"], re.IGNORECASE)
                if search_date_format_ymd is not None:
                    document["DOCUMENT_DATE"] = search_date_format_ymd.group()

    # --------------------- AUTHOR --------------------
    last_line_text = document["DISPLAYED_TEXT"].strip().split("\n")
    if re.search('Dr', last_line_text[-1], re.IGNORECASE) is not None:
        document["AUTHOR"] = last_line_text[-1].strip()

    return document


# ETL Pipeline
try:
    print("Connection opened")
    connection = sqlite3.connect(config['SOURCES']['DATABASE_PATH'])
    cursor = connection.cursor()
    # documents to insert
    documents = []
    UPLOAD_ID = get_last_upload_id(cursor)
    UPDATE_DATE = date.today()
    print("Retrieving informations")
    for file in files:
        # get filename and extension
        file_property = os.path.splitext(file)
        filename = file_property[0]
        file_details = filename.split("_")
        # file with invalid filename (not IPP_IDDOCUMENT) not treated
        if len(file_details) <= 1:
            continue
        row = list(extract_document_informations(
            file_property, file_details, cursor).values())
        row.extend([UPDATE_DATE, UPLOAD_ID])
        documents.append(row)
    print("Inserting data...")
    cursor.executemany(queries_insert, documents)
    connection.commit()
    print("Documents data integration finished...")

except Exception as error:
    print("Ann error occured...", error)

finally:
    if cursor:
        cursor.close()
        print("The Cursor is closed")
    if connection:
        connection.close()
        print("The SQLite connection is closed")
