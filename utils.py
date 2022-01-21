import re
import datetime
import pdfplumber
import docx


def get_table_dwh_patient_attributes():
    return ['HOSPITAL_PATIENT_ID', 'FIRSTNAME', 'LASTNAME', 'BIRTH_DATE', 'SEX', 'MAIDEN_NAME', 'RESIDENCE_ADDRESS', 'PHONE_NUMBER', 'ZIP_CODE',
            'RESIDENCE_CITY', 'DEATH_DATE', 'RESIDENCE_COUNTRY', 'DEATH_CODE', 'UPDATE_DATE', 'BIRTH_COUNTRY', 'BIRTH_CITY', 'BIRTH_ZIP_CODE']


def extract_text_pdf(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += "\r"+page.extract_text()
    return text


def extract_text_docx(filename):
    doc = docx.Document(filename)
    fullText = []
    for para in doc.paragraphs:
        fullText.append(para.text)
    return '\n'.join(fullText)


def valid_date(input_date, format, separator):
    if len(input_date.split(separator)) < 3:
        return False
    if(format == "dd"+separator+"mm"+separator+"yyyy"):
        day, month, year = input_date.split(separator)
    elif(format == "yyyy"+separator+"mm"+separator+"dd"):
        year, month, day = input_date.split(separator)
    elif(format == "mm"+separator+"dd"+separator+"yyyy"):
        month, day, year = input_date.split(separator)
    else:
        return False

    isValidDate = True
    try:
        date_val = datetime.datetime(int(year), int(month), int(day))
        if date_val > datetime.datetime.now():
            return False
    except ValueError:
        isValidDate = False
    return isValidDate


def has_more_one_char(str):
    return len(str) > 1


def is_valid_name(str):
    return bool(re.match("^[A-Za-z\s\']*$", str))


def has_no_repeating_word(str):
    return len(re.findall(r'(([A-Za-z])\2{2,})', str, re.IGNORECASE)) == 0


def validate_varchar_no_digit(str):
    return is_valid_name(str) and has_no_repeating_word(str)


def validate_place_name(str):
    return str and has_no_repeating_word(str)


def valid_phone_number(str):
    # only digit and more than four character in total
    return bool(re.match(r"^([0-9\s\.\-\(\)]){6,25}$", str))


def valid_address(str):
    # put at least one letter, one digit and may have some special characters and more than six character in total
    # return bool(re.match(r"^(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d\s\.\-\/\\\']{6,}$", str)) and has_no_repeating_word(str)
    return str and has_no_repeating_word(str)


def get_geographic_db(city, country, zip_code, cursor):
    sql = "select LATITUDE, LONGITUDE from DWH_THESAURUS_CITY where (CITY_NAME= ? and COUNTRY= ?) or ZIP_CODE= ?"
    cursor.execute(sql, (city, country, zip_code))
    return cursor.fetchall()
