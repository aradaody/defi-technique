# Configurations

in the config.ini file, fill these informations
 - FILEPATH_PATIENT = path of the file (name folder + name file.xlsx) that contains patient datas  
 Separate the two FILEPATH  
 !! Don't put punctuations for folder and file name
 - FILEPATH_DOCUMENTS = path of the folder (name of the folder) containing documents to integrate  
 Put only the documents to be imported in DWH_DOCUMENT in the folder
 - MATCHING_COUNT = Number to check if it's a duplicated patient 
    If we have more than "MATCHING_COUNT" columns with identical values when comparing to existing patient it's a duplicated patient 
 - SEPARATOR = separator of the day, month, year in date format

- I created a trigger from script.sql in the database  
!!Install extensions: pdfplumber, python-docx ... and others if missing