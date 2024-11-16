import pandas as pd
import numpy as np
import warnings
import time

warnings.simplefilter(action='ignore', category=FutureWarning)

print('STARTING DATA EXTRACTING AND TRANSFORMING...\n')

columns_of_interest = ['QT_TABLET_ALUNO',
                       'QT_COMP_PORTATIL_ALUNO',
                       'QT_DESKTOP_ALUNO',
                       'IN_INTERNET',
                       'IN_INTERNET_APRENDIZAGEM',
                       'IN_BANDA_LARGA',
                       'NO_REGIAO',
                       'SG_UF',
                       'NO_MESORREGIAO',
                       'NO_MICRORREGIAO',
                       'NO_MUNICIPIO',
                       'QT_MAT_BAS',
                       'TP_DEPENDENCIA']

rows_per_iteration = 10000  # ADD X ROWS TO THE DATAFRAME PER ITERATION
rows_to_skip = 1            # SKIP THE HEADER FOR NOW (IT IS ADDED LATER)
is_exception = False        # WHEN THE END OF CSV IS REACHED, AN EXCEPTION IS RAISED, MEANING END OF FILE 

final_data = pd.DataFrame() # CREATES AN EMPTY DATAFRAME, TO WHICH THE GENERATED 
                            # DATAFRAMES WILL BE CONCATENATED ON EACH ITERATION                           

#######################################################################################################################

print('Iterating over csv to add rows to dataframe...')
start = time.time()

while not(is_exception):
    # "partial_data" IS THE PARTIAL DATAFRAME GENERATED ON EACH ITERATION
    try:
        partial_data = pd.read_csv(
            filepath_or_buffer='./data/microdados_ed_basica_2023.csv',  # PATH FROM ROOT FOLDER
            skiprows=rows_to_skip,                                      # AMOUNT OF ROWS TO BE SKIPED STARTING FROM ZERO
            header=None,                                                # FIRST ROW IS SKIPED, SO THERE IS NO HEADER
            encoding='latin-1',                                         # AN ENCODING THAT SEEMS TO WORK
            nrows=rows_per_iteration,                                   # AMOUNT OF ROWS READ FROM CSV
            on_bad_lines='error',                                       # SKIP LINES WITH MORE VALUES THAN COLUMNS (ERROR LINES)
            sep=';',                                                    # CSV SEPARATOR
        )

        rows_to_skip += rows_per_iteration  # ADD NUMBER OF ALREADY ITERATED ROWS TO BE SKIPED ON NEXT ITERATION
        final_data = pd.concat(             # CONCATENATES PARTIAL DATAFRAME TO THE FINAL ONE
            [final_data, partial_data],
            ignore_index=True
        )

    except Exception as e:
        if str(e) == 'No columns to parse from file':
            # OK, ALL ROWS WERE LOADED
            is_exception = True
        else:
            print("ERROR: " +  str(e))
            exit(1)

end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds\n')

# print(final_data)

#######################################################################################################################

print('Getting csv headers and adding them as dataframe headers...')
start = time.time()

try:
    # GET THE HEADERS
    headers = pd.read_csv(
        filepath_or_buffer='./data/microdados_ed_basica_2023.csv',
        header=None,
        encoding='latin-1',
        skiprows=0,
        nrows=1,
        sep=';'
    )

    # "headers.values" RETURNS A NUMPY ARRAY, THAT MUST BE CONVERTED INTO A 
    # PYTHON ARRAY TO BE ATRIBUTED AS THE DATAFRAME'S COLUMNS 
    actual_headers = []
    for header in headers.values:
        actual_headers.append(header)
    
    # ATRIBUTE HEADERS TO THE DATAFRAME
    final_data.columns = actual_headers
except Exception as e:
    print("ERROR: " +  str(e))
    exit(0)

end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds\n')

# print(final_data)

#######################################################################################################################

print('Getting only columns of interest...')
start = time.time()
try:
    final_data = final_data[columns_of_interest]
except Exception as e:
    print('ERROR: ' + str(e))
    exit(0)
end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds\n')

# print(final_data)

#######################################################################################################################

print('Removing lines with empty values or value "88888"...')
start = time.time()
try:
    all_tuples = final_data.itertuples(index=True)
    for row in all_tuples:
        for item in row:
            if (( type(item) == float ) and ( np.isnan(item) )) or ( item == 88888 ):
                final_data.drop(row[0], inplace=True)
                # print(f'REMOVED ROW OF INDEX [{row[0]}]')
                break
except Exception as e:
    print('ERROR: ' + str(e))
    exit(0)
end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds\n')

# print(final_data)s

#######################################################################################################################

print('Converting float values to int...')
start = time.time()
try:
    float_columns = [
        'QT_TABLET_ALUNO',
        'QT_COMP_PORTATIL_ALUNO',
        'QT_DESKTOP_ALUNO',
        'IN_INTERNET',
        'IN_INTERNET_APRENDIZAGEM',
        'IN_BANDA_LARGA',
        'QT_MAT_BAS'
    ]
    for i in float_columns:
        final_data[i] = final_data[i].astype(int)
    
except Exception as e:
    print('ERROR: ' + str(e))
    exit(0)
end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds\n')

# print(final_data)

#######################################################################################################################

print('Translating "TP_DEPENDENCIA" values...')
start = time.time()
try:

    def translate_tp_dependencia(value):
        match value[0]:

            case 1:
                return 'Federal'
            case 2:
                return 'Estadual'
            case 3:
                return 'Municipal'
            case 4:
                return 'Privada'

    final_data['TP_DEPENDENCIA'] = final_data['TP_DEPENDENCIA'].apply(
        translate_tp_dependencia, 
        axis=1
    )
except Exception as e:
    print('ERROR: ' + str(e))
    exit(1)
end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds\n')

#######################################################################################################################

print('Translating "IN_INTERNET", "IN_INTERNET_APRENDIZAGEM" and "IN_BANDA_LARGA" values...')
start = time.time()
try:

    def translate_zero_and_one(value):
        match value[0]:

            case 0:
                return 'Não'
            case 1:
                return 'Sim'

    final_data['IN_INTERNET'] = final_data['IN_INTERNET'].apply(
        translate_zero_and_one, 
        axis=1
    )
    final_data['IN_INTERNET_APRENDIZAGEM'] = final_data['IN_INTERNET_APRENDIZAGEM'].apply(
        translate_zero_and_one, 
        axis=1
    )
    final_data['IN_BANDA_LARGA'] = final_data['IN_BANDA_LARGA'].apply(
        translate_zero_and_one, 
        axis=1
    )
except Exception as e:
    print('ERROR: ' + str(e))
    exit(1)
end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds\n')

#######################################################################################################################

print('Writing data on new csv...')
start = time.time()
try:
    final_data.to_csv('./data/microdados_ed_basica_2023_transformed.csv', index=False)
except Exception as e:
    print('ERROR: ' + str(e))
    exit(1)
end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds\n')