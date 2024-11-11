import pandas as pd
import numpy as np
import warnings
import time

warnings.simplefilter(action='ignore', category=FutureWarning)

print('STARTING DATA EXTRACTING AND TRANSFORMING...')

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
                       'TP_DEPENDENCIA',
                       ]

rows_per_iteration = 100    # ADD X ROWS TO THE DATAFRAME PER ITERATION
rows_to_skip = 1            # SKIP THE HEADER FOR NOW (IT IS ADDED LATER)
is_exception = False        # WHEN REACH THE END OF CSV, MAYBE AN EXCEPTION IS RAISED
final_data = pd.DataFrame() # CREATES AN EMPTY DATAFRAME, TO WHICH THE GENERATED 
                            # DATAFRAMES WILL BE CONCATENATED ON EACH ITERATION

# iterations = 0              # REMOVE LATER, USED TO LIMIT DATAFRAME SIZE FOR NOW

#######################################################################################################################

print('Iterating over csv to add rows to dataframe...')
start = time.time()

while not(is_exception):
    start = time.time()
    # "partial_data" IS THE PARTIAL DATAFRAME GENERATED ON EACH ITERATION
    try:
        partial_data = pd.read_csv(
            filepath_or_buffer='./data/microdados_ed_basica_2023.csv',  # PATH FROM ROOT FOLDER
            skiprows=rows_to_skip,                                      # AMOUNT OF ROWS TO BE SKIPED STARTING FROM ZERO
            header=None,                                                # FIRST ROW IS SKIPED, SO THERE IS NO HEADER
            encoding='latin-1',                                         # AN ENCODING THAT SEEMS TO WORK
            nrows=rows_per_iteration                                    # AMOUNT OF ROWS READ FROM CSV
        )

        rows_to_skip += rows_per_iteration  # ADD NUMBER OF ALREADY ITERATED ROWS TO BE SKIPED ON NEXT ITERATION
        final_data = pd.concat(             # CONCATENATES PARTIAL DATAFRAME TO THE FINAL ONE
            [final_data, partial_data],
            ignore_index=True
        )
        # iterations += 1                     # REMOVE LATER, USED TO LIMIT DATAFRAME SIZE FOR NOW


        # REMOVE LATER, LIMITING DATAFRAME SIZE FOR NOW
        # if iterations == 5:
        #     is_exception = True

    except Exception as e:
        is_exception = True
        print("ERROR: " +  str(e))

end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds')

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
        nrows=1
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
print(f'Done in {spent_time} seconds')

#######################################################################################################################

print('Removing rows with excessive columns...')
start = time.time()
try:
    indexes_to_drop = []
    excessive_columns = final_data[np.nan]
    for index, row in excessive_columns.iterrows():

        values_arr = []
        for i in range(0, 8, 1):
            values_arr.append(row[i])

        for i in values_arr:
            if not(np.isnan(i)):
                indexes_to_drop.append(index)

    final_data.drop(indexes_to_drop, inplace=True)

except Exception as e:
    print('ERROR: ' + str(e))
    exit(0)

end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds')

#######################################################################################################################

print('Removing columns with "NaN" header...')
start = time.time()
try:
    final_data = final_data.drop([np.nan], axis=1)
except Exception as e:
    print('ERROR: ' + str(e))
    exit(0)
end = time.time()
spent_time = round(end - start, 5)
print(f'Done in {spent_time} seconds')

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
print(f'Done in {spent_time} seconds')

#######################################################################################################################

print('FINAL DATAFRAME ->')
print(final_data)