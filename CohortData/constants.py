import numpy as np
from sklearn.linear_model import LinearRegression

'''
Names of files to be read and names of files exported that have been cleaned. Synced by index
'''
filesToRead = ['cohort_79_May10.csv', 'cohort_97_May10.csv']
nameOfFilesToExport = ['cleaned_79_may10.csv', 'cleaned_97_may10.csv']


'''
constant column names
'''
race = 'race'
adjusted_income = 'adjusted_income'
age = 'age'
curr_preg = 'curr_pregnant'
family_size = 'family_size'
urban_or_rural = 'urban_or_rural'
highest_grade = 'highest_grade'
marital_status = 'marital_status'
number_of_kids = 'number_of_kids'
industry = 'industry'
occupation = 'occupation'
unemployment = 'unemployment'
gdp_growth = 'gdp_growth'
inflation = 'inflation'
untouched_cols = [curr_preg, industry, occupation, number_of_kids, unemployment, gdp_growth, inflation]
hrs_wrkd_last_year = 'hours_worked_last_year'


'''
outlier constants
'''
outlier_stdNum = 3


'''
highest grade discretization constants
'''
highest_grade_binned = highest_grade + '_binned'
grade_map = {
    range(0,7): '0-6th',
    range(7,13): '7-12th',
    range(13, 99999): '13th and above'
}

def discretize_highest_grades(grade):
    for num_range, label in grade_map.items():
        if np.isnan(grade):
            return np.nan
        if grade in num_range:
            return grade_map[num_range]

    raise RuntimeError(f"discretization range not defined for grade: \"{grade}\"")

'''
regression to impute missing values for number_of_kids
'''
variables_considered = [age, family_size, marital_status]
variable_predicted = number_of_kids

def getRegression(dataframe):
    reg = LinearRegression().fit(dataframe[variables_considered], dataframe[variable_predicted])
    return reg
