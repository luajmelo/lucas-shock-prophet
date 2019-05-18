import time
import pandas as pd
import constants as con # if needed, change it according to your developing env's pathing


if __name__ == "__main__":
    #keep a timer
    start = time.time()

    '''
    increase number of columns displayed for ease during debugging
    '''
    pd.set_option('display.max_columns', 27)

    for i in range(len(con.filesToRead)):
        fileName = con.filesToRead[i]
        df = pd.read_csv(fileName)

        '''
        remove all cases that have negative values for columns that are not in untouched_cols
        '''
        columns = df.columns.tolist()
        column_toCheckForNegVals = [col for col in df.columns if col not in con.untouched_cols]
        mask_allPosCols = []
        for col in column_toCheckForNegVals:
            newMaskForCol = ((df[col] >= 0)) | (df[col].isna())
            # add "| (df[col].isna())" to mask to retain samples with null values
            # remove "| (df[col].isna())" to delete samples with null values
            if isinstance(mask_allPosCols, list):
                mask_allPosCols = newMaskForCol
            else:
                mask_allPosCols = mask_allPosCols & newMaskForCol
        df = df[mask_allPosCols]

        '''
        replace -4 with 2 in curr_pregnant
        then delete cases with negative values in curr_pregnant
        '''
        mask_curPregnant_neg4 = df[con.curr_preg] == -4
        df.loc[mask_curPregnant_neg4, con.curr_preg] = 2
        df = df[df[con.curr_preg] >= 0 | df[con.curr_preg].isna()]

        '''
        If hours_worked_last_year higher than 3 std devs, make it 3 std devs
        '''
        stdDev = df[con.hrs_wrkd_last_year].std()
        mean = df[con.hrs_wrkd_last_year].mean()
        # print(f'hrs_workd last year mean {mean}')
        # print(f'hrs workd last year std {stdDev}')
        highEndVal = (con.outlier_stdNum * stdDev) + mean
        highEndMask = df[con.hrs_wrkd_last_year] > highEndVal
        df.loc[highEndMask, con.hrs_wrkd_last_year] = highEndVal

        '''
        fill in missing values for num_kids
        '''
        # first delete samples with negative values fro num kids
        retained_numKids_Mask = df[con.number_of_kids] >= 0 | df[con.number_of_kids].isna()
        df = df[retained_numKids_Mask]

        # use linear regression to predict missing value
        reg = con.getRegression(df)
        for row in df[con.variables_considered].itertuples():
            idx = row[0]
            row = row[1:]
            imputed_val = reg.predict([row])
            df.at[idx, con.variable_predicted] = imputed_val

        '''
        highest_grades discretization
        For specific labels of bins, see constants.py
        '''
        df[con.highest_grade_binned] = df[con.highest_grade].apply(con.discretize_highest_grades)

        # reorder columns so highest_grade_binned comes after highest_grade
        colNames = list(df.columns.values)
        colNames.remove(con.highest_grade_binned)
        hst_grd_idx = colNames.index(con.highest_grade)
        colNames.insert(hst_grd_idx + 1, con.highest_grade_binned)
        df = df[colNames]
    
        '''
        export cleaned data
        '''
        df.to_csv(con.nameOfFilesToExport[i], index=False)

    print(f'total execution time {round(time.time() - start, 4)}s')