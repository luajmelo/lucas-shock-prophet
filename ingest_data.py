import os
import nlsy


if __name__ == '__main__':

    print("Creating NLSY database...")
    NLSY_db = nlsy.NLSY_database("data.db", True)

    print("Ingesting years data...")
    year_path = os.path.join('data', 'years.csv')
    NLSY_db.add_years_data(year_path)

    print("Ingesting and wrangling 1979 cohort data...")
    cohort_79 = NLSY_db.add_cohort(1979)
    rnum_path = os.path.join('data', 'dataset_rnum.NLSY79')
    qname_path = os.path.join('data', 'dataset_qname_with_year.NLSY79')
    responses_path = os.path.join('data', 'NLSY79.csv')
    cohort_79.add_cohort_data(rnum_path, qname_path, responses_path)

    print("Ingesting and wrangling 1997 cohort data...")
    cohort_97 = NLSY_db.add_cohort(1997)
    rnum_path = os.path.join('data', 'dataset_rnum.NLSY97')
    qname_path = os.path.join('data', 'dataset_qname_with_year.NLSY97')
    responses_path = os.path.join('data', 'NLSY97.csv')
    cohort_97.add_cohort_data(rnum_path, qname_path, responses_path)

    print("Done!")
