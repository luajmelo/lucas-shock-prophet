import os
import sqlite3
import csv
import json
import pandas as pd
import pdb

class NLSY_database(object):

    def __init__(self, path, initialize = False, db_structure="db_structure.json"):

        self._cohorts = []

        if initialize:
            if os.path.exists(path):
                overwrite = input("Continuing will delete your existing NLSY database. Continue (y/n)? ")
                if overwrite == "y":
                    os.remove(path)
                    self._conn = sqlite3.connect(path)
                else:
                    print("Exiting...")
                    exit()
            else:
                self._conn = sqlite3.connect(path)
        else:
            self._conn = sqlite3.connect(path)
            cursor = self._conn.cursor()

            # Check to see if years data exists in the database...
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = 'years'")
            row = cursor.fetchone()
            if row:
                self._years_table = "years"
            else:
                self._years_table = False

            # Check to see if region data exists in the database...
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = 'region_data'")
            row = cursor.fetchone()
            if row:
                self._region_table = "region_data"
            else:
                self._region_table = False

            # Check to see if cohort data has already been imported...
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'questions_%'")
            rows = cursor.fetchall()
            for row in rows:
                cohort_year = row[0][-4:]
                self.add_cohort(cohort_year, False)

        # This JSON file lays out the standard structure for each cohort's data, ensuring that
        # parallel data is collected on each of them.
        with open(db_structure) as json_file:
            self._db_structure = json.load(json_file)

    @property
    def cohorts(self):
        return self._cohorts

    @property
    def conn(self):
        return self._conn

    @property
    def db_structure(self):
        return self._db_structure

    @property
    def years_table(self):
        return self._years_table

    @property
    def region_table(self):
        return self._region_table

    def add_cohort(self, cohort_year, initialize=True):
        new_cohort = Cohort(self, cohort_year, initialize)
        self._cohorts.append(new_cohort)
        return new_cohort

    def add_years_data(self, year_path):
            """
            Creates the years table and stores all year-specific data in it.
            """
            cursor = self.conn.cursor()

            # The years table contains economic and other data associated with a specific year.
            cursor.execute("""CREATE TABLE IF NOT EXISTS years (
                year INTEGER PRIMARY KEY,
                unemployment REAL NOT NULL,
                gdp_growth REAL NOT NULL,
                inflation REAL NOT NULL
            )""")

            with open(year_path) as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=',')
                for row in csv_reader:
                    cursor.execute("""INSERT INTO
                        years (year, unemployment, gdp_growth, inflation)
                        VALUES (?, ?, ?, ?)
                        """, (row["year"], row["unemployment"],  row["gdp_growth"], row["inflation"]))

            self._years_table = "years"

            self.conn.commit()
            cursor.close()

    def add_region_data(self, region_path):
            """
            Creates the region table and stores all region-specific data in it.
            """
            cursor = self.conn.cursor()

            # The region table contains year-specific regional data.
            cursor.execute("""CREATE TABLE IF NOT EXISTS region_data (
                year INTEGER NOT NULL,
                region INTEGER NOT NULL,
                regional_unemployment REAL NOT NULL
            )""")

            with open(region_path) as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=',')
                for row in csv_reader:
                    #pdb.set_trace()
                    cursor.execute("""INSERT INTO
                        region_data (year, region, regional_unemployment)
                        VALUES (?, ?, ?)
                        """, (row["year"], row["region"], row["regional_unemployment"]))

            self._region_table = "region_data"

            self.conn.commit()
            cursor.close()


class Cohort(object):

    def __init__(self, NLSY_db, cohort_year, initialize = True, dictionary = "translate.json"):
        self._NLSY_db = NLSY_db
        self._cohort_year = cohort_year
        self._questions_table = "questions_{}".format(cohort_year)
        self._rnums_table = "rnums_{}".format(cohort_year)
        self._responses_table = "responses_{}".format(cohort_year)
        self._wrangled_respondents_table = "wrangled_respondents_{}".format(self._cohort_year)
        self._wrangled_data_table = "wrangled_data_{}".format(self._cohort_year)

        with open(dictionary) as json_file:
            self._dictionary = json.load(json_file)

        if initialize:
            self._create_data_tables()

    @property
    def cohort_year(self):
        return self._cohort_year

    def _create_data_tables(self):
        """
        Creates the individual RNUMs, responses, and questions tables. (Because RNUMs, which
        serve as primary keys in the NLSY data set, are reused across cohorts, it's safest and
        easiest to segregate each cohort's data into its own tables.)
        """

        cursor = self._NLSY_db.conn.cursor()

        # The questions table contains the NLSY-provided unique question name, as well as human-readable description of each question.
        cursor.execute("""CREATE TABLE {} (
            question_name TEXT PRIMARY KEY,
            description TEXT
        )""".format(self._questions_table))

        # The RNUMs table associates NLSY-provided unique RNUMs with a specific question.
        cursor.execute("""CREATE TABLE {} (
            rnum TEXT PRIMARY KEY,
            question_name TEXT NOT NULL,
            year INTEGER NOT NULL
        )""".format(self._rnums_table))

        # The responses table contains all of the individual responses to each RNUM.
        cursor.execute("""CREATE TABLE {} (
            response_id INTEGER PRIMARY KEY AUTOINCREMENT,
            rnum TEXT NOT NULL,
            case_id INTEGER NOT NULL,
            response INTEGER NOT NULL
        )""".format(self._responses_table))

        # The respondents table contains static information on each respondent
        # in the cohort, such as race, gender, etc.
        respondents_fields = self._NLSY_db.db_structure["wrangled_respondents_fields"]
        sql_query = "CREATE TABLE {} ( case_id INTEGER PRIMARY KEY".format(self._wrangled_respondents_table)
        for field in respondents_fields:
            sql_query = "{}, {} INTEGER".format(sql_query, field)
        cursor.execute("{})".format(sql_query))

        # The wrangled data table includes year-specific responses, normalized
        # to the style specified in our codebook.
        data_fields = self._NLSY_db.db_structure["wrangled_data_fields"]
        sql_query = "CREATE TABLE {} ( data_id INTEGER PRIMARY KEY".format(self._wrangled_data_table)
        for field in data_fields:
            sql_query = "{}, {} INTEGER".format(sql_query, field)
        cursor.execute("{})".format(sql_query))

        self._NLSY_db.conn.commit()
        cursor.close()

    def add_cohort_data(self, rnum_path, qname_path, responses_path, verbose=True):
        """
        Ingests all of the RNUM, question, and response data for a given cohort.
        """
        cursor = self._NLSY_db.conn.cursor()

        # The RNUMs and associated question names are stored on equivalent line numbers in two different files.
        if verbose:
            print("Ingesting {} survey data...".format(self._cohort_year))
        with open(rnum_path, 'r') as rnum_file:
            with open(qname_path, 'r') as qname_file:
                for rnum in rnum_file:
                    rnum = rnum.strip()
                    qname_content = qname_file.readline().strip()
                    (question_name, year) = qname_content.split(",")

                    cursor.execute("""INSERT INTO
                                {} (rnum, question_name, year)
                                VALUES (?, ?, ?)
                                """.format(self._rnums_table), (rnum, question_name, year))
                    cursor.execute("""INSERT OR IGNORE INTO
                                {} (question_name)
                                VALUES (?)
                                """.format(self._questions_table), (question_name, ))

        with open(responses_path) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for row in csv_reader:

                # R0000100 is a special value indicating the respondent's case ID.
                case_id = row["R0000100"]

                # Now, save all the variable response to the "responses" table.
                for rnum, response in row.items():
                    cursor.execute("""INSERT INTO
                        {} (rnum, case_id, response)
                        VALUES (?, ?, ?)
                        """.format(self._responses_table), (rnum, case_id, response))

        self._NLSY_db.conn.commit()
        cursor.close

        # Now that the data is in the database, the following steps wrangle it
        # into shape, including separating responses out by year, normalizing
        # survey codes to match our codebook, adjusting for inflation, and
        # labeling income shocks.
        if verbose:
            print("Restructuring {} data into longitudinal form...".format(self._cohort_year))
        self._wrangle_respondents_data()
        self._wrangle_survey_data()
        if verbose:
            print("Updating {} data to match codebook...".format(self._cohort_year))
        self._translate_respondents_data()
        self._translate_survey_data()
        self._translate_employer_data()
        self._adjust_for_inflation()
        if verbose:
            print("Labeling income shocks for {} cohort...".format(self._cohort_year))
        self._label_shocks()

    def _wrangle_respondents_data(self):
        """
        Adds the static data, such as race and sex, to the respondents table.
        """
        cursor = self._NLSY_db.conn.cursor()
        respondents_fields = self._NLSY_db.db_structure["wrangled_respondents_fields"]

        for field in respondents_fields:
            # The field provided here is the standard code (i.e., "race"), so
            # we need to identify what question it's associated with.
            for question_name, field_name in self._dictionary["static_question_names"][str(self._cohort_year)].items():
                if field_name == field:
                    break

            # Pull all of the data for a specific static field (e.g., race, sex)
            # for the cohort.
            cursor.execute("""SELECT {responses}.case_id, {responses}.response
            	FROM {questions} INNER JOIN {rnums}
            		ON {questions}.question_name = "{question_name}"
                    AND {questions}.question_name = {rnums}.question_name
            	INNER JOIN {responses}
            		ON {rnums}.rnum = {responses}.rnum""".format(
                        responses = self._responses_table,
                        rnums = self._rnums_table,
                        questions = self._questions_table,
                        question_name = question_name
                        )
                    )

            # Now, write all that data into the wrangled respondents table.
            rows = cursor.fetchall()
            for row in rows:
                (case_id, response) = row
                cursor.execute("""INSERT INTO
                    {respondents} (case_id, {field})
                    VALUES (?, ?)
                    ON CONFLICT (case_id) DO UPDATE SET {field} = (?)""".format(
                        case_id = case_id,
                        respondents = self._wrangled_respondents_table,
                        field = field),
                    (case_id, response, response)
                )

        self._NLSY_db.conn.commit()
        cursor.close()

    def _wrangle_survey_data(self, verbose=True):
        """
        Adds the data that varies by year, such as survey responses, to the
        wrangled_data table.
        """

        cursor = self._NLSY_db.conn.cursor()
        respondents_fields = self._NLSY_db.db_structure["wrangled_respondents_fields"]

        cursor.execute("""SELECT {respondents}.case_id, {rnums}.year,  {rnums}.question_name, {responses}.response
            FROM {respondents}
                INNER JOIN {responses} ON {respondents}.case_id = {responses}.case_id
                INNER JOIN {rnums} ON {responses}.rnum = {rnums}.rnum
            ORDER BY {respondents}.case_id, {rnums}.year""".format(
                respondents = self._wrangled_respondents_table,
                rnums = self._rnums_table,
                responses = self._responses_table
                )
            )

        rows = cursor.fetchall()
        curr_year = 0
        curr_case_id = 0
        curr_data_id = 0

        for row in rows:
            (case_id, year, question_name, response) = row

            if case_id != curr_case_id:
                if verbose and case_id % 1000 == 0:
                    print("{} respondents completed...".format(case_id))
                curr_case_id = case_id
                data_id = {}

            if not question_name in self._dictionary["dynamic_question_names"][str(self._cohort_year)]:
                continue

            # If the year has changed, insert in a new record and record its ID.
            if year != curr_year and year != "XRND":
                curr_year = year
                cursor.execute("""INSERT INTO
                    {data} (case_id, year)
                    VALUES (?, ?)""".format(
                        data = self._wrangled_data_table
                    ),
                    (case_id, year)
                )
                data_id[year] = cursor.lastrowid

            # The 1997 cohort data uses "XRND" as the year for constructed variables.
            if year == "XRND":
                last_two_digits = question_name[-2:]
                # If the last two digits begin with "9" (e.g., "97", "98", it's the 1990s...
                if last_two_digits[0] == "9":
                    year = int("19{}".format(last_two_digits))
                else:
                    year = int("20{}".format(last_two_digits))

            question_name = self._dictionary["dynamic_question_names"][str(self._cohort_year)][question_name]
            cursor.execute("""UPDATE {data}
                SET {question_name} = ?
                WHERE data_id = {data_id}""".format(
                    data = self._wrangled_data_table,
                    question_name = question_name,
                    data_id = data_id[year]),
                (response, )
            )

        self._NLSY_db.conn.commit()
        cursor.close()

    def _translate_respondents_data(self):
        """
        Update all static respondent characteristics to conform to our standard codebook.
        """
        cursor = self._NLSY_db.conn.cursor()
        fields_to_translate = self._dictionary["static_question_values"][str(self._cohort_year)]

        for question_name, translate_dict in fields_to_translate.items():
            for key, value in translate_dict.items():
                cursor.execute("""UPDATE {respondents}
                    SET {question_name} = ?
                    WHERE {question_name} = ?""".format(
                        respondents = self._wrangled_respondents_table,
                        question_name = question_name),
                    (value, key)
                )

        self._NLSY_db.conn.commit()
        cursor.close()

    def _translate_survey_data(self):
        """
        Update all survey responses (other than industry and occupation data,
        which are handled separately) to conform to our standard codebook.
        """
        cursor = self._NLSY_db.conn.cursor()
        fields_to_translate = self._dictionary["dynamic_question_values"][str(self._cohort_year)]

        for question_name, translate_dict in fields_to_translate.items():
            for key, value in translate_dict.items():
                # If the translation value is a dictionary, that means that different
                # translations must be performed for each year.
                if isinstance(value, dict):
                    year = key
                    for find_value, replace_value in value.items():
                        cursor.execute("""UPDATE {data}
                            SET {question_name} = ?
                            WHERE {question_name} = ?
                            AND YEAR = ?""".format(
                                data = self._wrangled_data_table,
                                question_name = question_name),
                            (replace_value, find_value, year)
                        )
                # Otherwise, simply make the same translation for all responses
                # to this question, regardless of year.
                else:
                    cursor.execute("""UPDATE {data}
                        SET {question_name} = ?
                        WHERE {question_name} = ?""".format(
                            data = self._wrangled_data_table,
                            question_name = question_name),
                        (value, key)
                    )

        self._NLSY_db.conn.commit()
        cursor.close()

    def _translate_employer_data(self, industry_file="industry_crosswalk.csv", occupation_file="occupation_crosswalk.csv"):
        """
        Update industry and occupation responses to conform to our standard codebook.
        """
        cursor = self._NLSY_db.conn.cursor()
        industry_crosswalk = pd.read_csv(industry_file)
        occupation_crosswalk = pd.read_csv(occupation_file)

        # Update industry data...
        for index, row in industry_crosswalk.iterrows():
            # The 1979 data is coded using two different sets of census codes, depending on the year.
            if self.cohort_year == 1979:
                if not pd.isna(row["1970"]):
                    cursor.execute("""UPDATE {data}
                                        SET industry = ?
                                        WHERE industry = ?
                                        AND YEAR < 2002""".format(
                                            data = self._wrangled_data_table),
                                        (row["IND1990"], row["1970"]))

                if not pd.isna(row["2000"]):
                    cursor.execute("""UPDATE {data}
                                        SET industry = ?
                                        WHERE industry = ?
                                        AND YEAR >= 2002""".format(
                                            data = self._wrangled_data_table),
                                        (row["IND1990"], row["2000"]))

            if self.cohort_year == 1997:
                if not pd.isna(row["ACS 2003-"]):
                    cursor.execute("""UPDATE {data}
                                        SET industry = ?
                                        WHERE industry = ?""".format(
                                            data = self._wrangled_data_table),
                                        (row["IND1990"], row["ACS 2003-"]))

        # Update ocupation data...
        for index, row in occupation_crosswalk.iterrows():
            # The 1979 data is coded using two different sets of census codes, depending on the year.
            if self.cohort_year == 1979:
                if not pd.isna(row["1970"]):
                    cursor.execute("""UPDATE {data}
                                        SET occupation = ?
                                        WHERE occupation = ?
                                        AND YEAR < 2002""".format(
                                            data = self._wrangled_data_table),
                                        (row["OCC2010"], row["1970"]))

                if not pd.isna(row["2000"]):
                    cursor.execute("""UPDATE {data}
                                        SET occupation = ?
                                        WHERE occupation = ?
                                        AND YEAR >= 2002""".format(
                                            data = self._wrangled_data_table),
                                        (row["OCC2010"], row["2000"]))

            if self.cohort_year == 1997:
                if not pd.isna(row["ACS 2003-2009"]):
                    cursor.execute("""UPDATE {data}
                                        SET occupation = ?
                                        WHERE occupation = ?""".format(
                                            data = self._wrangled_data_table),
                                        (row["OCC2010"], row["ACS 2003-2009"]))


        self._NLSY_db.conn.commit()
        cursor.close()

    def _adjust_for_inflation(self):
        """
        Update all income figures to account for inflation.
        """
        cursor = self._NLSY_db.conn.cursor()
        inflation_dict = {}

        cursor.execute("""SELECT year, inflation
            FROM {years} ORDER BY year""".format(
                years = self._NLSY_db.years_table
                )
            )

        rows = cursor.fetchall()
        for row in rows:
             year = row[0]
             inflation_rate = row[1]
             inflation_dict[year] = inflation_rate

        # Adjust for inflation.
        adjust_year = year
        for year, inflation_rate in inflation_dict.items():
            inflation_adjustment = 1
            for inflation_year in range(year, adjust_year):
                inflation_adjustment *= (1 + inflation_dict[inflation_year])
            cursor.execute("""UPDATE {data}
                SET adjusted_income = ROUND(adjusted_income * {inflation}, 0)
                WHERE adjusted_income > 0 AND year = {year}""".format(
                    data = self._wrangled_data_table,
                    inflation = inflation_adjustment,
                    year = year
                )
            )

        self._NLSY_db.conn.commit()
        cursor.close()

    def _label_shocks(self):
        """
        Identify all income shocks in the data.
        """
        cursor = self._NLSY_db.conn.cursor()

        cursor.execute("""ALTER TABLE {data}
            ADD COLUMN shock INTEGER DEFAULT -1""".format(
                data = self._wrangled_data_table
            )
        )

        for curr_year in range(self._cohort_year, 2018):
            future_year = curr_year + 2
            cursor.execute("""SELECT a.data_id, a.case_id, a.adjusted_income, b.adjusted_income
                FROM {data} AS a INNER JOIN {data} AS b ON a.case_id = b.case_id
                WHERE a.year = {curr_year} AND b.year = {future_year}""".format(
                    data = self._wrangled_data_table,
                    curr_year = curr_year,
                    future_year = future_year
                )
            )

            rows = cursor.fetchall()

            for index, row in enumerate(rows):
                (data_id, case_id, first_year_income, second_year_income) = row
                if first_year_income < 0 or second_year_income < 0:
                    pass
                elif second_year_income < .8 * first_year_income:
                    cursor.execute("UPDATE {data} SET shock = 1 WHERE data_id = {data_id}".format(
                        data = self._wrangled_data_table,
                        data_id = data_id
                        )
                    )
                else:
                    cursor.execute("UPDATE {data} SET shock = 0 WHERE data_id = {data_id}".format(
                        data = self._wrangled_data_table,
                        data_id = data_id
                        )
                    )

        self._NLSY_db.conn.commit()
        cursor.close()

    def data(self, include_nulls=False):
        conn = self._NLSY_db.conn
        cursor = conn.cursor()

        sql_query = """SELECT * FROM {respondents}
            INNER JOIN {data} ON {data}.case_id = {respondents}.case_id
            INNER JOIN {years} ON {data}.year = {years}.year
            INNER JOIN {region} ON {region}.region = {data}.region AND
                {region}.year = {data}.year""".format(
                respondents = self._wrangled_respondents_table,
                data = self._wrangled_data_table,
                years = self._NLSY_db.years_table,
                region = self._NLSY_db.region_table
                )

        df = pd.read_sql(sql_query, conn)

        # Get rid of the duplicate columns, as well as data_id, which is useful
        # only in the SQL database.
        df = df.loc[:,~df.columns.duplicated()]
        df.drop(['data_id'], axis=1, inplace=True)

        if not include_nulls:
            df.drop(df[df.shock < 0].index, inplace=True)

        return df
