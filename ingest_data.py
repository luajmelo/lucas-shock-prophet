import os
import sqlite3
import csv
import pdb


def create_tables(conn):

    """
    Creates the necessary tables in the SQLite database
    """
    cursor = conn.cursor()

    # Create the database tables.

    # The questions table contains the NLSY-provided unique question name, as well as human-readable description of each question.
    cursor.execute("""CREATE TABLE questions (
        question_name TEXT PRIMARY KEY,
        description TEXT
    )""")

    # The RNUMs tables associate NLSY-provided unique RNUMs with a specific question.
    cursor.execute("""CREATE TABLE rnums_79 (
        rnum TEXT PRIMARY KEY,
        question_name TEXT NOT NULL,
        year INTEGER NOT NULL
    )""")

    cursor.execute("""CREATE TABLE rnums_97 (
        rnum TEXT PRIMARY KEY,
        question_name TEXT NOT NULL,
        year INTEGER NOT NULL
    )""")

    # The responses_79 table contains all of the individual responses to each RNUM for the 1979 cohort.
    cursor.execute("""CREATE TABLE responses_79 (
        response_id INTEGER PRIMARY KEY AUTOINCREMENT,
        rnum TEXT NOT NULL,
        case_id INTEGER NOT NULL,
        response INTEGER NOT NULL
    )""")

    # The responses_97 table contains all of the individual responses to each RNUM for the 1997 cohort.
    cursor.execute("""CREATE TABLE responses_97 (
        response_id INTEGER PRIMARY KEY AUTOINCREMENT,
        rnum TEXT NOT NULL,
        case_id INTEGER NOT NULL,
        response INTEGER NOT NULL
    )""")

    # The years table contains economic and other data associated with a specific year.
    cursor.execute("""CREATE TABLE years (
        year INTEGER PRIMARY KEY,
        unemployment REAL NOT NULL,
        gdp_growth REAL NOT NULL,
        inflation REAL NOT NULL
    )""")

    # commit changes to the database
    conn.commit()
    cursor.close()


def connect(path="data.db"):
    """
    Creates the database. This is meant to initialize the data,
    so it deletes any existing database found.
    """

    if os.path.exists(path):
        overwrite = input("Continuing will delete your existing NLSY database. Continue (y/n)? ")
        if overwrite == "y":
            os.remove(path)
        else:
            print("Exiting...")
            exit()

    conn = sqlite3.connect(path)
    create_tables(conn)

    return conn


def insert_year_data(conn, year_path):
        """
        Saves all the year-specific data to the "years" table.
        """
        cursor = conn.cursor()

        with open(year_path) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for row in csv_reader:
                cursor.execute("""INSERT INTO
                    years (year, unemployment, gdp_growth, inflation)
                    VALUES (?, ?, ?, ?)
                    """, (row["year"], row["unemployment"], row["gdp_growth"], row["inflation"]))
        conn.commit()
        cursor.close()

def insert_NLSY_data(conn, rnum_path, qname_path, responses_path, rnums_table, questions_table, responses_table):
    """
    Saves all the individual responses to the appropriate "rnums," "questions," and "responses" tables.
    """

    cursor = conn.cursor()

    # Record each question to a dictionary as it's saved to the database to prevent entering duplicate questions.
    questions = dict()

    # The RNUMs and associated question names are stored on equivalent line numbers in two different files.
    with open(rnum_path, 'r') as rnum_file:
        with open(qname_path, 'r') as qname_file:
            for rnum in rnum_file:
                rnum = rnum.strip()
                qname_content = qname_file.readline().strip()
                (question_name, year) = qname_content.split(",")
                cursor.execute("""INSERT INTO
                            {} (rnum, question_name, year)
                            VALUES (?, ?, ?)
                            """.format(rnums_table), (rnum, question_name, year))

                if not question_name in questions:
                    cursor.execute("""INSERT INTO
                                {} (question_name)
                                VALUES (?)
                                """.format(questions_table), (question_name, ))
                    questions[question_name] = True

    with open(responses_path) as csv_file:

        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for response_num, row in enumerate(csv_reader):

            # R0000100 is a special value indicating the respondent's case ID.
            case_id = row["R0000100"]
            del row["R0000100"]

            # Now, save all the variable response to the "responses" table.
            for rnum, response in row.items():
                cursor.execute("""INSERT INTO
                    {} (rnum, case_id, response)
                    VALUES (?, ?, ?)
                    """.format(responses_table), (rnum, case_id, response))

    conn.commit()
    cursor.close()

if __name__ == '__main__':
    print("Creating database tables...")
    conn = connect()

    # Insert NLSY79 cohort data.
    rnum_path = os.path.join('data', 'dataset_rnum.NLSY79')
    qname_path = os.path.join('data', 'dataset_qname_with_year.NLSY79')
    responses_path = os.path.join('data', 'NLSY79.csv')
    rnums_table = 'rnums_79'
    questions_table = 'questions'
    responses_table = 'responses_79'
    print("Ingesting 1979 cohort data...")
    insert_NLSY_data(conn, rnum_path, qname_path, responses_path, rnums_table, questions_table, responses_table)

    # Insert NLSY97 cohort data.
    rnum_path = os.path.join('data', 'dataset_rnum.NLSY97')
    qname_path = os.path.join('data', 'dataset_qname_with_year.NLSY97')
    responses_path = os.path.join('data', 'NLSY97.csv')
    rnums_table = 'rnums_97'
    questions_table = 'questions'
    responses_table = 'responses_97'
    print("Ingesting 1997 cohort data...")
    insert_NLSY_data(conn, rnum_path, qname_path, responses_path, rnums_table, questions_table, responses_table)

    # Insert year data.
    year_path = os.path.join('data', 'years.csv')
    print("Ingesting years data...")
    insert_year_data(conn, year_path)

    print("Done!")
