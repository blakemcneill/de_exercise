import requests
import os
import duckdb

DATASET_URI = "https://data.wprdc.org/datastore/dump/4ab1e23f-3262-4bd3-adbf-f72f0119108b"
DATASET_DIRECTORY = "./air_quality_data"
DATASET_OUTPUT_NAME = "daily_air_quality"
OUTPUT_FORMAT = "csv"
RAW_DATASET_PATH = f"{DATASET_DIRECTORY}/{DATASET_OUTPUT_NAME}.{OUTPUT_FORMAT}"
DUCK_DB_NAME = "air_quality.duckdb"
DUCK_DB_PATH = f"{DATASET_DIRECTORY}/{DUCK_DB_NAME}"

#region Injestion functions (Bronze Layer)

def injest_data():
    '''
    Downloads the raw dataset and saves it to disk.
    '''
    print("Injesting data...")

    check_root_directory()

    response = request_data(OUTPUT_FORMAT)

    write_data_to_disk(RAW_DATASET_PATH, response.content)

    with duckdb.connect(DUCK_DB_PATH) as conn:
        # Create the table if it doesn't exist
        conn.sql("""
        CREATE TABLE IF NOT EXISTS raw_data(
            id INTEGER,
            date DATE,
            site VARCHAR,
            parameter VARCHAR,
            index_value FLOAT,
            description VARCHAR,
            health_advisory VARCHAR,
            health_effects VARCHAR
        );
        """)

        # Truncate the table
        conn.sql("""
        TRUNCATE TABLE raw_data;
        """)

        conn.read_csv(
        RAW_DATASET_PATH,
        delimiter=',',
        quotechar='"',
        escapechar='"',
        strict_mode=False,
        header=True).insert_into("raw_data")

    print("Data injestion complete.")


def write_data_to_disk(output_path: str, output_content: any):
    '''
    Write the raw data to disk.
    '''
    with open(output_path, "wb") as file:
            file.write(output_content)


def request_data(format: str):
    '''
    Runs the data request and handles issues.
    '''
    VALID_FORMATS = {"csv", "json"}
    if format not in VALID_FORMATS:
        raise ValueError(f"The requested format '{format}' must be one of the following: {VALID_FORMATS}.")

    query_parameters = {"downloadformat": format}
    response = requests.get(url=DATASET_URI, params=query_parameters)

    if response.status_code != 200:
        raise ValueError(f"Response code was {response.status_code}. It should be 200. Check the URL.")

    return response


def check_root_directory():
    ''' 
    Checks to make sure the root directory exists. If not, create it.
    '''

    os.makedirs(name=DATASET_DIRECTORY,exist_ok=True)

#endregion

#region Silver layer

def build_silver_layer():
    rel = duckdb.read_csv(
        RAW_DATASET_PATH,
        delimiter=',',
        quotechar='"',
        escapechar='"',
        strict_mode=False,
        header=True)

    # Let's run some tests
    tests_orchestration(rel=rel)
    # print(rel.describe())

    # print(rel.filter("index_value < 0.0"))

    # Let's build some dimensions
    build_date_dim()
    build_site_dim()
    build_parameter_dim()
    build_description_dim()
    build_health_advisory_dim()
    build_health_effects_dim()

    # And now the facts table
    build_daily_air_quality_fact()


def build_date_dim():
    '''
    Create and populate the date dim.
    '''
    with duckdb.connect(DUCK_DB_PATH) as conn:
        # Create the sequence if it doesn't exist
        conn.sql("""
        CREATE SEQUENCE IF NOT EXISTS dim_date_sequence START 1;
        """)
        # Create the table if it doesn't exist
        conn.sql("""
        CREATE TABLE IF NOT EXISTS dim_date(
            id INTEGER PRIMARY KEY DEFAULT nextval('dim_date_sequence'),
            date DATE NOT NULL UNIQUE
        );
        """)

        conn.sql("""
        INSERT INTO dim_date (date)
        SELECT DISTINCT date FROM raw_data
        ON CONFLICT (date) DO NOTHING;
        """)


def build_site_dim():
    '''
    Create and populate the site dim.
    '''
    with duckdb.connect(DUCK_DB_PATH) as conn:
        # Create the sequence if it doesn't exist
        conn.sql("""
        CREATE SEQUENCE IF NOT EXISTS dim_site_sequence START 1;
        """)
        # Create the table if it doesn't exist
        conn.sql("""
        CREATE TABLE IF NOT EXISTS dim_site(
            id INTEGER PRIMARY KEY DEFAULT nextval('dim_site_sequence'),
            site VARCHAR NOT NULL UNIQUE
        );
        """)

        conn.sql("""
        INSERT INTO dim_site (site)
        SELECT DISTINCT site FROM raw_data
        ON CONFLICT (site) DO NOTHING;
        """)


def build_parameter_dim():
    '''
    Create and populate the parameter dim.
    '''
    with duckdb.connect(DUCK_DB_PATH) as conn:
        # Create the sequence if it doesn't exist
        conn.sql("""
        CREATE SEQUENCE IF NOT EXISTS dim_parameter_sequence START 1;
        """)
        # Create the table if it doesn't exist
        conn.sql("""
        CREATE TABLE IF NOT EXISTS dim_parameter(
            id INTEGER PRIMARY KEY DEFAULT nextval('dim_parameter_sequence'),
            parameter VARCHAR NOT NULL UNIQUE
        );
        """)

        conn.sql("""
        INSERT INTO dim_parameter (parameter)
        SELECT DISTINCT parameter FROM raw_data
        ON CONFLICT (parameter) DO NOTHING;
        """)

def build_description_dim():
    '''
    Create and populate the description dim.
    '''
    with duckdb.connect(DUCK_DB_PATH) as conn:
        # Create the sequence if it doesn't exist
        conn.sql("""
        CREATE SEQUENCE IF NOT EXISTS dim_description_sequence START 1;
        """)
        # Create the table if it doesn't exist
        conn.sql("""
        CREATE TABLE IF NOT EXISTS dim_description(
            id INTEGER PRIMARY KEY DEFAULT nextval('dim_description_sequence'),
            description VARCHAR NOT NULL UNIQUE
        );
        """)

        conn.sql("""
        INSERT INTO dim_description (description)
        SELECT DISTINCT description FROM raw_data
        ON CONFLICT (description) DO NOTHING;
        """)

def build_health_advisory_dim():
    '''
    Create and populate the health_advisory dim.
    '''
    with duckdb.connect(DUCK_DB_PATH) as conn:
        # Create the sequence if it doesn't exist
        conn.sql("""
        CREATE SEQUENCE IF NOT EXISTS dim_health_advisory_sequence START 1;
        """)
        # Create the table if it doesn't exist
        conn.sql("""
        CREATE TABLE IF NOT EXISTS dim_health_advisory(
            id INTEGER PRIMARY KEY DEFAULT nextval('dim_health_advisory_sequence'),
            health_advisory VARCHAR NOT NULL UNIQUE
        );
        """)

        conn.sql("""
        INSERT INTO dim_health_advisory (health_advisory)
        SELECT DISTINCT health_advisory
        FROM raw_data
        WHERE health_advisory IS NOT NULL
        ON CONFLICT (health_advisory) DO NOTHING;
        """)

def build_health_effects_dim():
    '''
    Create and populate the health_effects dim.
    '''
    with duckdb.connect(DUCK_DB_PATH) as conn:
        # Create the sequence if it doesn't exist
        conn.sql("""
        CREATE SEQUENCE IF NOT EXISTS dim_health_effects_sequence START 1;
        """)
        # Create the table if it doesn't exist
        conn.sql("""
        CREATE TABLE IF NOT EXISTS dim_health_effects(
            id INTEGER PRIMARY KEY DEFAULT nextval('dim_health_effects_sequence'),
            health_effects VARCHAR NOT NULL UNIQUE
        );
        """)

        conn.sql("""
        INSERT INTO dim_health_effects (health_effects)
        SELECT DISTINCT health_effects 
        FROM raw_data
        WHERE health_effects IS NOT NULL
        ON CONFLICT (health_effects) DO NOTHING;
        """)

def build_daily_air_quality_fact():
    '''
    Create and populate the daily_air_quality facts table
    '''
    with duckdb.connect(DUCK_DB_PATH) as conn:
        # Create the table if it doesn't exist
        conn.sql("""
        CREATE TABLE IF NOT EXISTS fact_daily_air_quality (
            date_id INT NOT NULL REFERENCES dim_date(id),
            site_id INT NOT NULL REFERENCES dim_site(id),
            parameter_id INT NOT NULL REFERENCES dim_parameter(id),
            description_id INT REFERENCES dim_description(id),
            health_advisory_id INT REFERENCES dim_health_advisory(id),
            health_effects_id INT REFERENCES dim_health_effects(id),
            index_value FLOAT,
            PRIMARY KEY (date_id, site_id, parameter_id)
        );
        """)

        # Insert the facts data
        conn.sql("""
        INSERT INTO fact_daily_air_quality (
            date_id,
            site_id,
            parameter_id,
            description_id,
            health_advisory_id,
            health_effects_id,
            index_value
        ) SELECT
            dd.id,
            ds.id,
            dp.id,
            ddes.id,
            dha.id,
            dhe.id,
            rd.index_value
        FROM raw_data rd
        JOIN dim_date dd
            ON rd.date = dd.date
        JOIN dim_site ds
            ON rd.site = ds.site
        JOIN dim_parameter dp
            ON rd.parameter = dp.parameter
        JOIN dim_description ddes
            ON rd.description = ddes.description
        LEFT JOIN dim_health_advisory dha
            ON rd.health_advisory = dha.health_advisory
        LEFT JOIN dim_health_effects dhe
            ON rd.health_effects = dhe.health_effects
        ON CONFLICT (date_id, site_id, parameter_id) DO NOTHING;
        """)

#endregion

#region Silver layer tests

def tests_orchestration(rel: any):
    '''
    Orchestration function to run tests again the raw data relation.
    '''
    print("Starting tests...")

    raw_row_count(rel=rel)

    no_negative_index_value(rel=rel)

    print("Tests complete.")

def raw_row_count(rel: any):
    '''
    Make sure there are more than 0 records.
    If there are no records, we're going to assume we got bad data or ran a bad query.
    '''
    if len(rel) == 0:
        raise ValueError("There are 0 records from the raw data source. Check to make sure injestion didn't fail.")

def no_negative_index_value(rel):
    '''
    Make sure index_value does not contain a value less than 0
    '''
    if len(rel.filter("index_value < 0")) > 0:
        raise ValueError("There is a record with an index_value of less than 0.")

#endregion


#region Gold layer

def create_gold_layer():
    with duckdb.connect(DUCK_DB_PATH) as conn:
        # Create the table
        conn.sql("""
        CREATE TABLE IF NOT EXISTS polution_episodes (
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            num_days INT NOT NULL,
            max_peak FLOAT NOT NULL,
            peak_parameter VARCHAR NOT NULL,
            max_sites INT NOT NULL,
            PRIMARY KEY (start_date, end_date)
        );
        """)

        # Insert the data
        conn.sql("""
        WITH county_day AS (
            SELECT
                dd.date,
                max(fdaq.index_value) AS peak_value,
                arg_max(dp.parameter, fdaq.index_value) AS peak_parameter,
                count(DISTINCT site_id) FILTER (WHERE index_value > 100) AS sites_over
            FROM fact_daily_air_quality fdaq
            INNER JOIN dim_date dd
                ON fdaq.date_id = dd.id
            INNER JOIN dim_parameter dp
                ON fdaq.parameter_id = dp.id
            GROUP BY dd.date
            HAVING sites_over > 2
        ),
        grouped AS (
            SELECT
                date,
                peak_value,
                peak_parameter,
                sites_over,
                date - (row_number() OVER (ORDER BY date))::INT * INTERVAL 1 DAY AS grp
            FROM county_day
        )
        INSERT INTO polution_episodes (
            start_date,
            end_date,
            num_days,
            max_peak,
            peak_parameter,
            max_sites
        )
        SELECT
            min(date) AS start_date,
            max(date) AS end_date,
            count(1) AS num_days,
            max(peak_value) AS max_peak,
            arg_max(peak_parameter, peak_value) AS peak_parameter,
            max(sites_over) AS max_sites
        FROM grouped
        GROUP BY grp
        ON CONFLICT (start_date, end_date) DO NOTHING;
        """)

        print_polution_episodes()

def print_polution_episodes():
    with duckdb.connect(DUCK_DB_PATH) as conn:
        conn.table("polution_episodes").show()

#endregion


# MAIN FUNCTION
def main():
    injest_data()

    build_silver_layer()

    create_gold_layer()


if __name__ == "__main__":
    main()