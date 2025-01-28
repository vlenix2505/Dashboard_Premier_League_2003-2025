#Libraries to use
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import pandas as pd
import pyodbc
import sys
import os

#Structure of the database
teams = []
statistics =[]
seasons = []
teams_seasons = []

load_dotenv()

db_server = os.getenv("DB_SERVER")
db_name = os.getenv("DB_NAME")

def connect_to_sql_server():
    try:
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};'
            f'SERVER={db_server};'
            'Trusted_Connection=yes;'
        )
        return conn
    except pyodbc.Error as e:
        print(f"Error connecting to SQL Server: {e}")
        raise

# Create database
def create_database(database_name, conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{database_name}'")
            result = cursor.fetchone()
            if not result:
                conn.autocommit = True
                cursor.execute(f"CREATE DATABASE {database_name}")
                conn.autocommit = False
                print(f"Database '{database_name}' created successfully.")
            else:
                print(f"Database '{database_name}' already exists.")
    except pyodbc.Error as e:
        print(f"Error while creating/checking database '{database_name}': {e}")
        raise

# Connect to database
def connect_to_database(database_name):
    try:
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};'
            f'SERVER={db_server};'
            f'Database={database_name};'
            'Trusted_Connection=yes;'
        )
        print(f"Connected to database '{database_name}' successfully.")
        return conn
    except pyodbc.Error as e:
        return e
 
# Create tables
def create_tables(conn):
    cursor = conn.cursor()

    try:
        # Create table Teams
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Teams')
        BEGIN
            CREATE TABLE [Teams] (
                [TeamID] INT PRIMARY KEY,
                [TeamName] NVARCHAR(255) NOT NULL
            );
        END
        """)

        # Create table Seasons
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Seasons')
        BEGIN
            CREATE TABLE [Seasons] (
                [SeasonID] INT PRIMARY KEY,
                [Season] NVARCHAR(50),
                [StartYear] INT,
                [EndYear] INT
            );
        END
        """)

        # Create table TeamsSeasons
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='TeamsSeasons')
        BEGIN
            CREATE TABLE [TeamsSeasons] (
                [TeamSeasonID] INT PRIMARY KEY,
                [TeamID] INT FOREIGN KEY REFERENCES [Teams]([TeamID]),
                [SeasonID] INT FOREIGN KEY REFERENCES [Seasons]([SeasonID]),
                [Position] INT,
                [Points] INT
            );
        END
        """)

        # Create table Statistics
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Statistics')
        BEGIN
            CREATE TABLE [Statistics] (
                [StatisticID] INT PRIMARY KEY,
                [TeamSeasonID] INT FOREIGN KEY REFERENCES [TeamsSeasons]([TeamSeasonID]),
                [GamesPlayed] INT,
                [Wins] INT,
                [Losses] INT,
                [Draws] INT,
                [GoalsFor] INT,
                [GoalsAgainst] INT,
                [GoalDifference] INT
            );
        END
        """)

        # Commit all changes
        conn.commit()
        print("Tables ensured.")
    except pyodbc.Error as e:
        print(f"An error occurred while creating the tables: {e}")
        conn.rollback()
    finally:
        cursor.close()

# Insert data into tables
def insert_into_sql(table_name, data, conn):
    cursor = conn.cursor()

    for _, row in data.iterrows():        
        placeholders = ", ".join(["?" for _ in row])
        columns = ", ".join([f"[{col}]" for col in data.columns])      
        sql = f"INSERT INTO [{table_name}] ({columns}) VALUES ({placeholders})"
        values = tuple(row)

        try:
            cursor.execute(sql, values)
        except pyodbc.IntegrityError as e:
            print(f"IntegrityError: {e}")
            continue
        except pyodbc.Error as e:
            print(f"Error inserting data into {table_name}: {e}")
            raise

    conn.commit() 
    print(f"Data inserted successfully into {table_name}")

# Update table TeamsSeasons
def update_teams_seasons(conn, team_season_id, position, points):
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM TeamsSeasons WHERE TeamSeasonID = ?", (team_season_id,))
    count = cursor.fetchone()[0]

    if count > 0: 
        sql_update = """
            UPDATE [TeamsSeasons]
            SET Position = ?, Points = ?
            WHERE TeamSeasonID = ?
        """
        values = (position, points, team_season_id)
        cursor.execute(sql_update, values)
        print(f"TeamSeasonID {team_season_id} updated with Position={position}, Points={points}")
    else:
        print(f"TeamSeasonID {team_season_id} not was founded.")

    conn.commit()

# Update table Statistics
def update_statistics(
    conn, 
    team_season_id, 
    games_played, 
    wins, 
    losses, 
    draws, 
    goals_for, 
    goals_against, 
    goal_difference
):
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM [Statistics] WHERE TeamSeasonID = ?", (team_season_id,))
    count = cursor.fetchone()[0]

    if count > 0:
        sql_update = """
            UPDATE [Statistics]
            SET GamesPlayed = ?, Wins = ?, Losses = ?, Draws = ?, 
                GoalsFor = ?, GoalsAgainst = ?, GoalDifference = ?
            WHERE TeamSeasonID = ?
        """
        values = (games_played, wins, losses, draws, goals_for, goals_against, goal_difference, team_season_id)
        cursor.execute(sql_update, values)
        print(f"TeamSeasonID {team_season_id} with new statistics.")
    else:
        print(f"Register for TeamSeasonID {team_season_id} not was founded.")
    
    conn.commit()

# Configure database
def setup_database(database_name):
    conn = connect_to_sql_server()    
    create_database(database_name, conn)  
    conn = connect_to_database(database_name) 
    create_tables(conn)  
    conn.close()
    print("Database and Tables created.")

# Execute inserting
def insert_database(table_teams, table_seasons, table_teams_seasons, table_statistics):
    
    insert_into_sql("Teams", table_teams, conn)
    insert_into_sql("Seasons", table_seasons, conn)
    insert_into_sql("TeamsSeasons", table_teams_seasons, conn)
    insert_into_sql("Statistics", table_statistics, conn)

    conn.close()
    print("Database setup completed.")

# WebDriver configuration
def setup_driver():
    chromeOptions = Options()

    #To avoid opening windows
    chromeOptions.add_argument("--headless")
    chromeOptions.add_argument("--disable-gpu")
    chromeOptions.add_argument("--no-sandbox")
    chromeOptions.add_argument("--disable-dev-shm-usage")

    driverPath = r"C:\Users\User\Documents\tools_Webscrapping\chromedriver-win64\chromedriver.exe"
    service = Service(driverPath)
    driver = webdriver.Chrome(service=service, options=chromeOptions)
    return driver

# Extract data from a table
def extract_data(table, headers_included=True):
    rows = table.find_elements(By.TAG_NAME, "tr")
    data = []
    for row in rows:
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) > 0:
            data.append([cell.text.strip().replace("\n"," ") for cell in cells])
    if headers_included:
        headers = [header.text for header in table.find_elements(By.TAG_NAME, "th")]
        return pd.DataFrame(data, columns=headers)
    return pd.DataFrame(data)

# Process Tables
def process_tables(driver, max_attempts=3):
    attempt = 1

    while attempt <= max_attempts:
        try:
            tables = driver.find_elements(By.CLASS_NAME, "Table")
            print(f"{len(tables)} tables were found.")

            if len(tables) < 2:
                raise ValueError("Not enough tables found on the page.")

            # First table (teams)
            team_table = tables[0]
            df_team = extract_data(team_table, headers_included=False)
            df_team.columns = ["Team"]
            df_team["Team"] = df_team["Team"].str.lstrip("0123456789").str.strip()
            print(df_team)

            # Second table (stats)
            stats_table = tables[1]
            df_stats = extract_data(stats_table)
            df_stats.rename(columns={
                'GP': 'Games Played',
                'W': 'Wins',
                'L': 'Losses',
                'D': 'Draws',
                'F': 'Goals For',
                'A': 'Goals Against',
                'GD': 'Goal Difference',
                'P': 'Points'
            }, inplace=True)
            print(df_stats)

            # Combine tables
            df_combined = pd.merge(df_team, df_stats, left_index=True, right_index=True)
            return df_combined

        except ValueError as ve:
            print(f"ValueError: {ve}. Retrying... (Attempt {attempt}/{max_attempts})")
            attempt += 1
            driver.refresh()
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "Table"))
            )
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    print(f"Failed to find the required tables after {max_attempts} attempts.")
    return None

# Validate data integrity
def validate_tables(df_combined, season_name, driver, max_attempts=3):
    if df_combined is None:
        print(f"No data to validate for season {season_name}. Check the scraping process.")
        return None

    attempt = 1

    while attempt <= max_attempts:
        valid_teams = True
        valid_statistics = True

        # Validate teams
        empty_teams = df_combined['Team'].isnull().sum()
        if empty_teams > 0:
            valid_teams = False
            print(f"Warning: {empty_teams} teams missing in season {season_name} (attempt {attempt}/{max_attempts}). Reloading data...")

        # Validate statistics
        empty_statistics = df_combined.iloc[:, 2:].isnull().sum().sum()
        if empty_statistics > 0:
            valid_statistics = False
            print(f"Warning: {empty_statistics} statistic values missing in season {season_name} (attempt {attempt}/{max_attempts}). Reloading data...")

        # If there are issues, reload and reprocess
        if not valid_teams or not valid_statistics:
            driver.refresh()
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "Table"))
            )

            # Reprocess tables and update df_combined
            df_combined = process_tables(driver)

            if df_combined is None:
                print(f"Failed to reload data for season {season_name}.")
                break

            # Revalidate data
            empty_teams = df_combined['Team'].isnull().sum()
            empty_statistics = df_combined.iloc[:, 2:].isnull().sum().sum()

            if empty_teams == 0 and empty_statistics == 0:
                print(f"Data loaded successfully for season {season_name}.")
                break
        else:
            print(f"All data loaded successfully for season {season_name}.")
            break

        attempt += 1

    if empty_teams > 0 or empty_statistics > 0:
        print(f"Warning: Missing data still exists for season {season_name} after {max_attempts} attempts.")

    return df_combined

# Verify if the year is in the database
def verify_start_year(conn, start_year):
    try:
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM Seasons WHERE StartYear = ?"
        cursor.execute(query, (start_year,))
        result = cursor.fetchone()
        if result[0] > 0:
            print(f"StartYear {start_year} existe en la tabla Seasons.")
            return True
        else:
            print(f"StartYear {start_year} no existe en la tabla Seasons.")
            return False
    except pyodbc.Error as e:
        print(f"Error in query execution: {e}")
        return False

# Verify if the team alredy is in the database
def get_existing_team_ids(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT TeamID, TeamName FROM Teams")
    result = cursor.fetchall()
    existing_team_ids = {team_name: team_id for team_id, team_name in result}
    return existing_team_ids

# Verify if the team has a relation with the season
def get_existing_team_season_ids(conn, team_id, season_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TeamSeasonID, TeamID FROM TeamsSeasons 
        WHERE SeasonID = ? AND TeamID = ?
    """, (season_id, team_id))
    result = cursor.fetchall()
    existing_team_season_ids = {team_id: team_season_id for team_season_id, team_id in result}
    return existing_team_season_ids

# Verify if the teamseason has a register in statistics table
def get_existing_statistic_ids(conn, team_season_ids):
    if not team_season_ids:
        return {}
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT StatisticID, TeamSeasonID 
        FROM [Statistics] 
        WHERE TeamSeasonID IN ({})
    """.format(','.join(['?'] * len(team_season_ids))), tuple(team_season_ids))

    result = {row.TeamSeasonID: row.StatisticID for row in cursor.fetchall()}
    return result


#Main

# Set variables
start_year = 2003
current_year = datetime.now().year
database_name = db_name
seasons = []
seasons_columns = ['SeasonID','Season', 'StartYear', 'EndYear']
verify_current_season = 0

# Call connection functions
setup_database(database_name)
conn = connect_to_database(database_name)
existing_team_ids = get_existing_team_ids(conn)

#Generate seasons automatically
for year in range(start_year, current_year):
    if year not in seasons:
        next_year=str(year+1)[-2:]
        season_name=f"{year}-{next_year}"
        season_id = year - start_year +1 
        season_list =[season_id, season_name, year, year +1]
        seasons.append(season_list)

table_seasons = pd.DataFrame(seasons, columns=seasons_columns)
print(table_seasons)

# Iterate each season
for index, row_season in table_seasons.iterrows():
    year = row_season['StartYear']    
    flag_year = 0

    flag_year = verify_start_year(conn, year)
    if(flag_year == 1 and year == 2024):
        verify_current_season = 1
    
    if(flag_year == 0 or verify_current_season == 1):
        url = f"https://www.espn.com/soccer/standings/_/league/ENG.1/season/{year}"
        driver =setup_driver()
        try:
            driver.get(url)
            df_combined = process_tables(driver)
            if df_combined is not None:
                df_combined = validate_tables(df_combined, row_season["Season"], driver)  
            else:
                print("No data to process. Exiting script.")
                sys.exit()
        finally:
            driver.quit()

        df_combined["Position"] = df_combined.index+1
        cols = ["Position"] + [col for col in df_combined.columns if col != "Position"]
        df_combined = df_combined[cols]
        
        # Iterate df_combined rows
        for index, row_combined in df_combined.iterrows():
            team_name = row_combined["Team"]

            if team_name in existing_team_ids:
                team_id = existing_team_ids[team_name]
            else:   
                team_id = len(existing_team_ids) + 1
                existing_team_ids[team_name] = team_id
                teams.append({"TeamID": team_id, "TeamName": team_name})
                #print(teams)
                #print(existing_team_ids)

            existing_team_season_ids = get_existing_team_season_ids(conn, team_id, row_season["SeasonID"])
            if team_id in existing_team_season_ids:
                team_season_id = existing_team_season_ids[team_id]
                update_teams_seasons(
                    conn,
                    team_season_id=team_season_id,
                    position=row_combined["Position"],
                    points=row_combined["Points"],
                )
            else:
                team_season_id = len(teams_seasons) + 1
                existing_team_season_ids[team_id] = team_season_id
                teams_seasons.append({
                "TeamSeasonID": team_season_id,
                "TeamID": team_id,
                "SeasonID": row_season["SeasonID"],
                "Position": row_combined["Position"],
                "Points": row_combined["Points"]
            })       

            existing_statistic_ids = get_existing_statistic_ids(conn, list(existing_team_season_ids.values()))
            
            if team_season_id in existing_statistic_ids:
                statistic_id = existing_statistic_ids[team_season_id]                
                update_statistics(
                    conn=conn,
                    team_season_id=team_season_id,
                    games_played=row_combined["Games Played"],
                    wins=row_combined["Wins"],
                    losses=row_combined["Losses"],
                    draws=row_combined["Draws"],
                    goals_for=row_combined["Goals For"],
                    goals_against=row_combined["Goals Against"],
                    goal_difference=row_combined["Goal Difference"]
                )
            else:
                statistic_id = len(statistics) + 1
                existing_statistic_ids[team_season_id] = statistic_id 
                statistics.append({
                    "StatisticID": statistic_id,
                    "TeamSeasonID": team_season_id,
                    "GamesPlayed": row_combined["Games Played"],
                    "Wins": row_combined["Wins"],
                    "Losses": row_combined["Losses"],
                    "Draws": row_combined["Draws"],
                    "GoalsFor": row_combined["Goals For"],
                    "GoalsAgainst": row_combined["Goals Against"],
                    "GoalDifference": row_combined["Goal Difference"]
                })

    
if(verify_current_season == 1):
    print(f"Data of year {current_year-1} updated successfully.")
else:    
    table_teams = pd.DataFrame(teams)    
    table_teams_seasons = pd.DataFrame(teams_seasons)    
    table_statistics = pd.DataFrame(statistics)

    insert_database(table_teams, table_seasons, table_teams_seasons, table_statistics)
    #print(table_seasons.head())
    #print(table_teams.head())
    #print(table_teams_seasons.head())
    #print(table_statistics.head())
    print("Data loaded succesfully.")








