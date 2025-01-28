# Dashboard Premier League - Data Analysis & Automation
![image](https://github.com/user-attachments/assets/32f00521-02ad-45c9-8487-d796f9016b86)

![image](https://github.com/user-attachments/assets/df7fb9fe-1beb-44b5-a21c-b85b97aa13bb)


## Project Background
This project focuses on analyzing and visualizing Premier League statistics using Power BI. The data is collected through web scraping from ESPN and stored in a SQL Server database. The system is designed to automatically update the data on a weekly basis and refresh the Power BI reports accordingly. The dataset includes Premier League seasons from 2003-22 to the present (2024-25).

You can access the dashboard by [this link](https://app.powerbi.com/view?r=eyJrIjoiYzg5Njk1NWItMDM4Mi00OWZmLThhOTctZGRlNzg3MGJhMzZjIiwidCI6IjBlMGNiMDYwLTA5YWQtNDlmNS1hMDA1LTY4YjliNDlhYTFmNiIsImMiOjR9&pageName=e18b7136c414b00e0575)  

## Data Structure
The SQL Server database **premierLeague** consists of the following key tables:

- **Teams**: Stores information about each team.
- **Seasons**: Contains season details.
- **TeamsSeasons**: Links teams with specific seasons and their performance metrics.
- **Statistics**: Stores game statistics, including wins, losses, draws, goals scored, and goal difference.

The Python script is responsible for creating the database and tables, inserting the data, and updating only the current season's data.

![image](https://github.com/user-attachments/assets/f7d015aa-dffe-4974-90c1-fff36e820a8f)

By the other hand, in Power BI was created the table Measures to save the functions related to especific metrics or KPI's.

![image](https://github.com/user-attachments/assets/09645588-7eb3-4e9a-8699-8654c9ff542e)

## Executive Summary
- **Data Collection**: A Python script scrapes Premier League data from ESPN using Selenium and processes it with Pandas.
- **Database Management**: SQL Server is used for structured data storage, ensuring efficient querying and updating of historical and current season data.
- **Power BI Reports**: Two main dashboards were created:
  - **General View**: Provides league-wide statistics filtered by one, multiple, or all seasons. Metrics include:
    - Total games, total goals, top 5 teams by goal difference
    - Wins, losses, and draws distribution by team
    - Goals distribution by year
    - Season standings table
    - Top teams by wins, losses, and average goals per game
  - **Teams View**: Focuses on individual team statistics filtered by season(s). Metrics include:
    - Goals for vs. goals against per season
    - Position evolution by year
    - % distribution of wins, losses, and draws
    - Top final points per season
    - Average position, wins, losses, draws, goal difference, and games played
- **Automation**:
  - **Python Execution**: Windows Task Scheduler runs the script every Monday to ensure weekend matches are fully updated.
    
    ![image](https://github.com/user-attachments/assets/8d5cce0b-d710-4c03-a444-f73d00299bce)

  - **Power BI Refresh**: A Power BI Gateway is configured with SQL Server Authentication to refresh the dataset 30 minutes after the Python script execution.

## Conclusion
This project successfully automates the data pipeline for Premier League statistics, integrating web scraping, database storage, and Power BI reporting. The system enhances efficiency, providing up-to-date insights for analysis and decision-making.

## Additional Sections
### Technologies Used
- **Python**: Selenium for web scraping, Pandas for data processing, PyODBC for SQL Server interaction
- **SQL Server**: Data storage and management
- **Power BI**: Data visualization and reporting
- **Windows Task Scheduler**: Automates the data update process

### How to Set Up
1. Clone the repository.
2. Configure SQL Server with the required database structure.
3. Set up the Python environment and install dependencies.
4. Schedule the Python script to run weekly.
5. Connect Power BI to the SQL database and configure the gateway for automatic refresh.

