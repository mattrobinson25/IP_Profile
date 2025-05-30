#!/nfs_share/matt_desktop/server_scripts/ip_profile/venv_311/bin/python3.11
from datetime import datetime as dt, timedelta as td
import sqlite3
import pandas as pd
from ip_profile_lib import ip_info, db_con, logger, my_token, sql_date, ApiConnectionErrors, db_cursor

# User defined vars
db_f2b = '/var/lib/fail2ban/fail2ban.sqlite3'  # DB used by Fail2ban (default)
con_f2b = sqlite3.connect(db_f2b)   # db created by f2b

logger.info(" FAIL2BAN ".center(40, "#"))

# Try to get table from database (if exists)
try:
    df = pd.read_sql('select * from f2b', db_con)
except pd.errors.DatabaseError as e:
    error = e.args[0]

    if 'no such table' in error.lower():  # No such table found. Creating now.
        logger.info('Creating table')
        db_cursor.execute('''
            CREATE TABLE f2b (
            ip TEXT,
            city TEXT,
            region TEXT,
            country TEXT,
            loc TEXT,
            org TEXT,
            postal TEXT,
            timezone TEXT,
            date TEXT,
            hostname TEXT,
            time TEXT,
            anycast TEXT
            )''')

        df = pd.read_sql('select * from f2b', db_con)  # Blank data frame from new table
    else:
        logger.info('Failed to create table.')
        logger.info(e)
        logger.info('Exiting.')
        exit()

# Dates and timestamps
today_timestamp = dt.strptime(dt.now().strftime('%x'), '%x').timestamp()  # Midnight timestamp
yesterday_timestamp = dt.strptime((dt.now() - td(days=1)).strftime('%x'),'%x').timestamp()  # Yesterday midnight timestamp


# Personal database with added ip information
df_my_database = pd.read_sql('select * from f2b', db_con)
# Database created by F2B
df_f2b = pd.read_sql('select * from bips', con_f2b)

# Find all F2B bans from yesterday. Use UTC timestamps
df_f2b_yesterday = df_f2b[
    (df_f2b.timeofban >= yesterday_timestamp) &
    (df_f2b.timeofban < today_timestamp)
]

# A list of tuples containing ip addresses and the time they were banned yesterday
bans = []
for ip, time in zip(df_f2b_yesterday.ip, df_f2b_yesterday.timeofban):
    time_formated = dt.fromtimestamp(time).strftime('%H:%M:%S')  # Convert timestamps to HH:MM:SS string format
    bans.append((ip, time_formated))

if len(bans) == 0:
    logger.info('No new ips banned.')
else:
    for ban in bans:
        try:
            ip, time = ban                 # IP that was banned and the time of the ban
            entry = ip_info(ip, my_token)  # Call the API func and get the data in a dict
            entry['date'] = sql_date       # Yesterday's date
            entry['time'] = time           # Time of ban
            df_entry = pd.DataFrame([entry])  # Convert to dataframe
            # Concatenate new data with old data using pandas.concat()
            df_my_database = pd.concat(objs=[df_my_database, df_entry], ignore_index=True)
        except ApiConnectionErrors as e:
            logger.info(e)

    # Write new data to database
    df_my_database.to_sql(
        name='f2b', 
        con=db_con,
        if_exists='replace', 
        index=False
    )
    logger.info(f'{len(bans)} new f2b bans.')

con_f2b.commit()
con_f2b.close()
