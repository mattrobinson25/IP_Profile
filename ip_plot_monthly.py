#!/nfs_share/matt_desktop/server_scripts/ip_profile/.venv/bin/python3.12
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from matplotlib import pyplot as plt
from os import remove, mkdir, system as run, chown
from zipfile import ZipFile

# Working directories.
working_dir: str = '/nfs_share/matt_desktop/server_scripts/ip_profile'
fig_dir: str = f'{working_dir}/figs/monthly'
db_file: str = f'{working_dir}/ip_profile.db'


def truncate(word: str) -> str:
    if len(word) > 13:
        word = f'{word[:11]}...'
    return word


# This script will run at midnight on the 1st of every month.
# 'date' will be defined as yesterday's month.
# Therefore, this script will plot last month's data.
sql_date: str = (datetime.now() - timedelta(weeks=1)).strftime('%Y-%m')
date: str = (datetime.now() - timedelta(weeks=1)).strftime('%b_%Y')
email_figs: bool = False
email_recipient: str = 'mattrobinson25@protonmail.com'

# Each vhost will have its own table.
tables: list[str] = ['matthewrobinsonmusic', 'f2b', 'ssh_user', 'nextcloud']

try:
    mkdir(fig_dir)
except FileExistsError:
    print('Figure save directory already exists.')


# The body of this script will run four times. Once for every table in the database.
for table in tables:
    with sqlite3.connect(db_file) as con:
        df: pd.DataFrame = pd.read_sql(f'select * from {table}', con)
        df: pd.DataFrame = df[df.date.str.startswith(sql_date)]

    match table:
        case 'matthewrobinsonmusic' | 'nextcloud':
            data: pd.Series = df.groupby(['country']).packets.sum().sort_values(ascending=False)[:5]
            countries: list[str] = list(data.index)
            country_frequencies: list[str] = list(data)

            data: pd.Series = df.groupby(['city']).packets.sum().sort_values(ascending=False)[:10]
            cities: list[str] = [truncate(city) for city in list(data.index)]
            city_frequencies: list[str] = list(data)

        case 'ssh_user':
            data: pd.Series = df.groupby(['country']).attempts.sum().sort_values(ascending=False)[:5]
            countries: list[str] = list(data.index)
            country_frequencies: list[str] = list(data)

            data: pd.Series = df.groupby(['city']).attempts.sum().sort_values(ascending=False)[:10]
            cities: list[str] = [truncate(city) for city in list(data.index)]
            city_frequencies: list[str] = list(data)

        case 'f2b':
            data: pd.Series = df.groupby(['country']).ip.count().sort_values(ascending=False)[:5]
            countries: list[str] = list(data.index)
            country_frequencies: list[str] = list(data)

            data: pd.Series = df.groupby(['city']).ip.count().sort_values(ascending=False)[:10]
            cities: list[str] = [truncate(city) for city in list(data.index)]
            city_frequencies: list[str] = list(data)
    
    plt.subplot(1, 2, 1)

    # Plot the top ten cities.
    plt.bar(cities, city_frequencies)
    plt.xticks(rotation=45, fontsize=7)
    plt.xlabel('City')
    plt.ylabel('Number of Visits')
    plt.title('Visitors by City (Top 10)')

    # Plot the top five countries.
    plt.subplot(1, 2, 2)
    plt.pie(country_frequencies, labels=countries, autopct='%.1f')
    plt.title('Visits by Country (Top 5)')
    plt.tight_layout()
    plt.savefig(f'{fig_dir}/{table}_{sql_date}.png', dpi=1200, pad_inches=1)
    plt.clf()
    print(f'Saved {table}')

if email_figs:
    body: str = f"Monthly statistics {date}"
    subject: str = f"Monthly Stats -- {date}"
    attachments: list[str] = [f'-A {table}_{sql_date}.png ' for table in tables]
    attachments_str: str = ''

    for attachment in attachments:
        attachments_str += attachment

    send_mail_cmd: int = run(f"echo {body} | mail -s {subject} {attachments_str} {email_recipient}")

    if send_mail_cmd == 0:
        print('Figures were mailed successfully')
    else:
        print('Figures could not be mailed')


# Create a file.zip containing figures from all three tables.
zip_file: str = f'{fig_dir}/tablefigs_{date}.zip'
with ZipFile(zip_file, 'w') as zf:
    for table in tables:
        file: str = f'{fig_dir}/{table}_{sql_date}.png'
        zf.write(file)
        remove(file)  # The file can be deleted now that it is saved in file.zip.

chown(zip_file, 1000, 1001)
