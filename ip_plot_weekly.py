#!/nfs_share/matt_desktop/server_scripts/ip_profile/.venv/bin/python3.12
import pandas as pd
import sqlite3
from datetime import datetime as dt, timedelta as td
from matplotlib import pyplot as plt
from os import system as run, remove, chown


def trunc_str(string: str) -> str:
    if len(string) > 13:
        return f'{string[:11]}...'
    else:
        return string


working_dir = '/nfs_share/matt_desktop/server_scripts/ip_profile'
save_dir = f'{working_dir}/figs'
db_file = f'{working_dir}/ip_profile.db'
email_figs = False
send_to_email = 'mattrobinson25@protonmail.com'

last_week = [
    ((dt.now() - td(weeks=1)) + td(days=x)).strftime('%Y-%m-%d')
    for x in range(0, 7)
]

tables = ['nextcloud', 'matthewrobinsonmusic', 'f2b', 'ssh_user']
date = dt.now().strftime('%Y-%m-%d')

for table in tables:
    with sqlite3.connect(db_file) as con:
        df = pd.read_sql(f'select * from {table}', con)
        df_week = df[df.date.isin(last_week)]

    match table:
        case 'matthewrobinsonmusic' | 'nextcloud':
            data = df_week.groupby(['country']).packets.sum().sort_values(ascending=False)[:5]
            countries = list(data.index)
            country_frequencies = list(data)

            data = df_week.groupby(['city']).packets.sum().sort_values(ascending=False)[:10]
            cities = [trunc_str(city) for city in list(data.index)]
            city_frequencies = list(data)

        case 'ssh_user':
            data = df_week.groupby(['country']).attempts.sum().sort_values(ascending=False)[:5]
            countries = list(data.index)
            country_frequencies = list(data)

            data = df_week.groupby(['city']).attempts.sum().sort_values(ascending=False)[:10]
            cities = [trunc_str(city) for city in list(data.index)]
            city_frequencies = list(data)

        case 'f2b':
            data = df_week.groupby(['country']).ip.count().sort_values(ascending=False)[:5]
            countries = list(data.index)
            country_frequencies = list(data)

            data = df_week.groupby(['city']).ip.count().sort_values(ascending=False)[:10]
            cities = [trunc_str(city) for city in list(data.index)]
            city_frequencies = list(data)


    plt.subplot(1, 2, 1)

    # Plot the top ten cities.
    plt.bar(cities, city_frequencies)
    plt.xticks(rotation=45, fontsize=7)
    plt.xlabel('City')
    plt.ylabel('Number of Visits')
    plt.title(f'Visitors by City (Top 10)\n {date}')

    # Plot the top five countries.
    plt.subplot(1, 2, 2) 
    plt.pie(country_frequencies, labels=countries, autopct='%.1f')
    plt.title(f'Visits by Country (Top 5)\n {date}')
    plt.tight_layout()
    figure_file = f'{save_dir}/{table}_weekly.png'
    plt.savefig(figure_file, dpi=800, pad_inches=1)
    plt.clf()

    chown(figure_file, uid=1000, gid=1001)
    print(f'Saved {table}')

if email_figs:
    body = f"Weekly Statistics {date}"
    subject = f"Weekly Stats -- {date}"
    attachments = [f' -A {table}_{date}.png' for table in tables]
    attachments_str = ''

    for attachment in attachments:
        attachments_str += attachment

    cmd = run(f"echo {body} | mail -s {subject} {attachments_str} {send_to_email}")

    if cmd == 0:
        print('Figures were mailed successfully')
    else:
        print('Figures could not be mailed')
