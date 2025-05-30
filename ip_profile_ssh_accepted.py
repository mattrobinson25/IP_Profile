#!/nfs_share/matt_desktop/server_scripts/ip_profile/venv_311/bin/python3.11
import pandas as pd
from sqlite3 import OperationalError
from sys import argv
from os import listdir
from ip_profile_lib import (
    ip_info, log_reader, ApiConnectionErrors, db_con, LAN_prefix, LAN_region, sql_date, logger, db_cursor,
    LAN_city, LAN_country, LAN_timezone, LAN_postal, my_token, ssh_log_date, handle_failed_requests
)

# Defaults
log_files = [
    f'/var/log/{file}'
    for file in listdir('/var/log/')
    if file.startswith('auth.log')
]
failed_requests = []
script_name = argv[0]
logger.info(' SSH ACCEPTED '.center(40, "#"))


try:
    df_accepted = pd.read_sql('select * from accepted_ssh', db_con)
except pd.errors.DatabaseError as e:
    error = (e.args[0]).lower()

    if 'no such table' in error:
        logger.warning('Creating database')
        db_cursor.execute('''CREATE TABLE accepted_ssh (
                        ip TEXT,
                        user TEXT,
                        time TEXT,
                        date TEXT
                        )''')
        df_accepted = pd.read_sql('select * from accepted_ssh', db_con)
    else:
        logger.error(e)
        logger.error('Failed to create table. Exiting')
        exit()


for log_file in log_files:
    for line in log_reader(log_file):
        if (
                ('Accepted publickey' in line or 'Accepted password' in line)
                and ssh_log_date in line
        ):
            time: str = line.split()[2]
            user: str = line.split()[8]
            ip: str = line.split()[10]
            entry: dict = {'ip': ip, 'user': user, 'time': time, 'date': sql_date}

            if ip.startswith(LAN_prefix):
                entry['on_lan'] : bool = True
                entry['city'] : str = LAN_city
                entry['country'] : str = LAN_country
                entry['postal'] : str = LAN_postal
                entry['region'] : str = LAN_region
                entry['timezone'] : str = LAN_timezone
            else:
                entry['on_lan'] = False

                try:
                    api_info = ip_info(ip, my_token)
                    entry.update(api_info)
                except ApiConnectionErrors as e:
                    logger.error(e)
                    logger.error(line)
                    failed_request = {
                            'ip': ip, 'user': user, 
                            'time': time, 'date': sql_date, 
                            'line': line, 'script': 'ssh_accepted'
                            }
                    failed_requests.append(failed_request)
                    continue

            df_entry = pd.DataFrame([entry])
            df_accepted = pd.concat(objs=[df_accepted, df_entry], ignore_index=True)


df_accepted.to_sql(
    name='accepted_ssh',
    con=db_con,
    if_exists='replace',
    index=False
)

df = df_accepted[df_accepted.date == sql_date]

unique_users = df.user.unique().tolist()
users_str = 'Usernames found:'

for user in unique_users:
    num_uses = df[df.user == user].ip.count()
    users_str += f' {user} ({num_uses}),'

if len(unique_users) == 0:
    logger.info('No usernames found.')
else:
    logger.info(users_str[:-1])

handle_failed_requests(failed_requests)
