#!/nfs_share/matt_desktop/server_scripts/ip_profile/venv_311/bin/python3.11
from sys import argv
import pandas as pd
import json
from time import sleep
from ip_profile_lib import (
    ip_info, log_reader, ApiConnectionErrors, ssh_log_files, handle_failed_requests, convert_to_sql_type,
    LAN_prefix, trusted_ips, my_token, ssh_log_date, db_con, sql_date, db_cursor, logger
)


# Defaults
script_name = argv[0]
trusted_ips_counter = 0

# Try to get table from database if exists, or create new one
try:
    df_ssh = pd.read_sql('select * from ssh_user', db_con)
except pd.errors.DatabaseError as e:
    error = e.args[0]

    if 'no such table' in error:  # No such table found. Creating now.
        logger.info('Creating table')
        db_cursor.execute('''
            CREATE TABLE ssh_user (
            ip TEXT,
            city TEXT,
            region TEXT,
            country TEXT,
            loc TEXT,
            org TEXT,
            postal TEXT,
            timezone TEXT,
            attempts INTEGER,
            date TEXT,
            user TEXT,
            hostname TEXT,
            time TEXT,
            anycast TEXT
            )''')

        df_ssh = pd.read_sql('select * from ssh_user', db_con)  # Blank data frame from new table
    else:
        logger.critical('Failed to create table.')
        logger.critical(e)
        logger.critical('Exiting.')
        raise ConnectionError('Database connection failed')


logger.info(' SSH USERNAMES '.center(40, "#"))
failed_requests = []
logger.debug(ssh_log_files)

# Go through the ssh auth files and find IP addresses that failed to connect.
for ssh_log_file in ssh_log_files:
    lines = log_reader(ssh_log_file)

    for line in lines:  # find the indexes of 'time', 'user', and 'ip' in the matched lines (to use with .split())
        if ('Connection closed by invalid user' in line
                and ssh_log_date in line
                and LAN_prefix not in line):
            line_elements = (2, 10, 11)  # (2, -5, -4)
        elif ('Connection closed by authenticating user' in line
              and ssh_log_date in line
              and LAN_prefix not in line):
            line_elements = (2, 10, 11)  # (2, -5, -4)
        elif ('Disconnected from invalid user' in line
              and ssh_log_date in line
              and LAN_prefix not in line):
            line_elements = (2, 9, 10)  # (2, -5, -4)
        elif ('Disconnected from authenticating user' in line
              and ssh_log_date in line
              and LAN_prefix not in line):
            line_elements = (2, 9, 10)
        elif ('Disconnecting invalid user' in line
              and ssh_log_date in line
              and LAN_prefix not in line):
            line_elements = (2, 8, 9)
        else:
            line_elements = None  # line is logging something unrelated. Ignore.

        if line_elements:
            # split the line and grab the related elements (based on its unique indexes)
            x, y, z = line_elements
            time = line.split()[x]
            user = line.split()[y]
            ip = line.split()[z]

            """Some offenders will try to use a literal whitespace as the username, resulting in a whitespace where
            the username should be. That is problematic for .split(). This if-statement offsets the problem. Also,
            it is necessary to slice off the first 7 characters because dates will sometimes have a double white-
            space too since the date format is '%b %e' -- eg: Jan  1."""

            if '  ' in line[7:]:        # notice the double-whitespace in the quotes
                user = "\' \'"          # A whitespace inside literal single-quotes will be entered into the database
                ip = line.split()[z-1]  # 'ip' needs to be offset by -1 in .split(). And 'time' element is unaffected

            if ip not in trusted_ips:   # Trusted ips will not be recorded
                # Check to see how many prior attempts were made yesterday
                ip_entries = df_ssh[
                    (df_ssh.ip == ip) &
                    (df_ssh.user == user) &
                    (df_ssh.date == sql_date)
                    ].ip.count()

                if ip_entries == 0:  # a new attempt was made
                    try:
                        entry: dict = ip_info(ip, my_token)  # Gather info about ip address from the api
                        entry['user']: str = user  # Add user, date, attempts, and time to the dict
                        entry['date']: str = sql_date
                        entry['attempts']: int = 1
                        entry['time']: str = time
                        entry['attempt_times']: str = json.dumps([time])  # convert python list to sql text
                        entry = convert_to_sql_type(entry)

                        # Concat with old DataFrame
                        df_entry = pd.DataFrame([entry])  # Convert to DataFrame
                        df_ssh = pd.concat([df_ssh, df_entry], ignore_index=True)

                    except ApiConnectionErrors as e:
                        failed_entry: dict = {
                            'ip': ip,
                            'user': user,
                            'time': time,
                            'date': sql_date,
                            'line': line,
                            'script': 'ssh_user',
                        }
                        failed_requests.append(failed_entry)
                        logger.error(f'Failed : {failed_entry}')
                        logger.error(e)
                        logger.error(line)
                        sleep(60)  # possible connection issue. Wait for resolution and continue
                else:  # Attempt was already made. Increase 'attempt' counter by one in sql.
                    df_index = df_ssh[
                        (df_ssh.ip == ip) &
                        (df_ssh.user == user) &
                        (df_ssh.date == sql_date)
                        ].index.item()  # Get the index for the match.

                    # Increase the 'attempts' by one.
                    df_ssh.at[df_index, 'attempts'] += 1
                    # Create a list of all times a connection was attempted
                    attempt_times = json.loads(df_ssh.at[df_index, 'attempt_times'])
                    attempt_times.append(time)
                    # Convert list into string and insert into sql
                    df_ssh.at[df_index, 'attempt_times'] = json.dumps(attempt_times)
            else:
                trusted_ips_counter += 1

attempts_counter = df_ssh[df_ssh.date == sql_date].attempts.sum()

if attempts_counter == 0:
    logger.info(f'No new connections found. Done. {trusted_ips_counter} trusted ips found.')
else:
    df_ssh.to_sql(
        name='ssh_user', 
        con=db_con,
        if_exists='replace',
        index=False
    )
    num_unique_ips = df_ssh[df_ssh.date == sql_date].ip.nunique()
    logger.info(f'{num_unique_ips} ips connected. {attempts_counter} attempts made. {trusted_ips_counter} trusted ips')

handle_failed_requests(failed_requests)
