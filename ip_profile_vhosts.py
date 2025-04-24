#!/nfs_share/matt_desktop/server_scripts/ip_profile/venv_311/bin/python3.11
from sys import argv
import json
import pandas as pd
from time import sleep
from ip_profile_lib import (
    ip_info, log_reader, trusted_ips, sql_date, http_log_date, LAN_prefix, http_log_files,
    logger, db_cursor, vhosts, my_token, ApiConnectionErrors, handle_failed_requests, db_con
)


script_name = argv[0]
failed_requests = []

for vhost in vhosts:
    # Try to get table from database (if exists)
    try:
        df = pd.read_sql(f'select * from {vhost}', db_con)
        error = None
    except pd.errors.DatabaseError as e:
        error = (e.args[0]).lower()
        if 'no such table' in error:  # No such table found. Creating now.
            logger.info('Creating table')
            db_cursor.execute(f'''
                CREATE TABLE {vhost} (
                ip TEXT,
                city TEXT,
                region TEXT,
                country TEXT,
                org TEXT,
                packets INTEGER,
                date TEXT,
                hostname TEXT,
                time TEXT,
                data TEXT,
                loc TEXT,
                postal TEXT,
                timezone TEXT,
                bogon REAL
                )''')

            df = pd.read_sql(f'select * from {vhost}', db_con)  # Blank data frame from new table
        else:
            logger.error('Failed to create table.')
            logger.error(e)
            logger.error('Exiting.')
            exit()

    # Begin reading through logs and finding the data
    counter = 0

    for log_file in http_log_files:
        lines = log_reader(log_file)  # Generator based on the lines of the log file

        for line in lines:
            ip = line.split()[0]
            time = line.split()[3].split('/')[-1][5:]

            if (
                    http_log_date in line
                    and vhost in line
                    and LAN_prefix not in line
                    and ip not in trusted_ips
            ):
                counter += 1
                # Check to see if ip is already in database for yesterday
                ip_entries = df[
                    (df.ip == ip) &
                    (df.date == sql_date)
                    ].ip.count()

                if ip_entries == 0:  # A new connection was found
                    try:
                        entry = ip_info(ip, my_token)
                        entry['packets'] = 1
                        entry['date'] = sql_date
                        entry['time'] = time
                        entry['data'] = json.dumps([line])  # Python list stored in DB as a string

                        logger.debug(entry)
                        df_entry = pd.DataFrame([entry])
                        df = pd.concat(objs=[df, df_entry], ignore_index=True)

                    except ApiConnectionErrors as error:
                        failed_request = {'ip': ip, 'time': time, 'date': sql_date, 'line': line, 'script': 'vhost'}
                        failed_requests.append(failed_request)
                        logger.error(line)
                        logger.error(error)
                        sleep(60)

                else:  # Add to already existing connection
                    df_index = df[
                            (df.ip == ip) &
                            (df.date == sql_date)
                        ].index.item()

                    data = json.loads(df.at[df_index, 'data'])  # Take DB entry and convert to python list
                    data.append(line)                           # Add new packet data from http log to list
                    df.at[df_index, 'data'] = json.dumps(data)  # Convert data back to str and insert in DB
                    df.at[df_index, 'packets'] += 1             # Increase packet counter

    logger.info(f' {vhost.upper()} '.center(40, "#"))

    if counter > 0:
        num_ip_addresses = df[df.date == sql_date].ip.nunique()

        logger.info(f'{counter} packets transmitted. {num_ip_addresses} addresses connected.')
        df.to_sql(
            name=vhost,
            con=db_con,
            if_exists='replace',
            index=False
        )
    else:
        logger.info('No new connections found.')

handle_failed_requests(failed_requests)
