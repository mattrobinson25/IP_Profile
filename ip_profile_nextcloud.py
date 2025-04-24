#!/nfs_share/matt_desktop/server_scripts/ip_profile/venv_311/bin/python3.11
import pandas as pd
from os import listdir
import json
from ip_profile_lib import (
    ip_info, log_reader, ApiError, trusted_ips, sql_date, http_log_date,
    LAN_prefix, logger, auth_file_dir, my_token, db_cursor, db_con
)

# User defined variables.
vhosts = ['nextcloud']  # Each vhost can have its own table by the same name

log_files = [
    f'{auth_file_dir}/{file}'
    for file in listdir(auth_file_dir)
    if file.startswith('nextcloud-access.log')
]

for vhost in vhosts:
    # Try to get table from database (if exists)
    try:
        df = pd.read_sql(f'select * from {vhost}', db_con)
        error = None
    except pd.errors.DatabaseError as e:
        error = e.args[0].lower()

        if 'no such table' in error:  # No such table found. Creating now.
            logger.info('Creating table')
            db_cursor.execute(f'''
                CREATE TABLE {vhost} (
                ip TEXT,
                city TEXT,
                region TEXT,
                country TEXT,
                loc TEXT,
                org TEXT,
                postal TEXT,
                timezone TEXT,
                packets INTEGER,
                date TEXT,
                hostname TEXT,
                time TEXT,
                anycast TEXT,
                data TEXT
                )''')

            df: pd.DataFrame = pd.read_sql(f'select * from {vhost}', db_con)  # Blank data frame from new table

        else:
            logger.info('Failed to create table.')
            logger.info(error)
            logger.info('Exiting.')
            exit()

    # Begin reading through http logs and finding the data
    counter = 0

    for file in log_files:
        lines = log_reader(file)

        for line in lines:
            ip = line.split()[0]
            time = line.split()[3].split('/')[-1][5:]

            if (http_log_date in line
                    and LAN_prefix not in line
                    and ip not in trusted_ips):

                # Valid connection found. Add to counter.
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

                        df_entry = pd.DataFrame([entry])  # convert dict to dataframe
                        df = pd.concat([df, df_entry], ignore_index=True)
                    except ApiError as e:
                        logger.error(e)

                else:  # Add to already existing connection
                    df_index = df[
                        (df.ip == ip) &
                        (df.date == sql_date)
                        ].index.item()

                    data = json.loads(
                        df.at[df_index, 'data']
                    )
                    data.append(line)

                    df.at[df_index, 'data'] = json.dumps(data)
                    df.at[df_index, 'packets'] += 1  # increment packet counter

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
