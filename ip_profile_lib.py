#!/nfs_share/matt_desktop/server_scripts/ip_profile/venv_311/bin/python3.11
import gzip
import requests
import logging
from importlib import import_module
from urllib3.exceptions import ReadTimeoutError, DecodeError
from requests.exceptions import ReadTimeout, ContentDecodingError
from os.path import isfile, isdir
from os import listdir
from datetime import datetime, timedelta
import pickle
import sqlite3
from admintools import MyLogger
import json

# User defined vars
working_dir: str = '/nfs_share/matt_desktop/server_scripts/ip_profile'
trusted_ips: list[str] = ['127.0.0.1', 'localhost', '::1']   # Trusted ips will not be recorded
trusted_ips_file: str = f'{working_dir}/knownip.conf'        # Readable file with trusted ips. Or set to 'None'
api_token_file: str = f'{working_dir}/token.conf'            # File containing api token from ipinfo.io, or set to 'None'
db_file: str = f'{working_dir}/ip_profile.db'                # A sqlite3 database to hold the data
logger_file: str = '/var/log/ip_profile.log'                 # This script will log to this file
vhosts: list[str] = ['matthewrobinsonmusic'] 				 # Each vhost can have its own table by the same name
LAN_prefix: str = '192.168.1.'                               # IP addresses coming from LAN will be trusted and ignored
LAN_city: str = 'Atlanta'
LAN_country: str = 'US'
LAN_postal: str = '30315'
LAN_region: str = 'Georgia'
LAN_timezone: str = 'America/New_York'

if not isdir(working_dir):
    working_dir = './'


# Log Dates - Some distros may log dates and times slightly differently
sql_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
http_log_date = (datetime.now() - timedelta(days=1)).strftime('%d/%b/%Y')
ssh_log_date = (datetime.now() - timedelta(days=1)).strftime('%b %e')
# sql connector
db_con = sqlite3.connect(db_file)
db_cursor = db_con.cursor()


if api_token_file and isfile(api_token_file):
    with open(api_token_file, 'r') as f:
        my_token = f.read()[:-1]
else:
    my_token = None


class ApiError(Exception):
    def __init__(self, ip=None):
        self.ip = ip
        self.message = f'API returned 404 error. Perhaps an invalid ip address was sent. IP: {self.ip}'

    def __str__(self):
        return self.message


ApiConnectionErrors = (TimeoutError, ReadTimeoutError, ReadTimeout, ApiError, DecodeError, ContentDecodingError)

logger = MyLogger(
    name='ip_profile',
    to_file=logger_file,
    to_console=True,
    level=20
).logger


def convert_to_sql_type(dictionary: dict) -> dict:
    sql_types = [int, float, str]

    for key in dictionary:
        val, val_type = dictionary[key], type(dictionary[key])

        if val_type not in sql_types:
            dictionary[key] = json.dumps(val)  # converts to str

    return dictionary


def import_module_by_str(mod) -> None:
    globals()[mod] = import_module(mod)


def os_release() -> dict[str, str]:
    with open('/etc/os-release', 'r') as fh:
        text: list[str] = fh.read().replace('\"', '').split('\n')

    release: dict[str, str] = {}

    for element in text:
        if (
                element
                and '=' in element
                and not element.startswith('#')
        ):
            items: list[str] = element.split('=')
            k: str = items[0]
            v: str = items[1]
            release[k] = v

    return release


def ip_info(ip_address, token=None) -> dict[str, str | int]:
    # API provided by ipinfo.io
    # Returns a dict with information about the given ip address. Keys include :
    # ip, hostname, city, region, country, loc, org, postal, timezone, and more...
    if token:
        api: str = 'https://ipinfo.io/' + ip_address + token  # HTTPS
    else:
        api: str = 'http://ipinfo.io/' + ip_address  # HTTP (data limits may apply)

    return requests.get(api).json()


class IpInfoApi:
    def __init__(self, token=my_token):
        self.token = token
        self.failed_requests = []
        self.successful_requests = []
        self.count_requests = 0

    def ip_request(self, ip_addr) -> dict[str, str | int]:
        # API provided by ipinfo.io
        # Returns a dict with information about the given ip address. Keys include :
        # ip, hostname, city, region, country, loc, org, postal, timezone, and more...
        if self.token:
            api: str = 'https://ipinfo.io/' + ip_addr + self.token  # HTTPS
        else:
            api: str = 'http://ipinfo.io/' + ip_addr  # HTTP (data limits apply)

        self.count_requests += 1
        data =  requests.get(api).json()
        self.successful_requests.append(data)
        return data

    def check_failed_requests(self) -> bool:
        return len(self.failed_requests) > 0


def log_reader(filename):
    if filename.endswith('.gz'):
        with gzip.open(filename, 'rb') as fh:
            for line in fh:
                yield line.decode()
    else:
        with open(filename, 'r') as fh:
            for line in fh:
                yield line


def handle_failed_requests(failed_requests: list) -> None:
    if len(failed_requests) > 0:
        pickle_file = f'{working_dir}/api_error.pickle'

        if not isfile(pickle_file):
            with open(pickle_file, 'wb') as pf:
                pickle.dump([], pf)

        logger.warning(f'Api Error occurred. Check {pickle_file}')

        with open(pickle_file, 'rb') as pf:
            failure_data = pickle.load(pf)

        failed_requests.extend(failure_data)

        with open(pickle_file, 'wb') as pf:
            pickle.dump(failed_requests, pf)
    else:
        logger.debug('failed_requests list is empty.')


if trusted_ips_file and isfile(trusted_ips_file):
    with open(trusted_ips_file, 'r') as f:
        for line in f:
            if line and not line.startswith('#'):
                if '\n' in line:
                    ip = line.replace('\n', '')
                else:
                    ip = line

                if line:
                    trusted_ips.append(ip)


distro = os_release()['ID'].lower()
match distro:
    # find http logs based on distro
    case 'ubuntu' | 'debian':
        auth_file_dir = '/var/log/apache2/'  # http log files directory
        ssh_log_files = [f'/var/log/{file}' for file in listdir('/var/log/') if file.startswith('auth.log')]

    case 'centos' | 'rhel' | 'rocky' | 'almalinux' | 'fedora':
        auth_file_dir = '/var/log/httpd/'  # http log files directory
        ssh_log_files = [f'/var/log/{file}' for file in listdir('/var/log/') if file.startswith('secure')]

    case _:
        logging.warning(f'Unsupported OS -- {distro}')
        raise NotImplementedError(f'{distro.title()} is not currently supported.'
                                  f' Only supports Ubuntu, Debian, CentOs, RHEL, Rocky, AlmaLinux, and Fedora')

http_log_files = [
    f'{auth_file_dir}/{file}'
    for file in listdir(auth_file_dir)
    if file.startswith('access.log')
]
