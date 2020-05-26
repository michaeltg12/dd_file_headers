#!/data/home/giansiracusa/miniconda3/bin/python3

"""
This code is a proof of concept and should not be put into production as is.
It could use some oop and better logging.
It needs better argument parsing and proper debug logging.
The hard coded paths should be changed to user defined or relative. The problem with this is that
a huge amount of space is needed to download the lasso files.
"""

# ui1b armweb-stage armweb-dev
# This will have to be run on one of the machines that has /data/reproc mounted so that is
# has the space to temporarily download the Lasso tar bundles which are 25-50 Gb.
import os
import sys
import json
import shutil
import tarfile
import argparse
import psycopg2
import subprocess
import configparser
import datetime as dt
from loguru import logger
from os.path import expanduser
from logging.handlers import RotatingFileHandler


script_description = """
This script will get the netCDF headers for the most recent file for all datastreams available on
Data Discovery and put them in a location that the Data Discovery UI can read and display them.
The list of files will be given based on a database query. Files will be retrieved from
/data/archive/ if available, else they will be downloaded from hpss using adrsws. Files that are
downloaded from hpss will have the appropriate headers dumped and then the downloaded files will be
deleted."""

example='TODO' #TODO

# output_path = '/var/www/html/headers' # this was when it could run on ui1b
tmp_dir = "/work/netcdf_headers/"
log_dir = "/var/log/dd_file_headers"
header_dir = "/var/www/vhosts/archive.arm.gov/headers"
download_loc = os.path.join(tmp_dir, 'download/')
extraction_loc = os.path.join(tmp_dir, 'extraction/')
download_list = os.path.join(tmp_dir, 'download_list.{}.txt'.format(dt.datetime.now().strftime('%Y%m%d_%H%M%S')))
adrsws_path = '/home/ofg/DD_cdf_headers/bin/./adrsws.sh'
userid = 'giansiracusam1'


def parse_arguments():
    arg_parser = argparse.ArgumentParser(description=script_description, epilog=example,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument("-T", "--test", dest='config', action="store_const",
                            default='config_prod.ini', const='config_dev.ini',
                            help="Use testing configuration.")
    # arg_parser.add_argument("-D", "--debug", action="store_const",
    #                         default=logger.setLevel('INFO'), const=logger.setLevel('DEBUG'),
    #                         help="Set console logging.")
    return arg_parser.parse_args()


def parse_config(config_file):
    try:
        config = configparser.ConfigParser()
        config.read_file(open(config_file))
        #os.path.join(tmp_dir, 'download_list.{}.txt'.format(dt.datetime.now().strftime('%Y%m%d_%H%M%S')))
        return config
    except FileNotFoundError:
        logger.critical('Could not find configuration file: {}'.format(config_file))
        exit(1)


def setup_logging(args):
    logger.remove()
    if not args.test:
        log_dir = defaults['logging']['log_dir']
    else:
        log_dir = defaults['logging']['dev_log_dir']
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'ingest_log_parser.log')
    logger.add(log_file, rotation='50MB', retention='1 month')
    if args.debug:
        logger.add(sys.stdout, colorize=True, level="DEBUG")
    else:
        logger.add(sys.stdout, colorize=True, level="INFO")


def get_headers_db():
    backup_download_list()
    results = db_query()
    for result in results:
        fname, site, ds, file_date = parse_result(result)

        archive_file_path = build_archive_path(fname, site, ds)
        header_path = build_header_path(ds)

        skip_list = ['.png', '.mpg', '.raw', '.jpg', '.tsv.tar',
                     '.pdf.tar', '.txt.tar', '.asc.tar', '.00.']
        if any([x for x in skip_list if x in fname]): # files that won't have cdf files
            logger.warning('Skipping: {0}'.format(fname))
            continue
        elif os.path.exists(archive_file_path): # dump header from /data/archive
            logger.info("{} > {}".format(archive_file_path, header_path))
            dump_archive_header(archive_file_path, header_path)
        elif '.tar' in fname: # create list of files to download from hpss
            logger.info('File not in archive: {}'.format(archive_file_path))
            with open(download_list, 'a+') as dlist:
                logger.info('Writing to download list: {}'.format(fname))
                dlist.write('{}\n'.format(fname))
            continue
    # download file from hpss
    if False: # basic way of not doing this very long process
        logger.info('Downloading from hpss...')
        stage_from_hpss(adrsws_path, userid, download_loc)
        tar_paths = find_tars()
        for tar_path in tar_paths:
            extract_tar(tar_path)
            remove_tar(tar_path)
            netcdf_paths = find_netcdf()
            header_path = build_tar_header(tar_path)
            dump_multi_netcdf(netcdf_paths, header_path)
            clean_extraction()
    copy_files_2_ddprod()
    clean_headers()


def setup_logging(loglevel):
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    global logger
    logger = logging.getLogger('DD_header_processing')  # Name the logger
    formatter = logging.Formatter(
        '%(levelname)s (%(funcName)s %(lineno)s) : %(message)s')  # Format prints

    handler = logging.StreamHandler()  # Handler to print to console
    handler.setFormatter(formatter)  # Set the console logger format
    logger.addHandler(handler)  # Attach console handler to the logger

    time_fmt = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, 'dd_headers.{}.log'.format(time_fmt))
    file_handler = RotatingFileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)  # Attach file handler to logger

    logger.setLevel(loglevel)  # Set logger level


def backup_download_list():
    if os.path.exists(download_list):
        new_name = "{}.old".format(download_list)
        os.rename(download_list, new_name)


def db_query():
    # get password from users .pgpass file
    user = 'utilities_select_only'
    passwd = parse_pgpass(user)
    # build connection info dictionary
    connection_info = {
        "application_name" : 'data_descovery_header_dump',
        "host" : 'armdb-foo.ornl.gov',
        "dbname" : 'arm_all',
        "user" : user,
        "password" : passwd
    }
    # connect using dictionary expansion
    with psycopg2.connect(**connection_info) as conn:
        cursor = conn.cursor()
        sql_like = '''
            with dates as (
                select max(start_time) as last_file, zeb_platform
                from user_access_files.zebfile_timeplat
                group by zeb_platform
            )
            select old_filename, z.zeb_platform, last_file from user_access_files.zebfile_timeplat z
            inner join dates d on z.start_time=d.last_file and z.zeb_platform=d.zeb_platform;
        '''
        cursor.execute(sql_like)
        results = cursor.fetchall()
        return results


def parse_pgpass(user):
    with open(os.path.join(expanduser('~'), '.pgpass'), 'r') as pgpass:
        for line in pgpass.readlines():
            if user in line:
                return line.split(":")[-1].strip()
    raise ValueError("User {} password not defined in .pgpass file.".format(user))


def parse_result(result):
    fname = result[0]
    site = fname[:3]
    ds = result[1]
    file_date = result[2]
    return fname, site, ds, file_date


def build_archive_path(fname, site, ds):
    return '/data/archive/{}/{}/{}'.format(site, ds, fname)


def build_header_path(ds):
    return os.path.join(header_dir, '{0}.header.txt'.format(ds))


def dump_archive_header(file_path, header_path):
    if os.path.exists(header_path):
        os.remove(header_path)
    os.system('ncdump -h {0} > {1}'.format(file_path, header_path))


def stage_from_hpss(adrsws_path, userid, download_loc):
    # helper method for printing subprocess output
    def get_process_output(process):
        while True:
            output = process.stdout.readline().decode('utf-8')
            try:
                order_info = json.loads(output)
            except json.decoder.JSONDecodeError:
                pass
            poll = process.poll()
            if output == '' and poll is not None:
                return order_info
            if output:
                output = output.strip().replace('\n', '').replace('\r', '')
                logger.info(output)
    cmd = [adrsws_path, '-u', userid, '-g', download_loc, download_list]
    logger.info('... This may take a while ...')
    logger.debug('\n{}\n'.format(' '.join(cmd)))
    logger.info('The following output is from adrsws.')
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return get_process_output(process)


def parse_dates(file_date):
    start = file_date.strftime('%Y-%m-%d')
    end_dt = file_date + dt.timedelta(days=1)
    end = end_dt.strftime('%Y-%m-%d')
    return start, end


def find_tars():
    tar_paths = []
    for path, dirs, files in os.walk(download_loc):
        for f in files:
            if '.tar' in f:
                tar_paths.append(os.path.join(path,f))
    return tar_paths


def extract_tar(tar_path):
    logger.info('Extracting: {}'.format(tar_path))
    if not os.path.exists(extraction_loc):
        os.mkdir(extraction_loc)
    tar_file = tarfile.open(tar_path)
    tar_file.extractall(path=extraction_loc)


def remove_tar(tar_path):
    os.remove(tar_path)


def build_tar_header(tar_path):
    base_name = os.path.basename(tar_path)
    datastream = '.'.join(base_name.split('.')[:2])
    return os.path.join(header_dir, '{0}.header.txt'.format(datastream))


def find_netcdf():
    logger.info('Looking for netcdf files.')
    netcdf_paths = []
    netcdf_ext = ['.nc', '.cdf']
    for path, dirs, files in os.walk(extraction_loc):
        for f in files:
            if any([True for x in netcdf_ext if f.endswith(x)]):
                netcdf_paths.append(os.path.join(path, f))
    logger.debug('netcdf files found: {}'.format(netcdf_paths))
    netcdf_paths.sort()
    return netcdf_paths


def dump_multi_netcdf(netcdf_paths, header_path):
    if os.path.exists(header_path):
        logger.debug('Removing: {}'.format(header_path))
        os.remove(header_path)
    logger.info('Writing to: {}'.format(header_path))
    for netcdf_file in netcdf_paths:
        dump_file = 'ncdump -h {0} >> {1}'.format(netcdf_file, header_path)
        line_break = 'echo "{0}" >> {1}'.format('='*50, header_path)
        logger.debug(dump_file)
        os.system(dump_file)
        os.system(line_break)


def copy_files_2_ddprod():
    logger.info('Transfering files to Data Discovery Production Server.')
    header_files = os.path.join(header_dir, '*.header.txt')
    logger.debug('header files for transfer: {}'.format(header_files))
    destination = 'ofg@ui1b.ornl.gov:/var/www/vhosts/archive.arm.gov/headers/'
    cmd = ["scp", header_files, destination]
    logger.debug('Running command: {}'.format(' '.join(cmd[:])))
    os.system(' '.join(cmd[:]))


def clean_extraction():
    logger.info('Cleaning up downloads in: {}'.format(extraction_loc))
    shutil.rmtree(extraction_loc, ignore_errors=True)
    logger.debug('Recreating extraction loc.')
    os.mkdir(extraction_loc)


def clean_downloads():
    logger.info('Cleaning up downloads in: {}'.format(download_loc))
    shutil.rmtree(download_loc, ignore_errors=True)
    logger.debug('Recreating download loc.')
    os.mkdir(download_loc)


def clean_headers():
    logger.info('Cleaning up headers in: {}'.format(header_dir))
    shutil.rmtree(header_dir, ignore_errors=True)
    logger.debug('Recreating header loc.')
    os.mkdir(header_dir)


if __name__ == "__main__":
    args = parse_arguments()
    print(args)
    config = parse_config(args.config)
    print(config.get('path', 'header_dir'))

    exit()
    try:
        setup_logging(log_level)
        get_headers_db()
    except Exception as e:
        logger.warning('Exception raised: {}'.format(e))
    finally:
        clean_extraction()
        clean_downloads()
        logger.info('Done')
