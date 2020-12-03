#!/home/ofg/miniconda3/bin/python3.7
#!/home/webmgr/miniconda3/bin/python3.7

# ui1b armweb-stage armweb-dev
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


script_description = """
This script will get the netCDF headers for the most recent file for all datastreams available on
Data Discovery and put them in a location that the Data Discovery UI can read and display them.
The list of files will be given based on a database query. Files will be retrieved from
/data/archive/ if available, else they will be downloaded from hpss using adrsws. Files that are
downloaded from hpss will have the appropriate headers dumped and then the downloaded files will be
deleted."""

example='TODO' #TODO


def parse_arguments():
    arg_parser = argparse.ArgumentParser(description=script_description, epilog=example,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument("-T", "--test", dest='config', action="store_const",
                            default='config_prod.ini', const='config_dev.ini',
                            help="Use testing configuration.")
    return arg_parser.parse_args()


def parse_config(config_file):
    try:
        config_path = os.path.join(expanduser("~"), 'DD_cdf_headers', config_file)
        config = configparser.ConfigParser()
        config.read_file(open(config_path))
        return config
    except FileNotFoundError:
        logger.critical('Could not find configuration file: {}'.format(config_path))
        exit(1)


def setup_logging(config):
    logger.remove()
    os.makedirs(config["logging"]["log_dir"], exist_ok=True)
    log_file = os.path.join(config["logging"]["log_dir"], 'dd_file_headers.log')
    logger.add(log_file, rotation='50MB', retention='1 month')
    logger.add(sys.stdout, colorize=True, level=config["logging"]["level"])


def get_headers_db(config):
    create_starting_directories(config)
    download_list = os.path.join(expanduser("~"), "DD_cdf_headers", 'download_list',
                                 'download_list.{}.txt'.format(
        dt.datetime.now().strftime('%Y%m%d_%H%M%S')))
    backup_download_list(download_list)
    results = db_query(config)
    for result in results:
        fname, site, ds, file_date = parse_result(result)

        archive_file_path = build_archive_path(fname, site, ds)
        header_path = build_header_path(config, ds)

        skip_list = ['.png', '.mpg', '.raw', '.jpg', '.tsv.tar',
                     '.pdf.tar', '.txt.tar', '.asc.tar', '.00.']
        if any([x for x in skip_list if x in fname]): # files that won't have cdf headers
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
    if config["hpss"]["stage"] == 'True':
        # basic way of not doing this very long process
        logger.info('Downloading from hpss...')
        stage_from_hpss(config, download_list)
        tar_paths = find_tars(config)
        for tar_path in tar_paths:
            extract_tar(config, tar_path)
            remove_tar(tar_path)
            netcdf_paths = find_netcdf(config)
            header_path = build_tar_header(tar_path)
            dump_multi_netcdf(netcdf_paths, header_path)


def create_starting_directories(config):
    os.makedirs(config["path"]["header_dir"], exist_ok=True)
    os.makedirs(config["path"]["download_loc"], exist_ok=True)
    os.makedirs(config["path"]["extraction_loc"], exist_ok=True)


def backup_download_list(download_list):
    if os.path.exists(download_list):
        new_name = "{}.old".format(download_list)
        os.rename(download_list, new_name)


def db_query(config):
    # build connection info dictionary
    connection_info = {
        "application_name" : config['db_conn']["application_name"],
        "host" : config['db_conn']["host"],
        "dbname" : config['db_conn']["dbname"],
        "user" : config['db_conn']["user"],
        "password" : config['db_conn']["password"]
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


def parse_result(result):
    fname = result[0]
    site = fname[:3]
    ds = result[1]
    file_date = result[2]
    return fname, site, ds, file_date


def build_archive_path(fname, site, ds):
    return '/data/archive/{}/{}/{}'.format(site, ds, fname)


def build_header_path(config, ds):
    return os.path.join(config["path"]["header_dir"], '{0}.header.txt'.format(ds))


def dump_archive_header(file_path, header_path):
    if os.path.exists(header_path):
        os.remove(header_path)
    os.system('ncdump -h {0} > {1}'.format(file_path, header_path))


def stage_from_hpss(config, download_list):
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
    cmd = [config["adrsws"]["adrsws_path"],
           '-u', config["adrsws"]["userid"],
           '-g', config["path"]["download_loc"],
           download_list]
    logger.info('\nrunning command: {}\n'.format(' '.join(cmd)))
    logger.info('... This may take a while ...')
    logger.info('The following output is from adrsws.')
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc_out = get_process_output(process)
    logger.info('Done with adrsws log.')
    return proc_out


def parse_dates(file_date):
    start = file_date.strftime('%Y-%m-%d')
    end_dt = file_date + dt.timedelta(days=1)
    end = end_dt.strftime('%Y-%m-%d')
    return start, end


def find_tars(config):
    tar_paths = []
    for path, dirs, files in os.walk(config["path"]["download_loc"]):
        for f in files:
            if '.tar' in f:
                tar_paths.append(os.path.join(path,f))
    return tar_paths


def extract_tar(config, tar_path):
    logger.info('Extracting: {}'.format(tar_path))
    if not os.path.exists(config["path"]["extraction_loc"]):
        os.mkdir(config["path"]["extraction_loc"])
    tar_file = tarfile.open(tar_path)
    tar_file.extractall(path=config["path"]["extraction_loc"])


def remove_tar(tar_path):
    os.remove(tar_path)


def build_tar_header(config, tar_path):
    base_name = os.path.basename(tar_path)
    datastream = '.'.join(base_name.split('.')[:2])
    return os.path.join(config["path"]["header_dir"], '{0}.header.txt'.format(datastream))


def find_netcdf(config):
    logger.info('Looking for netcdf files.')
    netcdf_paths = []
    netcdf_ext = ['.nc', '.cdf']
    for path, dirs, files in os.walk(config["path"]["extraction_loc"]):
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


def clean_extraction(config):
    logger.info('Cleaning up downloads in: {}'.format(config["path"]["extraction_loc"]))
    shutil.rmtree(config["path"]["extraction_loc"], ignore_errors=True)
    logger.debug('Recreating extraction loc.')
    os.mkdir(config["path"]["extraction_loc"])


def clean_downloads(config):
    logger.info('Cleaning up downloads in: {}'.format(config["path"]["download_loc"]))
    shutil.rmtree(config["path"]["download_loc"], ignore_errors=True)
    logger.debug('Recreating download loc.')
    os.mkdir(config["path"]["download_loc"])


def clean_headers(config):
    logger.info('Cleaning up headers in: {}'.format(config["path"]["header_dir"]))
    shutil.rmtree(config["path"]["header_dir"], ignore_errors=True)
    logger.debug('Recreating header loc.')
    os.mkdir(config["path"]["header_dir"])


if __name__ == "__main__":
    args = parse_arguments()
    config = parse_config(args.config)
    try:
        setup_logging(config)
        get_headers_db(config)
    except Exception as e:
        print('exception: {}'.format(e))
        logger.warning('Exception raised: {}'.format(e))
    finally:
        clean_extraction(config)
        clean_downloads(config)
        logger.info('Done')
        print('Done')
