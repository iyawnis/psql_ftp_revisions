#!/usr/bin/env python3
import shutil
import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from io import BytesIO
import subprocess

from ftplib import FTP
from pathlib import Path, PurePath
from typing import List, Tuple
from collections import namedtuple

# ********************************************** #
# ********  CONFIGURATION VARIABLES ************
# ********************************************** #

FTP_HOST = '192.168.0.15'
FTP_USER = 'pi'
FTP_PASSWD = 'raspberry'
FTP_DIR = 'FTP'

conn_config = {
    'host': 'localhost',
    'dbname': 'postbooks',
}

LOWRITER_COMMAND = ['lowriter', '--convert-to', 'pdf:writer_pdf_Export']
PS_COMMAND = ['ps2pdf', '-dPDFSETTINGS=/ebook']
TEMP_DIR = 'temp_files'

logging.basicConfig(format='%(asctime)s:%(levelname)s: %(message)s', level=logging.INFO)

# ********************************************** #
# ********************************************** #

VersionUpdate = namedtuple('VersionUpdate', [
    'file_id',
    'file_name',
    'item_number'])

NewUpload = namedtuple('NewUpload', [
    'item_id',
    'file_name',
    'item_number'])


class NewFile(object):
    def __init__(self, *args, **kwargs):
        self.file_id = None
        self.item_id = kwargs['item_id']
        self.file_title = kwargs['file_title']
        self.file_stream = kwargs['file_stream']

    def get_description(self):
        descr = str(PurePath(self.file_title).stem).split(' ')
        if len(descr) > 1:
            return ' '.join(descr[1:])
        return descr[0]

    def file(self):
        return self.file_title, self.file_stream, self.get_description()

    def docass(self):
        return self.item_id, 'I', self.file_id, 'FILE', 'S', 'now()'


class File(object):
    def __init__(self, *args, **kwargs):
        self.file_id = kwargs['file_id']
        self.file_title = kwargs['file_title']
        self.file_stream = kwargs['file_stream']

    def get_description(self):
        descr = str(PurePath(self.file_title).stem).split(' ')
        if len(descr) > 1:
            return ' '.join(descr[1:])
        return descr[0]

    def file(self):
        return self.file_id, self.file_title, self.get_description(), self.file_stream


def temp_path(filename):
    return os.path.join(TEMP_DIR, filename)


def ftp_path(filename):
    return os.path.join(FTP_DIR, filename)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def is_updated_version(db_file_name, ftp_file_name):
    db_version = PurePath(db_file_name).stem.split('_')[1].split(' ')[0]
    ftp_version = PurePath(ftp_file_name).stem.split('_')[1].split(' ')[0]
    # Numbers always newer than letters
    if ftp_version.isdigit() and db_version.isalpha():
        return True
    elif ftp_version.isalpha() and db_version.isdigit():
        return False
    if ftp_version.isdigit():
        return int(ftp_version) > int(db_version)
    return ftp_version.lower() > db_version.lower()


class FileSync(object):

    def cleanup(self):
        """
        Empty the temporary directory for file processing
        """
        if os.path.exists(TEMP_DIR):
            shutil.rmtree(TEMP_DIR)
        os.makedirs(TEMP_DIR)

    @staticmethod
    def execute_sql(sql, params):
        with psycopg2.connect(**conn_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (params,))
                return cursor.fetchall()

    @staticmethod
    def file_name_to_item(file_name):
        return file_name.split('_')[0]

    def get_ftp_file_names(self):
        """
        Find the files that we have in FTP,
        and create a dict with item_name to file mapping
        """
        with FTP(host=FTP_HOST, user=FTP_USER, passwd=FTP_PASSWD) as ftp:
            files = ftp.nlst(FTP_DIR)
            self.file_dict = {
                self.file_name_to_item(file_name): file_name
                for file_name in files if '.' in file_name}
            return self.file_dict.keys()

    @staticmethod
    def get_db_item_names() -> List[Tuple[str, str]]:
        SQL = """SELECT item.item_id, item.item_number FROM item;"""
        return [row for row in FileSync.execute_sql(SQL, None)]

    def filter_ftp_dir_items(self, items_in_db):
        """
        Compare DB item_number entries with FTP file_name
        to find all the files that we are interested in
        """
        items_in_db = [item[1] for item in items_in_db]
        items_to_remove = filter(lambda x: x not in items_in_db, self.file_dict.keys())
        for item in list(items_to_remove):
            self.file_dict.pop(item)
        return self.file_dict.keys()

    def filter_ftp_items_already_stored(self):
        """
        Filter ftp items that are already stored in the DB
        If a file exists, with file_title same as the ftp file name, then no action required.
        """

        sql = """
            SELECT file.file_title FROM file WHERE file_title SIMILAR TO %s;
        """
        # uploaded files will be PDF, or on their original format
        files_without_suffix = [
            str(PurePath(file).with_suffix('')) + '%'
            for file in sorted(self.file_dict.values())]
        sql_similar = '|'.join(files_without_suffix)
        uploaded_files = self.execute_sql(sql, sql_similar)
        # we have all already uploaded these files, remove them from the ftp file dict
        for file_match in uploaded_files:
            self.file_dict.pop(self.file_name_to_item(file_match[0]))

    def files_to_be_updated(self) -> List[VersionUpdate]:
        """
        Find which files have newer versions in FTP, and upload them
        """
        files_with_existing_versions = """
            SELECT file.file_id, file.file_title FROM file WHERE file_title SIMILAR TO %s;
        """
        files_from_ftp = '|'.join(sorted(self.file_dict.keys()))
        query_filter = "({})%".format(files_from_ftp)

        files_with_versions_uploaded = self.execute_sql(files_with_existing_versions, query_filter)
        files_to_update = []
        for file_id, file_name in files_with_versions_uploaded:
            item_number = self.file_name_to_item(file_name)
            if item_number not in self.file_dict:
                logging.info('File not matching naming scheme, ignoring (%s)' % item_number)
                continue
            # remove the file from file_dict, as it's either updated, or older version
            ftp_file_name = self.file_dict.pop(item_number)
            if is_updated_version(file_name, ftp_file_name):
                files_to_update.append(
                    VersionUpdate(
                        file_id=file_id,
                        file_name=ftp_file_name,
                        item_number=item_number))
        return files_to_update

    def files_not_in_system(self, items_dict: dict):
        """
        Any remaining files in FTP that are not for updating existing ones, are new entries.
        For new files we need to add docass and ls entries
        """
        return [
            NewUpload(
                item_id=items_dict[item_number],
                file_name=file_name,
                item_number=item_number) for item_number, file_name in sorted(self.file_dict.items())]

    def load_ftp_file(self, filename):
        """
        Given a filename, fetch the file from FTP, and return a stream object
        """
        with BytesIO() as byte_stream:
            with FTP(host=FTP_HOST, user=FTP_USER, passwd=FTP_PASSWD) as ftp:
                ftp.retrbinary('RETR {}'.format(ftp_path(filename)), byte_stream.write)
            byte_stream.seek(0)
            return BytesIO(byte_stream.read())

    def store_stream_as_file(self, filename, file_stream):
        with open(temp_path(filename), 'wb') as temp_file:
            temp_file.write(file_stream.read())

    def transform_file(self, filename):
        """
        lowriter and ps2pdf don't play nice with some file names
        to counter that we simply rename the files for processing.
        """
        source_path = temp_path(filename)
        lowriter_source = Path(source_path)
        lowriter_filename = temp_path('lowriter_in' + lowriter_source.suffix)
        lowriter_source.rename(lowriter_filename)
        lowriter_source = PurePath(lowriter_filename)

        lowriter_dest = lowriter_source.with_suffix('.pdf')
        ps2pdf_dest = temp_path('compressed.pdf')

        return_file_path = ps2pdf_dest
        return_file_name = str(PurePath(filename).with_suffix('.pdf'))
        try:
            logging.info('Transforming "{}"'.format(source_path))
            subprocess.call(LOWRITER_COMMAND + [str(lowriter_source), '--outdir', TEMP_DIR])
            subprocess.call(PS_COMMAND + [str(lowriter_dest), ps2pdf_dest])
        except Exception as e:
            pass
        finally:
            if not Path(return_file_path).is_file():
                # commands failed
                return_file_path = str(lowriter_source)
                return_file_name = filename
        with open(return_file_path, 'rb') as fin:
            return return_file_name, BytesIO(fin.read())

    def update_existing_files(self, files: List[File]):
        """
        Insert the updated file on existing file objects
        """
        sql = """
            UPDATE file
            SET file_title=data.title, file_descrip=data.descr, file_stream=data.stream
            FROM (VALUES %s) as data(id, title, descr, stream)
            WHERE file_id=data.id;
        """
        for file_batch in chunks(files, 5):
            with psycopg2.connect(**conn_config) as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, sql, [file.file() for file in file_batch])

    def process_updates(self, files_to_update: List[VersionUpdate]):
        """
        Files of different format, will be fetched locally, and transformed to PDF
        Then all files are uploaded
        """
        transformed_files = []
        for update in files_to_update:
            file_title = update.file_name
            file_stream = self.load_ftp_file(file_title)
            if PurePath(file_title).suffix != '.pdf':
                self.store_stream_as_file(file_title, file_stream)
                file_title, file_stream = self.transform_file(file_title)
            transformed_files.append(File(
                file_id=update.file_id,
                file_title=file_title,
                file_stream=psycopg2.Binary(file_stream.read())))
        return self.update_existing_files(transformed_files)

    def insert_new_files(self, files: List[NewFile]) -> List[NewFile]:
        """
        Upload new files to DB, and keep track of their IDs
        """
        sql = """
        INSERT INTO file (file_title, file_stream, file_descrip)
        VALUES %s
        RETURNING file_id;
        """
        for file_batch in chunks(files, 5):
            with psycopg2.connect(**conn_config) as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, sql, [file.file() for file in file_batch])
                    insert_ids = cursor.fetchall()
                    for bundle in zip(file_batch, list(insert_ids)):
                        bundle[0].file_id = bundle[1][0]
        return files

    def link_new_files(self, files: List[NewUpload]):
        """
        Given a list of Files that have file_id and item_id, create new ls and docass entries
        """
        sql_docass = """
            INSERT INTO docass (docass_source_id, docass_source_type, docass_target_id, docass_target_type, docass_purpose, docass_created)
            VALUES %s;
        """
        for file_batch in chunks(files, 5):
            with psycopg2.connect(**conn_config) as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, sql_docass, [file.docass() for file in file_batch])

    def process_new_files(self, files: List[NewUpload]):
        """
        New files that don't exist in the system. Need to create LS and Docass entries and link them
        """
        transformed_files = []
        for file in files:
            file_title = file.file_name
            file_stream = self.load_ftp_file(file_title)
            if PurePath(file_title).suffix != '.pdf':
                self.store_stream_as_file(file_title, file_stream)
                file_title, file_stream = self.transform_file(file_title)
            transformed_files.append(
                NewFile(
                    file_title=file_title,
                    file_stream=psycopg2.Binary(file_stream.read()),
                    item_id=file.item_id)
            )
        inserted_files = self.insert_new_files(transformed_files)
        self.link_new_files(inserted_files)

    def main(self):
        logging.info('Begin file sync')
        self.cleanup()
        self.get_ftp_file_names()
        items_in_db = self.get_db_item_names()

        # only work with ftp items that have db records
        ftp_items = self.filter_ftp_dir_items(items_in_db)
        self.filter_ftp_items_already_stored()
        if bool(self.file_dict) is False:
            logging.info('No files matches for upload')
            return
        files_to_update = self.files_to_be_updated()
        items_mapping = {i[1]: i[0] for i in items_in_db}
        files_to_create = self.files_not_in_system(items_mapping)
        logging.info('Files to update: {}'.format([file.file_name for file in files_to_update]))
        logging.info('Files to create: {}'.format([file.file_name for file in files_to_create]))
        self.process_updates(files_to_update)
        self.process_new_files(files_to_create)
        self.cleanup()


if __name__ == "__main__":
    process = FileSync()
    process.main()
