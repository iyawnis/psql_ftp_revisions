import unittest
from unittest.mock import patch, MagicMock, mock_open, call
from ftp_db_sync import FileSync, VersionUpdate, NewUpload, is_updated_version, File, NewFile
from io import BytesIO
import psycopg2

class TestCase(unittest.TestCase):

    def test_filter_ftp_dir_items_file_exists_in_db(self):
        sync = FileSync()
        db_names = [('1', 'item1')]
        sync.file_dict = {'item1': 'item1_1.txt'}
        self.assertEqual(list(sync.filter_ftp_dir_items(db_names)), ['item1'])

    def test_is_updated_version(self):
        # is_updated_version db_file ftp_file
        self.assertTrue(is_updated_version('t_1 some.txt', 't_2 some.txt'))
        self.assertTrue(is_updated_version('t_a.txt', 't_B.txt'))
        self.assertTrue(is_updated_version('t_z.txt', 't_1.txt'))
        self.assertTrue(is_updated_version('t_z 2.txt', 't_1 a.txt'))

        self.assertFalse(is_updated_version('t_1.txt', 't_1.txt'))
        self.assertFalse(is_updated_version('t_2.txt', 't_1.txt'))
        self.assertFalse(is_updated_version('t_b.txt', 't_a.txt'))
        self.assertFalse(is_updated_version('t_1.txt', 't_z.txt'))

    def test_filter_ftp_dir_items_file_not_exists_in_db(self):
        sync = FileSync()
        db_names = [('1', 'item2')]
        sync.file_dict = {'item1': 'item1_1.txt'}
        self.assertEqual(list(sync.filter_ftp_dir_items(db_names)), [])

    @patch.object(FileSync, 'execute_sql')
    def test_filter_ftp_items_already_stored_match(self, mock_sql):
        mock_sql.return_value = set([('item1_1.pdf',)])
        sync = FileSync()
        sync.file_dict = {
            'item1': 'item1_1.txt'
        }
        sync.filter_ftp_items_already_stored()
        mock_sql.assert_called_with(
            '\n            SELECT file.file_title FROM file WHERE file_title in %s;\n        ',
            ('item1_1.pdf',)
        )
        self.assertEqual(sync.file_dict, {})

    @patch.object(FileSync, 'execute_sql')
    def test_filter_ftp_items_already_stored_no_match(self, mock_sql):
        mock_sql.return_value = set([])
        sync = FileSync()
        sync.file_dict = {
            'item1': 'item1_1.txt'
        }
        sync.filter_ftp_items_already_stored()
        self.assertEqual(sync.file_dict, {'item1': 'item1_1.txt'})
        mock_sql.assert_called_with(
            '\n            SELECT file.file_title FROM file WHERE file_title in %s;\n        ',
            ('item1_1.pdf',)
        )

    @patch.object(FileSync, 'execute_sql')
    def test_files_to_be_updated_query(self, mock_sql):
        mock_sql.return_value = [('1', 'item1_1.sql',)]
        sync = FileSync()

        sync.file_dict = {
            'item1': 'item1_2.txt',
            'item2': 'item2_2.txt'
        }
        sync.files_to_be_updated()
        mock_sql.assert_called_with(

            '\n            SELECT file.file_id, file.file_title FROM file WHERE file_title SIMILAR TO %s;\n        ',
            ('(item1|item2)%')
        )

    @patch.object(FileSync, 'execute_sql')
    def test_files_to_be_updated_files_to_upload(self, mock_sql):
        mock_sql.return_value = [('1', 'item1_1.sql',), ]
        sync = FileSync()

        sync.file_dict = {
            'item1': 'item1_2.txt',
            'item2': 'item2_2.txt'
        }
        files_to_upload = sync.files_to_be_updated()
        self.assertEqual(
            files_to_upload[0]._asdict(),
            {'item_number': 'item1', 'file_id':'1', 'file_name':'item1_2.txt'})

    def test_files_not_in_system_filter(self):
        sync = FileSync()
        items_dict = {
            'item4': '15',
            'item5': '16'
        }
        sync.file_dict = {
            'item4': 'item4_2.txt',
            'item5': 'item5_2.txt',
        }
        results = [res._asdict() for res in sync.files_not_in_system(items_dict)]
        self.assertEqual(
            results,
            [
                {'item_number': 'item4', 'file_name': 'item4_2.txt', 'item_id': '15'},
                {'item_number': 'item5', 'file_name': 'item5_2.txt', 'item_id': '16'},
            ])
    @patch.object(FileSync, 'update_existing_files')
    @patch.object(FileSync, 'transform_file')
    @patch.object(FileSync, 'load_ftp_file')
    @patch.object(FileSync, 'store_stream_as_file')
    def test_process_updates_txt(self, mock_store, mock_load, mock_transform, mock_update):
        empty_stream = BytesIO(b'')
        mock_transform.return_value = '/temp_files/file.pdf', empty_stream
        mock_load.return_value = empty_stream
        files = [
            VersionUpdate(file_id='1', file_name='file.txt', item_number='1')]
        sync = FileSync()
        sync.process_updates(files)
        mock_load.assert_called_with('file.txt')
        mock_store.assert_called_with('file.txt', empty_stream)
        mock_transform.assert_called_with('file.txt')

    @patch.object(FileSync, 'update_existing_files')
    @patch.object(FileSync, 'load_ftp_file')
    @patch.object(FileSync, 'store_stream_as_file')
    def test_process_updates_pdf(self, mock_store, mock_load, mock_update):
        empty_stream = BytesIO(b'abc')
        mock_load.return_value = empty_stream
        files = [
            VersionUpdate(file_id='1', file_name='file.pdf', item_number='1')]
        sync = FileSync()
        sync.process_updates(files)
        mock_load.assert_called_with('file.pdf')
        mock_store.assert_not_called()

    @patch('ftp_db_sync.psycopg2')
    @patch('ftp_db_sync.execute_values')
    def test_update_existing_files(self, mock_extras,  mock_psycopg2):
        mock_psycopg2.cursor.return_value.execute.fetch_all = []
        files = [
            File(file_id='1', file_title='hello.txt', file_stream=BytesIO(b'onetwothree')),
            File(file_id='1', file_title='hello.txt', file_stream=BytesIO(b'onetwothree')),
            File(file_id='1', file_title='hello.txt', file_stream=BytesIO(b'onetwothree')),
            File(file_id='1', file_title='hello.txt', file_stream=BytesIO(b'onetwothree')),
            File(file_id='1', file_title='hello.txt', file_stream=BytesIO(b'onetwothree')),
            File(file_id='1', file_title='hello.txt', file_stream=BytesIO(b'onetwothree')),
        ]
        sync = FileSync()
        sync.update_existing_files(files)
        self.assertEqual(mock_extras.call_count, 2)

    @patch('ftp_db_sync.psycopg2')
    @patch('ftp_db_sync.execute_values')
    def test_insert_new_files(self, mock_extras,  mock_psycopg2):
        expected = ['10', '2', '3', '4', '5']
        mock_psycopg2.connect().__enter__().cursor().__enter__().fetchall.return_value = [('10',), ('2',), ('3',), ('4',), ('5',)]

        files = [
            NewFile(item_id='1', file_title='one', file_stream=psycopg2.Binary(b'123123')),
            NewFile(item_id='2', file_title='one', file_stream=psycopg2.Binary(b'123123')),
            NewFile(item_id='3', file_title='one', file_stream=psycopg2.Binary(b'123123')),
            NewFile(item_id='4', file_title='one', file_stream=psycopg2.Binary(b'123123')),
            NewFile(item_id='5', file_title='one', file_stream=psycopg2.Binary(b'123123')),
        ]
        sync = FileSync()
        files = sync.insert_new_files(files)
        for bundle in zip(files, expected):
            self.assertEqual(bundle[0].file_id, bundle[1])

    @patch('ftp_db_sync.open', new_callable=mock_open, read_data=b"data")
    @patch('ftp_db_sync.subprocess.call')
    def test_transform_function(self, mock_subprocess, mock_open):
        mock_subprocess.call.return_value = None
        expected_call_one = ['lowriter', '--convert-to', 'pdf:writer_pdf_Export', 'temp_files/file.txt', '--outdir', 'temp_files']
        expected_call_two = ['ps2pdf', '-dPDFSETTINGS=/ebook', 'temp_files/file.pdf', 'temp_files/file.pdf']
        expected_calls = [call(expected_call_one), call(expected_call_two)]
        sync = FileSync()
        sync.transform_file('file.txt')
        self.assertEqual(mock_subprocess.call_count, 2)

        self.assertTrue(mock_subprocess.call_args_list[0], expected_call_one)
        mock_subprocess.assert_has_calls(expected_calls)
    def test_file_description(self):
        file = NewFile(file_title='PHKIT_3 some file description.pdf', item_id='1', file_stream='')
        self.assertEqual(file.get_description(), 'some file description')

        file = NewFile(file_title='PHKIT_3.pdf', item_id='1', file_stream='')
        self.assertEqual(file.get_description(), 'PHKIT_3')
if __name__ == '__main__':
    unittest.main()