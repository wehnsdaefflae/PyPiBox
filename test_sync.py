import json
import unittest
import os
import shutil
import time
from main import DropboxSync


class TestSyncClient(unittest.TestCase):
    def setUp(self):
        config = DropboxSync.get_config("test_config.json")

        local_dropbox_mirror = "/"

        self.local_folder = config['local_folder']
        self.remote_folder = os.path.join(local_dropbox_mirror, config['dropbox_folder'])

        os.makedirs(self.local_folder)
        os.makedirs(self.remote_folder)

        self.sync_client = DropboxSync(**config)
        self.sync_client.sync()

    def tearDown(self):
        shutil.rmtree(self.local_folder)
        shutil.rmtree(self.remote_folder)

    def test_file_creation(self):
        # Local to remote
        with open(os.path.join(self.local_folder, 'test_file.txt'), 'w') as f:
            f.write('Test content')
        time.sleep(2)  # Give time for syncing
        self.assertTrue(os.path.isfile(os.path.join(self.remote_folder, 'test_file.txt')))

        # Remote to local
        with open(os.path.join(self.remote_folder, 'test_file_2.txt'), 'w') as f:
            f.write('Test content 2')
        time.sleep(2)  # Give time for syncing
        self.assertTrue(os.path.isfile(os.path.join(self.local_folder, 'test_file_2.txt')))

    def test_folder_creation(self):
        # Local to remote
        os.makedirs(os.path.join(self.local_folder, 'test_folder'))
        time.sleep(2)  # Give time for syncing
        self.assertTrue(os.path.isdir(os.path.join(self.remote_folder, 'test_folder')))

        # Remote to local
        os.makedirs(os.path.join(self.remote_folder, 'test_folder_2'))
        time.sleep(2)  # Give time for syncing
        self.assertTrue(os.path.isdir(os.path.join(self.local_folder, 'test_folder_2')))

    def test_file_modification(self):
        # Local to remote
        with open(os.path.join(self.local_folder, 'test_file.txt'), 'w') as f:
            f.write('Test content')
        time.sleep(2)  # Give time for syncing
        with open(os.path.join(self.local_folder, 'test_file.txt'), 'a') as f:
            f.write('Modified content')
        time.sleep(2)  # Give time for syncing
        with open(os.path.join(self.remote_folder, 'test_file.txt'), 'r') as f:
            self.assertEqual(f.read(), 'Test contentModified content')

        # Remote to local
        with open(os.path.join(self.remote_folder, 'test_file_2.txt'), 'w') as f:
            f.write('Test content 2')
        time.sleep(2)  # Give time for syncing
        with open(os.path.join(self.remote_folder, 'test_file_2.txt'), 'a') as f:
            f.write('Modified content 2')
        time.sleep(2)  # Give time for syncing
        with open(os.path.join(self.local_folder, 'test_file_2.txt'), 'r') as f:
            self.assertEqual(f.read(), 'Test content 2Modified content 2')

    def test_file_deletion(self):
        # Local to remote
        with open(os.path.join(self.local_folder, 'test_file.txt'), 'w') as f:
            f.write('Test content')
        time.sleep(2)  # Give time for syncing
        os.remove(os.path.join(self.local_folder, 'test_file.txt'))
        time.sleep(2)  # Give time for syncing
        self.assertFalse(os.path.isfile(os.path.join(self.remote_folder, 'test_file.txt')))

        # Remote to local
        with open(os.path.join(self.remote_folder, 'test_file_2.txt'), 'w') as f:
            f.write('Test content 2')
        time.sleep(2)  # Give time for syncing
        os.remove(os.path.join(self.remote_folder, 'test_file_2.txt'))
        time.sleep(2)  # Give time for syncing
        self.assertFalse(os.path.isfile(os.path.join(self.local_folder, 'test_file_2.txt')))

    def test_folder_deletion(self):
        # Local to remote
        os.makedirs(os.path.join(self.local_folder, 'test_folder'))
        time.sleep(2)  # Give time for syncing
        shutil.rmtree(os.path.join(self.local_folder, 'test_folder'))
        time.sleep(2)  # Give time for syncing
        self.assertFalse(os.path.isdir(os.path.join(self.remote_folder, 'test_folder')))

        # Remote to local
        os.makedirs(os.path.join(self.remote_folder, 'test_folder_2'))
        time.sleep(2)  # Give time for syncing
        shutil.rmtree(os.path.join(self.remote_folder, 'test_folder_2'))
        time.sleep(2)  # Give time for syncing
        self.assertFalse(os.path.isdir(os.path.join(self.local_folder, 'test_folder_2')))

    def test_conflict_resolution(self):
        # Create the same file both locally and remotely
        with open(os.path.join(self.local_folder, 'conflict_file.txt'), 'w') as f:
            f.write('Local content')
        with open(os.path.join(self.remote_folder, 'conflict_file.txt'), 'w') as f:
            f.write('Remote content')

        time.sleep(2)  # Give time for syncing

        # Check if both versions are preserved and conflict is resolved
        local_files = os.listdir(self.local_folder)
        remote_files = os.listdir(self.remote_folder)

        # Modify the following lines to check for the actual conflict resolution strategy implemented by the sync client
        self.assertIn('conflict_file.txt', local_files)
        self.assertIn('conflict_file_conflict.txt', local_files)  # Assuming the conflict resolution strategy creates a separate file with a suffix
        self.assertIn('conflict_file.txt', remote_files)
        self.assertIn('conflict_file_conflict.txt', remote_files)

        # Compare the content of the original and conflict files
        with open(os.path.join(self.local_folder, 'conflict_file.txt'), 'r') as f:
            local_content = f.read()
        with open(os.path.join(self.local_folder, 'conflict_file_conflict.txt'), 'r') as f:
            local_conflict_content = f.read()

        self.assertIn('Local content', (local_content, local_conflict_content))
        self.assertIn('Remote content', (local_content, local_conflict_content))


if __name__ == '__main__':
    unittest.main()

