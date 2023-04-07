import time
import unittest
import os
import shutil
from main import DropboxSync


class TestSyncClient(unittest.TestCase):
    def setUp(self):
        print("Setting up test environment...")
        config = DropboxSync.get_config("test_config.json")

        self.remote_folder = config['dropbox_folder']
        if self.remote_folder.startswith('/'):
            self.remote_folder = self.remote_folder[1:]

        self.local_folder = config['local_folder']

        os.makedirs(self.local_folder)
        os.makedirs(self.remote_folder)

        self.sync_client = DropboxSync(**config)
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)

    def tearDown(self):
        print("Tearing down test environment...")
        self.sync_client.close()

        shutil.rmtree(self.local_folder)
        shutil.rmtree(self.remote_folder)

    # @unittest.skip
    def test_file_creation(self):
        print("Testing file creation...")

        # Local to remote
        with open(os.path.join(self.local_folder, 'test_file.txt'), 'w') as f:
            f.write('Test content')

        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        self.assertTrue(os.path.isfile(os.path.join(self.remote_folder, 'test_file.txt')))

        # Remote to local
        with open(os.path.join(self.remote_folder, 'test_file_2.txt'), 'w') as f:
            f.write('Test content 2')
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        self.assertTrue(os.path.isfile(os.path.join(self.local_folder, 'test_file_2.txt')))

    # @unittest.skip
    def test_folder_creation(self):
        print("Testing folder creation...")

        # Local to remote
        os.makedirs(os.path.join(self.local_folder, 'test_folder'))
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        self.assertTrue(os.path.isdir(os.path.join(self.remote_folder, 'test_folder')))

        # Remote to local
        os.makedirs(os.path.join(self.remote_folder, 'test_folder_2'))
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        self.assertTrue(os.path.isdir(os.path.join(self.local_folder, 'test_folder_2')))

    # @unittest.skip
    def test_file_modification(self):
        print("Testing file modification...")

        # Local to remote
        with open(os.path.join(self.local_folder, 'test_file.txt'), 'w') as f:
            f.write('Test content')
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        with open(os.path.join(self.local_folder, 'test_file.txt'), 'a') as f:
            f.write('Modified content')
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        with open(os.path.join(self.remote_folder, 'test_file.txt'), 'r') as f:
            self.assertEqual(f.read(), 'Test contentModified content')

        # Remote to local
        with open(os.path.join(self.remote_folder, 'test_file_2.txt'), 'w') as f:
            f.write('Test content 2')
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        with open(os.path.join(self.remote_folder, 'test_file_2.txt'), 'a') as f:
            f.write('Modified content 2')
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        with open(os.path.join(self.local_folder, 'test_file_2.txt'), 'r') as f:
            self.assertEqual(f.read(), 'Test content 2Modified content 2')

    # @unittest.skip
    def test_file_deletion(self):
        print("Testing file deletion...")

        # Local to remote
        with open(os.path.join(self.local_folder, 'test_file.txt'), 'w') as f:
            f.write('Test content')
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        os.remove(os.path.join(self.local_folder, 'test_file.txt'))
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        self.assertFalse(os.path.isfile(os.path.join(self.remote_folder, 'test_file.txt')))

        # Remote to local
        with open(os.path.join(self.remote_folder, 'test_file_2.txt'), 'w') as f:
            f.write('Test content 2')
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        os.remove(os.path.join(self.remote_folder, 'test_file_2.txt'))
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        self.assertFalse(os.path.isfile(os.path.join(self.local_folder, 'test_file_2.txt')))

    # @unittest.skip
    def test_folder_deletion(self):
        print("Testing folder deletion...")

        # Local to remote
        os.makedirs(os.path.join(self.local_folder, 'test_folder'))
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        shutil.rmtree(os.path.join(self.local_folder, 'test_folder'))
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        self.assertFalse(os.path.isdir(os.path.join(self.remote_folder, 'test_folder')))

        # Remote to local
        os.makedirs(os.path.join(self.remote_folder, 'test_folder_2'))
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        shutil.rmtree(os.path.join(self.remote_folder, 'test_folder_2'))
        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)
        self.assertFalse(os.path.isdir(os.path.join(self.local_folder, 'test_folder_2')))

    # @unittest.skip
    def test_conflict_resolution(self):
        print("Testing conflict resolution...")

        # Create the same file both locally and remotely
        with open(os.path.join(self.local_folder, 'conflict_file.txt'), 'w') as f:
            f.write('Local content')
        with open(os.path.join(self.remote_folder, 'conflict_file.txt'), 'w') as f:
            f.write('Remote content')

        time.sleep(5)
        self.sync_client.sync()
        time.sleep(5)

        # Check if both versions are preserved and conflict is resolved
        local_files = os.listdir(self.local_folder)
        remote_files = os.listdir(self.remote_folder)

        # Modify the following lines to check for the actual conflict resolution strategy implemented by the sync client
        self.assertIn('conflict_file.txt', local_files)
        self.assertIn('conflict_file.txt', remote_files)

        # Compare the content of the original and conflict files
        with open(os.path.join(self.local_folder, 'conflict_file.txt'), 'r') as f:
            local_content = f.read()

        with open(os.path.join(self.remote_folder, 'conflict_file.txt'), 'r') as f:
            remote_content = f.read()

        self.assertIn('Remote content', local_content)
        self.assertNotIn('Local content', remote_content)


if __name__ == '__main__':
    unittest.main()

