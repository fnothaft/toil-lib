import os
import subprocess
import filecmp
import tempfile
import unittest
from contextlib import closing
from uuid import uuid4
from os.path import expanduser

from toil.common import Toil
from toil.job import Job


class TestUrls(unittest.TestCase):

    def setUp(self):
        # set up mktemp
        super(TestUrls, self).setUp()
        home = expanduser("~") + '/'
        self.tmpdir = tempfile.mkdtemp(prefix=home)
        self.options = Job.Runner.getDefaultOptions(os.path.join(str(self.tmpdir), 'jobstore'))
        self.options.clean = 'always'

    def tearDown(self):
        # delete temp
        super(TestUrls, self).tearDown()
        for file in os.listdir(self.tmpdir):
            os.remove(os.path.join(self.tmpdir,file))
        os.removedirs(self.tmpdir)

    def test_download_url_job(self):
        from toil_lib.urls import download_url_job
        j = Job.wrapJobFn(download_url_job, 'www.google.com')
        with Toil(self.options) as toil:
            toil.start(j)

    def test_download_url(self):
        from toil_lib.urls import download_url
        A = Job.wrapJobFn(download_url, work_dir=self.tmpdir, url='www.google.com', name='testy')
        with Toil(self.options) as toil:
            toil.start(A)
        assert os.path.exists(os.path.join(self.tmpdir, 'testy'))


    def test_upload_and_download_with_encryption(self):
        from toil_lib.urls import s3am_upload
        from toil_lib.urls import download_url
        from boto.s3.connection import S3Connection, Bucket, Key
        # Create temporary encryption key
        key_path = os.path.join(self.tmpdir, 'foo.key')
        subprocess.check_call(['dd', 'if=/dev/urandom', 'bs=1', 'count=32',
                               'of={}'.format(key_path)])
        # Create test file
        upload_fpath = os.path.join(self.tmpdir, 'upload_file')
        with open(upload_fpath, 'wb') as fout:
            fout.write(os.urandom(1024))
        # Upload file
        random_key = os.path.join('test/', str(uuid4()), 'upload_file')
        s3_url = os.path.join('s3://cgl-driver-projects/', random_key)
        try:
            s3_dir = os.path.split(s3_url)[0]
            A = Job.wrapJobFn(s3am_upload, fpath=upload_fpath, s3_dir=s3_dir, s3_key_path=key_path)
            with Toil(self.options) as toil:
                toil.start(A)
            # Download the file
            B = Job.wrapJobFn(download_url, url=s3_url, name='download_file', work_dir=self.tmpdir,
                              s3_key_path=key_path)
            with Toil(self.options) as toil:
                toil.start(B)
            download_fpath = os.path.join(self.tmpdir, 'download_file')
            assert os.path.exists(download_fpath)
            assert filecmp.cmp(upload_fpath, download_fpath)
        finally:
            # Delete the Key. Key deletion never fails so we don't need to catch any exceptions
            with closing(S3Connection()) as conn:
                b = Bucket(conn, 'cgl-driver-projects')
                k = Key(b)
                k.key = random_key
                k.delete()
