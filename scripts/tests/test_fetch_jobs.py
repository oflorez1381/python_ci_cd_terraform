import unittest
import toml
from scripts.fetch_jobs import read_api


class TestMainScript(unittest.TestCase):
    def test_read_api(self):
        app_config = toml.load("config/config.toml")
        url = app_config["api"]["url"]
        status_code = read_api(url).status_code
        self.assertEqual(200, status_code)

    def test_process_data(self):
        pass

    def test_upload_to_s3(self):
        pass


if __name__ == "__main__":
    unittest.main()
