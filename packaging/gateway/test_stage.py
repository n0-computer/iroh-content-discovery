import json
from pathlib import Path
import tempfile
import unittest
import zipfile
from stage import extensions, FILES

class ExtensionFiles(unittest.TestCase):
    def test_browser_manifests_and_unsigned_package(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            extensions(root)
            chrome = json.loads((root / 'chrome/manifest.json').read_text())
            firefox = json.loads((root / 'firefox/manifest.json').read_text())
            self.assertNotIn('scripts', chrome['background'])
            self.assertNotIn('browser_specific_settings', chrome)
            self.assertNotIn('service_worker', firefox['background'])
            self.assertIn('scripts', firefox['background'])
            self.assertIn('id', firefox['browser_specific_settings']['gecko'])
            with zipfile.ZipFile(root / 'iroh-link-firefox-unsigned.xpi') as archive:
                self.assertEqual(set(archive.namelist()), set(FILES + ['manifest.json']))
                self.assertEqual(json.loads(archive.read('manifest.json')), firefox)
            self.assertTrue((root / 'Install extensions.html').is_file())
