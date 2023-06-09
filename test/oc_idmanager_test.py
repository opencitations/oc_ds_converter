#!python
# Copyright 2019, Silvio Peroni <essepuntato@gmail.com>
# Copyright 2022, Giuseppe Grieco <giuseppe.grieco3@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>, Elia Rizzetto <elia.rizzetto@studio.unibo.it>, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import json
import unittest
from os import makedirs
from os.path import exists, join

from oc_ds_converter.oc_idmanager import *
from oc_ds_converter.oc_idmanager.jid import JIDManager
from oc_ds_converter.oc_idmanager.url import URLManager


class IdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")

        test_dir = join("test", "data")
        with open(join(test_dir, "glob.json"), encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_wikidata_1 = "Q34433"
        self.valid_wikidata_2 = "Q24698708"
        self.valid_wikidata_3 = "Q15767074"
        self.invalid_wikidata_1 = "Q34433Q345"
        self.invalid_wikidata_3 = "Q12"  # not existing yet

        self.valid_wikipedia_1 = "30456"
        self.valid_wikipedia_2 = "43744177" # category page
        self.invalid_wikipedia_1 = "01267548"
        self.invalid_wikipedia_2 = "Berlin_Wall"

        self.valid_url_1 = "https://datacite.org/"
        self.valid_url_2 = "opencitations.net"
        self.valid_url_3 = "https://www.nih.gov/"
        self.valid_url_4 = "https://it.wikipedia.org/wiki/Muro di Berlino"
        self.invalid_url_1 = "https://www.nih.gov/invalid_url"
        self.invalid_url_2 = "opencitations.net/not a real page .org"  # not existing yet

        self.valid_ror_1 = "https://ror.org/040jc3p57"
        self.valid_ror_2 = "01111rn36"
        self.invalid_ror_1 = "la673822"
        self.invalid_ror_2 = ".org/560jc3p57"

    def test_ror_normalise(self):
        rm = RORManager()
        self.assertEqual(
            rm.normalise(self.valid_ror_1),
            rm.normalise(self.valid_ror_1.replace("https://", "")),
        )
        self.assertEqual(
            rm.normalise(self.valid_ror_2),
            rm.normalise("https://ror.org/" + self.valid_ror_2),
        )

    def test_ror_is_valid(self):
        rm = RORManager()
        self.assertTrue(rm.is_valid(self.valid_ror_1))
        self.assertTrue(rm.is_valid(self.valid_ror_2))
        self.assertFalse(rm.is_valid(self.invalid_ror_1))
        self.assertFalse(rm.is_valid(self.invalid_ror_2))

    def test_url_normalise(self):
        um = URLManager()
        self.assertEqual(
            um.normalise(self.valid_url_1),
            um.normalise(self.valid_url_1.replace("https://.", "https://www.")),
        )
        self.assertEqual(
            um.normalise(self.valid_url_2),
            um.normalise("https://" + self.valid_url_2),
        )
        self.assertEqual(
            um.normalise(self.valid_url_3),
            um.normalise(
                self.valid_url_3.replace("https://www.", "https://")
            ),
        )

    def test_url_valid(self):
        um_nofile = URLManager()
        self.assertTrue(um_nofile.is_valid(self.valid_url_1))
        self.assertTrue(um_nofile.is_valid(self.valid_url_2))
        self.assertTrue(um_nofile.is_valid(self.valid_url_3))
        self.assertTrue(um_nofile.is_valid(self.valid_url_4))
        self.assertFalse(um_nofile.is_valid(self.invalid_url_1))
        self.assertFalse(um_nofile.is_valid(self.invalid_url_2))

        um_file = URLManager(self.data, use_api_service=False)
        self.assertTrue(um_file.normalise(self.valid_url_1, include_prefix=True) in self.data)
        self.assertTrue(um_file.normalise(self.valid_url_2, include_prefix=True) in self.data)

        clean_data = {}

        um_nofile_noapi = URLManager(clean_data, use_api_service=False)
        self.assertTrue(um_nofile_noapi.is_valid(self.valid_url_1))
        self.assertTrue(um_nofile_noapi.is_valid(self.invalid_url_1))


    def test_wikipedia_normalise(self):
        wpm = WikipediaManager()
        self.assertTrue(
            self.valid_wikipedia_1,
            wpm.normalise("30456")
        )
        self.assertTrue(
            self.valid_wikipedia_2,
            wpm.normalise(self.valid_wikipedia_2)
        )
        self.assertTrue(
            self.valid_wikipedia_2,
            wpm.normalise("wikipedia" + self.valid_wikipedia_2)
        )

    def test_wikipedia_is_valid(self):
        wpm = WikipediaManager()
        self.assertTrue(wpm.is_valid(self.valid_wikipedia_1))
        self.assertTrue(wpm.is_valid(self.valid_wikipedia_2))
        self.assertFalse(wpm.is_valid(self.invalid_wikipedia_1))
        self.assertFalse(wpm.is_valid(self.invalid_wikipedia_2))

        wpm_file = WikipediaManager(self.data)
        self.assertTrue(wpm_file.normalise(self.valid_wikipedia_1, include_prefix=True) in self.data)
        self.assertTrue(wpm_file.normalise(self.valid_wikipedia_2, include_prefix=True) in self.data)
        self.assertTrue(wpm_file.normalise(self.invalid_wikipedia_1, include_prefix=True) in self.data)
        self.assertTrue(wpm_file.is_valid((wpm_file.normalise(self.valid_wikipedia_1, include_prefix=True))))
        self.assertTrue(wpm_file.is_valid((wpm_file.normalise(self.valid_wikipedia_2, include_prefix=True))))
        self.assertFalse(wpm_file.is_valid((wpm_file.normalise(self.invalid_wikipedia_1, include_prefix=True))))

        clean_data = {}
        wpm_nofile_noapi = WikipediaManager(clean_data, use_api_service=False)
        self.assertTrue(wpm_nofile_noapi.is_valid(self.valid_wikipedia_1))
        self.assertTrue(wpm_nofile_noapi.is_valid(self.valid_wikipedia_2))

    def test_wikidata_normalise(self):
        wdm = WikidataManager()
        self.assertTrue(
            self.valid_wikidata_1,
            wdm.normalise(self.valid_wikidata_1.replace("Q", "https://www.wikidata.org/wiki/Q"))
        )
        self.assertTrue(
            self.valid_wikidata_2,
            wdm.normalise(self.valid_wikidata_2)
        )
        self.assertTrue(
            self.valid_wikidata_2,
            wdm.normalise(self.valid_wikidata_2.replace("Q", "wikidata: Q"))
        )
        self.assertTrue(
            self.valid_wikidata_3,
            wdm.normalise((self.valid_wikidata_3.replace("Q", "Q ")))
        )

    def test_wikidata_is_valid(self):
        wdm = WikidataManager()
        self.assertTrue(wdm.is_valid(self.valid_wikidata_1))
        self.assertTrue(wdm.is_valid(self.valid_wikidata_2))
        self.assertTrue(wdm.is_valid(self.valid_wikidata_3))
        self.assertFalse(wdm.is_valid(self.invalid_wikidata_1))
        self.assertFalse(wdm.is_valid(self.invalid_wikidata_3))

        wdm_file = WikidataManager(self.data)
        self.assertTrue(wdm_file.normalise(self.valid_wikidata_1, include_prefix=True) in self.data)
        self.assertTrue(wdm_file.normalise(self.valid_wikidata_2, include_prefix=True) in self.data)
        self.assertTrue(wdm_file.normalise(self.invalid_wikidata_3, include_prefix=True) in self.data)
        self.assertTrue(wdm_file.is_valid((wdm_file.normalise(self.valid_wikidata_1, include_prefix=True))))
        self.assertTrue(wdm_file.is_valid((wdm_file.normalise(self.valid_wikidata_2, include_prefix=True))))
        self.assertFalse(wdm_file.is_valid((wdm_file.normalise(self.invalid_wikidata_3, include_prefix=True))))

        clean_data = {}
        wdm_nofile_noapi = WikidataManager(clean_data, use_api_service=False)
        self.assertTrue(wdm_nofile_noapi.is_valid(self.valid_wikidata_1))
        self.assertTrue(wdm_nofile_noapi.is_valid(self.valid_wikidata_2))



