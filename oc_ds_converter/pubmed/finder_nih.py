#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
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

import re
from datetime import datetime
from urllib.parse import quote, unquote

from bs4 import BeautifulSoup
from oc_ds_converter.oc_idmanager.issn import ISSNManager
from oc_ds_converter.oc_idmanager.pmid import PMIDManager
from requests import get


class NIHResourceFinder():
    """This class implements an api pmid resource finder for NIH"""

    def __init__(self, data={}, use_api_service=True):
        """National Institute of Health resource finder constructor."""
        self._api = "https://pubmed.ncbi.nlm.nih.gov/"
        self._use_api_service = use_api_service
        self._p = "pmid:"
        self._data = data
        self._im = ISSNManager()
        self._pm = PMIDManager()
        self._headers = {
            "User-Agent": "ResourceFinder / OpenCitations Indexes "
            "(http://opencitations.net; mailto:contact@opencitations.net)"
        }
        self._issn_regex = r"(?<=^IS\s{2}-\s)[0-9]{4}-[0-9]{3}[0-9X]"
        self._jtitle_regex = r"(?<=^JT\s{2}-\s)(.*)(?<!\n)$"
        self._date_regex = r"DP\s+-\s+(\d{4}(\s?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))?(\s?((3[0-1])|([1-2][0-9])|([0]?[1-9])))?)"
        super(NIHResourceFinder, self).__init__()

    def _get_issn(self, txt_obj):
        result = set()
        fa_issn = re.finditer(self._issn_regex, txt_obj, re.MULTILINE)
        for matchNum_issn, match_issn in enumerate(fa_issn, start=1):
            m_issn = match_issn.group()
            if m_issn:
                norm_issn = self._im.normalise(m_issn, include_prefix=True)
                if norm_issn is not None:
                    result.add(norm_issn)
        return result

    def _get_extended_j_title(self, txt_obj):
        matches = re.finditer(self._jtitle_regex, txt_obj, re.MULTILINE)
        for matchNum, match in enumerate(matches, start=1):
            m = match.group()
            if m:
                m = unquote(m.strip())
                return m

    def _get_date(self, txt_obj):
        pmid_date = None
        date = re.search(self._date_regex,
                         txt_obj,
                         re.IGNORECASE,
                         ).group(1)
        re_search = re.search(
            r"(\d{4})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+((3[0-1])|([1-2][0-9])|([0]?[1-9]))",
            date,
            re.IGNORECASE,
        )
        if re_search is not None:
            src = re_search.group(0)
            datetime_object = datetime.strptime(src, "%Y %b %d")
            pmid_date = datetime.strftime(datetime_object, "%Y-%m-%d")
        else:
            re_search = re.search(
                r"(\d{4})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
                date,
                re.IGNORECASE,
            )
            if re_search is not None:
                src = re_search.group(0)
                datetime_object = datetime.strptime(src, "%Y %b")
                pmid_date = datetime.strftime(datetime_object, "%Y-%m")
            else:
                re_search = re.search(r"(\d{4})", date)
                if re_search is not None:
                    src = re.search(r"(\d{4})", date).group(0)
                    datetime_object = datetime.strptime(src, "%Y")
                    pmid_date = datetime.strftime(datetime_object, "%Y")
        return pmid_date


    def _call_api(self, pmid_full):
        if self._use_api_service:
            pmid = self._pm.normalise(pmid_full)
            r = get(
                self._api + quote(pmid) + "/?format=pubmed",
                headers=self._headers,
                timeout=30,
            )
            if r.status_code == 200:
                r.encoding = "utf-8"
                soup = BeautifulSoup(r.text, features="lxml")
                mdata = str(soup.find(id="article-details"))
                return mdata
