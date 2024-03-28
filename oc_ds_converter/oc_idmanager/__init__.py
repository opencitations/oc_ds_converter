#!python
# Copyright 2019, Silvio Peroni <essepuntato@gmail.com>
# Copyright 2022-2023, Giuseppe Grieco <giuseppe.grieco3@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>, Elia Rizzetto <elia.rizzetto@studio.unibo.it>, Arcangelo Massari <arcangelo.massari@unibo.it>
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


from oc_ds_converter.oc_idmanager.base import IdentifierManager
from oc_ds_converter.oc_idmanager.doi import DOIManager
from oc_ds_converter.oc_idmanager.isbn import ISBNManager
from oc_ds_converter.oc_idmanager.issn import ISSNManager
from oc_ds_converter.oc_idmanager.orcid import ORCIDManager
from oc_ds_converter.oc_idmanager.pmcid import PMCIDManager
from oc_ds_converter.oc_idmanager.pmid import PMIDManager
from oc_ds_converter.oc_idmanager.ror import RORManager
from oc_ds_converter.oc_idmanager.url import URLManager
from oc_ds_converter.oc_idmanager.viaf import ViafManager
from oc_ds_converter.oc_idmanager.wikidata import WikidataManager
from oc_ds_converter.oc_idmanager.wikipedia import WikipediaManager
from oc_ds_converter.oc_idmanager.openalex import OpenAlexManager
from oc_ds_converter.oc_idmanager.crossref import CrossrefManager

