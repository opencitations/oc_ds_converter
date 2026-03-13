import json as json_module
import re
from urllib.parse import unquote

import pytest
import responses

from oc_ds_converter.oc_idmanager.oc_data_storage.in_memory_manager import InMemoryStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.redis_manager import RedisStorageManager
from oc_ds_converter.oc_idmanager.oc_data_storage.sqlite_manager import SqliteStorageManager


@pytest.fixture(params=["redis", "sqlite", "inmemory"])
def storage_manager(request: pytest.FixtureRequest, tmp_path):
    if request.param == "redis":
        sm = RedisStorageManager(testing=True)
    elif request.param == "sqlite":
        sm = SqliteStorageManager(str(tmp_path / "test.db"))
    else:
        sm = InMemoryStorageManager(str(tmp_path / "test.json"))
    yield sm
    sm.delete_storage()


@pytest.fixture(autouse=True)
def mock_http_requests(request):
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        _register_doi_ra_mocks(rsps)
        _register_doi_mocks(rsps)
        _register_crossref_mocks(rsps)
        _register_arxiv_mocks(rsps)
        _register_pmid_mocks(rsps)
        _register_orcid_mocks(rsps)
        _register_openalex_mocks(rsps)
        _register_pmc_mocks(rsps)
        _register_datacite_mocks(rsps)
        _register_jid_mocks(rsps)
        _register_ror_mocks(rsps)
        _register_viaf_mocks(rsps)
        _register_wikidata_mocks(rsps)
        _register_wikipedia_mocks(rsps)
        _register_medra_mocks(rsps)
        _register_url_mocks(rsps)
        yield rsps


def _register_doi_ra_mocks(rsps: responses.RequestsMock) -> None:
    def doi_ra_callback(request):
        return (200, {}, json_module.dumps([{"RA": "Crossref"}]))

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://doi\.org/ra/.*"),
        callback=doi_ra_callback,
        content_type="application/json",
    )


def _register_doi_mocks(rsps: responses.RequestsMock) -> None:
    valid_dois = {
        "10.1108/jd-12-2013-0166",
        "10.1130/2015.2513(00)",
        "10.1007/s11192-022-04367-w",
        "10.1007/s12160-011-9282-0",
        "10.5281/zenodo.4725899",
        "10.2307/3053861",
        "10.1016/0006-2944(75)90147-7",
        "10.1016/0006-291x(75)90482-9",
        "10.1016/0006-291x(75)90498-2",
        "10.1016/0006-291x(75)90506-9",
        "10.1016/0006-291x(75)90508-2",
        "10.48550/arxiv.1509.08217",
        "10.1016/0361-9230(93)90026-8",
        "10.1184/r1/12841247.v1",
        "10.1038/nature12373",
        "10.3390/ijerph15061183",
        "10.17605/osf.io/abcde",
        "10.13039/100005522",
        "10.11578/1367552",
        "10.11221/jima.51.86",
        "10.11185/imt.8.380",
        "10.11230/jsts.17.2_1",
        "10.11178/example.1",
        "10.11178/example.2",
        "10.1016/0022-0248(83)90411-6",
        "10.1016/0038-1098(88)91128-3",
        "10.11224/cleftpalate1976.23.2_83",
        "10.11178/jdsa.8.49",
        "10.11178/jdsa.5.47",
        "10.11230/jsts.17.1_11",
        "10.11450/seitaikogaku1989.10.1",
        "10.11230/jsts.18.1_34",
        "10.2494/photopolymer.12.209",
        "10.1007/bf00354389",
        "10.1007/bf00354390",
        "10.1088/0954-0083/11/1/013",
        "10.1177/0954008303015002003",
        "10.1021/ma0201779",
        "10.1246/cl.1997.333",
        "10.1126/science.2563171",
        "10.1016/0032-3861(95)90668-r",
        "10.1016/0032-3861(95)90669-s",
        "10.1088/0954-0083/11/1/008",
        "10.1016/s0032-3861(02)00362-2",
        "10.1093/comjnl/4.4.332",
        "10.2307/1252042",
        "10.1063/1.1656693",
        "10.3233/978-1-61499-672-9-227",
        "10.3233/ds-170012",
        "10.1145/3360901.3364434",
        "10.1007/978-3-030-61244-3_6",
        "10.1007/978-3-030-62466-8_28",
        "10.3897/bdj.3.e5063",
        "10.1103/physrevd.84.084046",
        "10.48550/arxiv.1107.5979",
        "10.1001/10-v4n2-hsf10003",
        "10.1001/jama.299.12.1471",
        "10.1016/s0196-0644(99)70224-6",
        "10.1056/nejmsa021807",
        "10.1089/bsp.2008.0020",
        "10.1097/01.bcr.0000155527.76205.a2",
        "10.1097/01.ccm.0000151067.76074.21",
        "10.1097/01.ccm.0000151072.17826.72",
        "10.1097/dmp.0b013e31817196bf",
        "10.1097/dmp.0b013e318194898d",
        "10.1097/dmp.0b013e31819d977c",
        "10.1097/dmp.0b013e31819f1ae2",
        "10.1177/003335490812300219",
        "10.1177/003335490912400218",
        "10.1378/chest.07-2693",
        "10.2105/ajph.2006.101626",
        "10.2105/ajph.2009.162677",
        "10.9799/ksfan.2012.25.1.069",
        "10.9799/ksfan.2012.25.1.077",
        "10.9799/ksfan.2012.25.1.083",
        "10.9799/ksfan.2012.25.1.090",
        "10.9799/ksfan.2012.25.1.099",
        "10.9799/ksfan.2012.25.1.105",
        "10.9799/ksfan.2012.25.1.116",
        "10.9799/ksfan.2012.25.1.123",
        "10.9799/ksfan.2012.25.1.132",
        "10.9799/ksfan.2012.25.1.142",
        "10.11426/nagare1970.2.3_3",
        "10.11426/nagare1970.2.4_1",
        "10.11426/nagare1970.3.3_13",
        "10.14825/kaseki.68.0_14",
        "10.14825/kaseki.68.0_18",
        "10.1017/s0022112062000762",
        "10.1295/kobunshi.16.842",
        "10.1295/kobunshi.16.921",
        "10.1002/zamm.19210010401",
        "10.1002/zamm.19210010402",
        "10.1126/science.235.4793.1156",
        "10.1098/rstb.1989.0091",
        "10.5575/geosoc.96.265",
        "10.1007/s00266-017-1063-0",
        "10.1016/s0006-3495(95)79925-8",
        "10.1016/s0306-4530(03)00098-2",
        "10.1021/bi00230a017",
        "10.1080/2162402x.2015.1114203",
        "10.1093/infdis/jir563",
        "10.1137/1.9781611973730.123",
        "10.1137/s0097539793255151",
        "10.1172/jci115925",
        "10.1186/s12891-018-2177-5",
        "10.2466/pr0.1959.5.3.355",
        "10.3171/jns.1980.53.6.0765",
        "10.3389/fnana.2012.00034",
        "10.3389/fnint.2013.00107",
        "10.4021/wjon235w",
        "10.48550/arxiv.1410.2266",
        "10.5812/aapm.19333",
        "10.1007/jhep03(2014)050",
        "10.1016/0024-3795(84)90143-5",
        "10.1093/ajcn/79.5.727",
        "10.1109/sfcs.1981.28",
        "10.1136/bjo.74.3.130-a",
        "10.1186/1477-7827-12-11",
        "10.1248/cpb.50.1128",
        "10.1371/journal.pone.0060793",
        "10.17511/jooo.2020.i06.02",
        "10.18632/oncotarget.8868",
        "10.2527/1995.7392834x",
        "10.3390/ijms19082345",
        "10.4103/abr.abr_141_17",
        "10.48550/arxiv.1312.6380",
        "10.48550/arxiv.1712.08660",
        "10.7557/5.5607",
        "10.48550/arxiv.1107.5979",
        "10.5100/jje.33.1",
        "10.11575/jet.v46i3.52198",
        "10.11578/1367548",
        "10.11578/1372474",
        "10.11578/1480643",
        "10.11578/dc.20191106.1",
        "10.15407/scin11.06.057",
        "10.1021/acs.jpclett.7b01097",
        "10.1063/1.4973421",
        "10.1371/journal.pone.0284601",
        "10.1007/978-3-030-00668-6_8",
        "10.1234/null-date",
        "10.1234/empty-date",
        "10.1234/no-dateparts",
        "10.1234/html-title",
        "10.1234/with-editor",
        "10.1001/test.12345",
        # DataCite test DOIs
        "10.1016/j.archger.2019.103975",
        "10.1016/j.archger.2020.104228",
        "10.1016/j.clinbiomech.2022.105711",
        "10.1016/j.humov.2013.06.005",
        "10.1016/j.humov.2020.102588",
        "10.1016/j.jbiomech.2014.07.010",
        "10.1016/j.jbmt.2023.04.020",
        "10.1021/acsami.3c04897",
        "10.1080/00222895.2014.916651",
        "10.1080/03091902.2022.2043947",
        "10.1080/10749357.2022.2130620",
        "10.1111/sms.12847",
        "10.12678/1089-313x.21.4.151",
        "10.1590/s0103-51502010000200003",
        "10.2174/1871527315666151111120403",
        # NOTE: 10.46979/rbn.v52i4.5546 is intentionally invalid for datacite tests
        "10.5007/1980-0037.2014v16n3p287",
        "10.5258/soton/d2733",
        "10.5281/zenodo.8210025",
        "10.5281/zenodo.8232826",
        "10.5281/zenodo.8233112",
        "10.5281/zenodo.8233113",
        "10.5281/zenodo.8265216",
        "10.5281/zenodo.8265217",
        "10.6061/clinics/2013(11)07",
        "10.5281/zenodo.8249952",
        "10.5281/zenodo.8249970",
        "10.1017/9781009157896",
        "10.1017/9781009157896.005",
    }

    def doi_callback(request):
        url_path = request.url.replace("https://doi.org/api/handles/", "")
        doi = unquote(url_path).lower()
        if doi in valid_dois:
            return (200, {}, json_module.dumps({"responseCode": 1, "handle": doi}))
        return (404, {}, json_module.dumps({"responseCode": 100}))

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://doi\.org/api/handles/.*"),
        callback=doi_callback,
        content_type="application/json",
    )


def _register_crossref_mocks(rsps: responses.RequestsMock) -> None:
    valid_members = {
        "297": {"id": 297, "primary-name": "IWA Publishing"},
        "4443": {"id": 4443, "primary-name": "Example Publisher"},
        "78": {"id": 78, "primary-name": "Elsevier BV"},
        "311": {"id": 311, "primary-name": "MDPI"},
    }

    def crossref_members_callback(request):
        member_id = request.url.split("/")[-1]
        if member_id in valid_members:
            return (
                200,
                {},
                json_module.dumps({"status": "ok", "message": valid_members[member_id]}),
            )
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://api\.crossref\.org/members/.*"),
        callback=crossref_members_callback,
        content_type="application/json",
    )

    crossref_works_responses = {
        "10.1038/nature12373": {
            "DOI": "10.1038/nature12373",
            "type": "journal-article",
            "title": ["Nanometre-scale thermometry in a living cell"],
            "author": [
                {"given": "G.", "family": "Kucsko", "sequence": "first"},
                {"given": "P. C.", "family": "Maurer", "sequence": "additional"},
                {"given": "M. D.", "family": "Lukin", "sequence": "additional"}
            ],
            "container-title": ["Nature"],
            "volume": "500",
            "issue": "7460",
            "page": "54-58",
            "issued": {"date-parts": [[2013, 7, 31]]},
            "ISSN": ["0028-0836", "1476-4687"],
            "publisher": "Springer Science and Business Media LLC",
            "member": "297",
            "prefix": "10.1038"
        },
        "10.1371/journal.pone.0284601": {
            "DOI": "10.1371/journal.pone.0284601",
            "type": "journal-article",
            "title": ["Biochemical evaluation of vaccination in rats"],
            "author": [
                {"given": "Mahsa", "family": "Teymoorzadeh", "sequence": "first"},
                {"given": "Razieh", "family": "Yazdanparast", "sequence": "additional",
                 "ORCID": "https://orcid.org/0000-0003-0530-4305", "authenticated-orcid": True}
            ],
            "container-title": ["PLOS ONE"],
            "volume": "18",
            "issue": "5",
            "page": "e0284601",
            "issued": {"date-parts": [[2023, 5, 4]]},
            "ISSN": ["1932-6203"],
            "publisher": "Public Library of Science (PLoS)"
        },
        "10.1007/978-3-030-00668-6_8": {
            "DOI": "10.1007/978-3-030-00668-6_8",
            "type": "book-chapter",
            "title": ["The SPAR Ontologies"],
            "author": [
                {"given": "Silvio", "family": "Peroni", "sequence": "first"},
                {"given": "David", "family": "Shotton", "sequence": "additional"}
            ],
            "container-title": ["Lecture Notes in Computer Science", "The Semantic Web – ISWC 2018"],
            "page": "119-136",
            "issued": {"date-parts": [[2018]]},
            "ISBN": ["9783030006679", "9783030006686"],
            "publisher": "Springer International Publishing"
        },
        "10.1234/null-date": {
            "DOI": "10.1234/null-date",
            "type": "journal-article",
            "title": ["Article with null date"],
            "issued": {"date-parts": [[None]]}
        },
        "10.1234/empty-date": {
            "DOI": "10.1234/empty-date",
            "type": "journal-article",
            "title": ["Article with empty date-parts"],
            "issued": {"date-parts": [[]]}
        },
        "10.1234/no-dateparts": {
            "DOI": "10.1234/no-dateparts",
            "type": "journal-article",
            "title": ["Article without date-parts key"],
            "issued": {}
        },
        "10.1234/html-title": {
            "DOI": "10.1234/html-title",
            "type": "journal-article",
            "title": ["A study of <i>Escherichia coli</i> in <b>biofilms</b>"],
            "issued": {"date-parts": [[2024, 1, 15]]}
        },
        "10.1234/with-editor": {
            "DOI": "10.1234/with-editor",
            "type": "edited-book",
            "title": ["Edited volume test"],
            "author": [{"given": "John", "family": "Doe", "sequence": "first"}],
            "editor": [{"given": "Jane", "family": "Smith", "sequence": "first"}],
            "issued": {"date-parts": [[2024, 6, 20]]}
        },
    }

    def crossref_works_callback(request):
        doi = unquote(request.url.split("/works/")[-1]).lower()
        if doi in crossref_works_responses:
            return (200, {}, json_module.dumps({"status": "ok", "message": crossref_works_responses[doi]}))
        return (200, {}, json_module.dumps({"status": "ok", "message": {"DOI": doi}}))

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://api\.crossref\.org/works/.*"),
        callback=crossref_works_callback,
        content_type="application/json",
    )

    prefix_to_publisher = {
        "10.1016": ("Elsevier BV", "78"),
        "10.1136": ("BMJ", "239"),
        "10.1021": ("American Chemical Society (ACS)", "316"),
        "10.1007": ("Springer Science and Business Media LLC", "297"),
        "10.1097": ("Ovid Technologies (Wolters Kluwer Health)", "276"),
        "10.1378": ("Elsevier BV", "78"),
        "10.1055": ("Georg Thieme Verlag KG", "194"),
        "10.1080": ("Informa UK Limited", "301"),
        "10.1210": ("The Endocrine Society", "13"),
        "10.1159": ("S. Karger AG", "32"),
        "10.3233": ("IOS Press", "2428"),
    }

    def crossref_prefixes_callback(request):
        prefix = request.url.split("/")[-1]
        if prefix in prefix_to_publisher:
            name, member_id = prefix_to_publisher[prefix]
            return (
                200,
                {},
                json_module.dumps({
                    "status": "ok",
                    "message": {
                        "prefix": prefix,
                        "name": name,
                        "member": f"http://id.crossref.org/member/{member_id}"
                    }
                }),
            )
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://api\.crossref\.org/prefixes/.*"),
        callback=crossref_prefixes_callback,
        content_type="application/json",
    )


def _register_arxiv_mocks(rsps: responses.RequestsMock) -> None:
    valid_arxiv_xml = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
    <opensearch:totalResults>1</opensearch:totalResults>
    <entry>
        <id>http://arxiv.org/abs/{arxiv_id}</id>
        <title>Example Paper Title</title>
    </entry>
</feed>"""

    invalid_arxiv_xml = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
    <opensearch:totalResults>0</opensearch:totalResults>
</feed>"""

    valid_ids = {"2109.05583", "2109.05582", "1509.08217", "1107.5979", "1410.2266", "1712.08660", "1312.6380"}
    valid_versions = {"2109.05583v2", "1509.08217v1", "1107.5979v1", "1410.2266v1", "1712.08660v1", "1312.6380v1"}

    def arxiv_api_callback(request):
        url = request.url
        if "search_query=all:" in url:
            arxiv_id = url.split("search_query=all:")[-1]
            arxiv_id = unquote(arxiv_id)
            if arxiv_id in valid_ids:
                return (200, {}, valid_arxiv_xml.format(arxiv_id=arxiv_id))
            return (200, {}, invalid_arxiv_xml)
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://export\.arxiv\.org/api/query.*"),
        callback=arxiv_api_callback,
        content_type="application/xml",
    )

    def arxiv_abs_callback(request):
        url = request.url
        arxiv_id = url.split("/abs/")[-1]
        if arxiv_id in valid_versions:
            return (200, {}, "<html><body>ArXiv paper</body></html>")
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://arxiv\.org/abs/.*"),
        callback=arxiv_abs_callback,
        content_type="text/html",
    )


def _register_pmid_mocks(rsps: responses.RequestsMock) -> None:
    valid_pmid_html = """<html><body>
<div id="article-details">
PMID- {pmid}
TI  - Brevetoxin depresses synaptic transmission in guinea pig hippocampal slices.
FAU - Adler, M
FAU - Sheridan, R E
FAU - Apland, J P
DP  - 1993
TA  - Brain research bulletin
VI  - 31
IP  - 1-2
PG  - 201-7
IS  - 0361-9230
AID - 10.1016/0361-9230(93)90026-8 [doi]
</div></body></html>"""

    valid_pmids = {
        "2942070", "1509982", "8384044", "6716460",
        "20662931", "12345", "5", "1", "2", "3", "4",
        "24484640", "21887584", "29890726", "23483834", "2938",
        "1056020", "4882249", "14834145", "5509841", "5059118",
        "118", "120", "351", "352", "353", "324",
        "1387883", "19934524", "20879334", "21949042", "22952459",
        "23199016", "25340169", "27429628", "27467927", "29147199",
        "30064399", "8527666", "8946776", "11169242", "15113710",
        "1581770", "16277675", "2322507", "23577161", "24632350",
        "29416127", "29456979", "30096915", "7441336", "28383409",
        "8582874", "9488524", "12192153", "12892987", "21533295",
        "24363676", "24409128", "24479789", "25893183", "27105522", "29305643",
    }

    def pmid_callback(request):
        url = request.url
        match = re.search(r"/(\d+)/", url)
        if match:
            pmid = match.group(1)
            if pmid in valid_pmids:
                return (200, {}, valid_pmid_html.format(pmid=pmid))
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://pubmed\.ncbi\.nlm\.nih\.gov/.*"),
        callback=pmid_callback,
        content_type="text/html",
    )


def _register_orcid_mocks(rsps: responses.RequestsMock) -> None:
    specific_orcids = {
        "0000-0001-5506-523X": {
            "orcid-identifier": {"path": "0000-0001-5506-523X"},
            "person": {
                "name": {"family-name": {"value": "Shotton"}, "given-names": {"value": "David"}},
                "emails": {"email": []},
                "external-identifiers": {"external-identifier": []},
            },
            "history": {
                "submission-date": {"value": 1351641600000},
                "last-modified-date": {"value": 1700000000000},
            },
        },
    }
    generic_valid_orcids = {
        "0000-0003-0530-4305",
        "0000-0001-8513-8700", "0000-0002-9286-2630",
        "0000-0001-9597-7030", "0000-0002-8210-7076",
        "0000-0001-9759-3938", "0000-0003-4082-1500",
        "0000-0002-6227-4053", "0000-0002-0861-0511",
        "0000-0002-2149-4113", "0000-0001-6946-5074",
        "0000-0001-5096-5538", "0000-0001-5115-3099",
        "0000-0001-6174-9013", "0000-0001-6293-0113",
        "0000-0001-6419-1668", "0000-0001-9831-6254",
        "0000-0002-1844-174x", "0000-0002-5266-9991",
        "0000-0002-6396-4603", "0000-0003-0585-8228",
        "0000-0003-2921-5606", "0000-0003-4409-2261",
        "0000-0001-8128-1032", "0000-0002-0743-7905",
        "0000-0002-4847-4163", "0000-0002-8454-1159",
        "0000-0003-1094-3363", "0000-0003-1223-5934",
        "0000-0003-2712-1825",
        "0000-0001-6147-9981", "0000-0001-7392-1415",
        "0000-0001-7543-3466", "0000-0001-9940-8409",
        "0000-0002-0801-0890", "0000-0002-3019-5377",
        "0000-0002-5870-1542", "0000-0002-6210-8370",
        "0000-0002-6715-3533", "0000-0002-8013-9947",
        "0000-0002-9747-4928", "0000-0003-0621-9209",
        "0000-0003-1445-0291", "0000-0003-2185-3267",
        "0000-0003-2328-5769", "0000-0003-2713-8387",
        "0000-0003-4149-9760",
    }
    base_orcid_data = {
        "person": {
            "name": {"family-name": {"value": "Test"}, "given-names": {"value": "User"}},
            "emails": {"email": []},
            "external-identifiers": {"external-identifier": []},
        },
        "history": {
            "submission-date": {"value": 1351641600000},
            "last-modified-date": {"value": 1700000000000},
        },
    }

    def orcid_callback(request):
        orcid = request.url.split("/")[-1]
        if orcid in specific_orcids:
            return (200, {}, json_module.dumps(specific_orcids[orcid]))
        if orcid in generic_valid_orcids:
            data = dict(base_orcid_data)
            data["orcid-identifier"] = {"path": orcid}
            return (200, {}, json_module.dumps(data))
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://pub\.orcid\.org/v3\.0/.*"),
        callback=orcid_callback,
        content_type="application/json",
    )


def _register_openalex_mocks(rsps: responses.RequestsMock) -> None:
    valid_works = {
        "W2013228336": {"id": "https://openalex.org/W2013228336", "title": "Example"},
        "W748315831": {"id": "https://openalex.org/W748315831", "title": "Example 2"},
    }
    valid_sources = {
        "S4210229581": {
            "id": "https://openalex.org/S4210229581",
            "display_name": "Example Source",
        },
    }

    def openalex_callback(request):
        url = request.url
        for work_id, data in valid_works.items():
            if work_id in url:
                return (200, {}, json_module.dumps(data))
        for source_id, data in valid_sources.items():
            if source_id in url:
                return (200, {}, json_module.dumps(data))
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://api\.openalex\.org/.*"),
        callback=openalex_callback,
        content_type="application/json",
    )


def _register_pmc_mocks(rsps: responses.RequestsMock) -> None:
    valid_pmcs = {
        "PMC8384044": {
            "records": [
                {"pmcid": "PMC8384044", "pmid": "34356789", "doi": "10.1234/example1"}
            ]
        },
        "PMC6716460": {
            "records": [
                {"pmcid": "PMC6716460", "pmid": "31456789", "doi": "10.1234/example2"}
            ]
        },
        "PMC3928621": {
            "records": [
                {"pmcid": "PMC3928621", "pmid": "24484640", "doi": "10.2307/3053861"}
            ]
        },
        "PMC5555555": {
            "records": [
                {"pmcid": "PMC5555555", "pmid": "12345678", "doi": "10.1234/example3"}
            ]
        },
        "PMC2873764": {
            "records": [
                {"pmcid": "PMC2873764", "pmid": "20662931", "doi": "10.1186/1471-2164-11-245"}
            ]
        },
        "PMC1236277": {"records": [{"pmcid": "PMC1236277", "pmid": "16277675"}]},
        "PMC329969": {"records": [{"pmcid": "PMC329969", "pmid": "1387883"}]},
        "PMC3429885": {"records": [{"pmcid": "PMC3429885", "pmid": "22952459"}]},
        "PMC4206050": {"records": [{"pmcid": "PMC4206050", "pmid": "25340169"}]},
        "PMC4634289": {"records": [{"pmcid": "PMC4634289", "pmid": "29147199"}]},
        "PMC4910730": {"records": [{"pmcid": "PMC4910730", "pmid": "27467927"}]},
        "PMC4933747": {"records": [{"pmcid": "PMC4933747", "pmid": "27429628"}]},
        "PMC5649856": {"records": [{"pmcid": "PMC5649856", "pmid": "29147199"}]},
        "PMC6069818": {"records": [{"pmcid": "PMC6069818", "pmid": "30064399"}]},
        "PMC1042030": {"records": [{"pmcid": "PMC1042030", "pmid": "1581770"}]},
        "PMC1042034": {"records": [{"pmcid": "PMC1042034", "pmid": "2322507"}]},
        "PMC1297570": {"records": [{"pmcid": "PMC1297570", "pmid": "16277675"}]},
        "PMC3618337": {"records": [{"pmcid": "PMC3618337", "pmid": "23577161"}]},
        "PMC4005913": {"records": [{"pmcid": "PMC4005913", "pmid": "24632350"}]},
        "PMC5777326": {"records": [{"pmcid": "PMC5777326", "pmid": "29416127"}]},
        "PMC5812102": {"records": [{"pmcid": "PMC5812102", "pmid": "29456979"}]},
        "PMC6025535": {"records": [{"pmcid": "PMC6025535", "pmid": "29890726"}]},
        "PMC6121471": {"records": [{"pmcid": "PMC6121471", "pmid": "30096915"}]},
        "PMC5411193": {"records": [{"pmcid": "PMC5411193", "pmid": "28383409"}]},
        "PMC1191243": {"records": [{"pmcid": "PMC1191243", "pmid": "16277675"}]},
        "PMC3869588": {"records": [{"pmcid": "PMC3869588", "pmid": "24363676"}]},
        "PMC3885986": {"records": [{"pmcid": "PMC3885986", "pmid": "24409128"}]},
        "PMC3922134": {"records": [{"pmcid": "PMC3922134", "pmid": "24479789"}]},
        "PMC4377162": {"records": [{"pmcid": "PMC4377162", "pmid": "25893183"}]},
        "PMC5058700": {"records": [{"pmcid": "PMC5058700", "pmid": "27105522"}]},
        "PMC5840246": {"records": [{"pmcid": "PMC5840246", "pmid": "29305643"}]},
    }

    def pmc_callback(request):
        url = request.url
        for pmc_id, data in valid_pmcs.items():
            if pmc_id in url:
                return (200, {}, json_module.dumps(data))
        return (200, {}, json_module.dumps({"records": [{"status": "error", "errmsg": "not found"}]}))

    rsps.add_callback(
        responses.GET,
        re.compile(r".*ncbi\.nlm\.nih\.gov/pmc/utils/idconv/.*"),
        callback=pmc_callback,
        content_type="application/json",
    )


def _register_datacite_mocks(rsps: responses.RequestsMock) -> None:
    def datacite_callback(request):
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://api\.datacite\.org/dois/.*"),
        callback=datacite_callback,
        content_type="application/json",
    )


def _register_jid_mocks(rsps: responses.RequestsMock) -> None:
    valid_jids = {"otoljpn1970", "jscej1944b", "japeoj"}

    valid_xml = """<?xml version="1.0" encoding="UTF-8"?>
<result xmlns="http://www.w3.org/2005/Atom">
    <status>0</status>
    <entry><title>Example Journal</title></entry>
</result>"""

    invalid_xml = """<?xml version="1.0" encoding="UTF-8"?>
<result xmlns="http://www.w3.org/2005/Atom">
    <status>ERR_001</status>
</result>"""

    def jid_api_callback(request):
        url = request.url
        for jid in valid_jids:
            if f"cdjournal={jid}" in url:
                return (200, {}, valid_xml)
        return (200, {}, invalid_xml)

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://api\.jstage\.jst\.go\.jp/searchapi/.*"),
        callback=jid_api_callback,
        content_type="application/xml",
    )

    def jid_browse_callback(request):
        url = request.url
        for jid in valid_jids:
            if jid in url:
                return (200, {}, '<html><div id="page-content">Journal Page</div></html>')
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://www\.jstage\.jst\.go\.jp/browse/.*"),
        callback=jid_browse_callback,
        content_type="text/html",
    )


def _register_ror_mocks(rsps: responses.RequestsMock) -> None:
    valid_rors = {
        "040jc3p57", "01111rn36", "0138va192", "00wb4mk85",
        "03ztgj037", "03ztgj039", "041qv0h25", "04wxnsj81", "04wxnsj89",
    }

    def ror_callback(request):
        url = request.url
        for ror_id in valid_rors:
            if ror_id in url:
                return (200, {}, json_module.dumps({"id": f"https://ror.org/{ror_id}", "name": "Example Org"}))
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://api\.ror\.org/organizations/.*"),
        callback=ror_callback,
        content_type="application/json",
    )


def _register_viaf_mocks(rsps: responses.RequestsMock) -> None:
    valid_viafs = {
        "5604148947771454950004", "234145033", "56752857",
        "148463773", "102333412",
    }

    def viaf_callback(request):
        url = request.url
        for viaf_id in valid_viafs:
            if viaf_id in url:
                return (200, {}, json_module.dumps({
                    "ns1:VIAFCluster": {"ns1:viafID": viaf_id}
                }))
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https?://viaf\.org/viaf/.*"),
        callback=viaf_callback,
        content_type="application/json",
    )


def _register_wikidata_mocks(rsps: responses.RequestsMock) -> None:
    valid_wikidata_ids = {"Q34433", "Q24698708", "Q15767074", "Q7842", "Q42"}

    def wikidata_callback(request):
        url = request.url
        for wikidata_id in valid_wikidata_ids:
            if wikidata_id in url:
                return (200, {}, json_module.dumps({
                    "entities": {wikidata_id: {"id": wikidata_id, "type": "item"}}
                }))
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://www\.wikidata\.org/wiki/Special:EntityData/.*"),
        callback=wikidata_callback,
        content_type="application/json",
    )


def _register_wikipedia_mocks(rsps: responses.RequestsMock) -> None:
    valid_wikipedia_ids = {"30456", "43744177"}

    def wikipedia_callback(request):
        url = request.url
        for wiki_id in valid_wikipedia_ids:
            if f"pageids={wiki_id}" in url or f"pageids%3D{wiki_id}" in url:
                return (200, {}, json_module.dumps({
                    "query": {"pages": {wiki_id: {"pageid": int(wiki_id), "title": "Example Page"}}}
                }))
        return (200, {}, json_module.dumps({"query": {"pages": {}}}))

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://en\.wikipedia\.org/w/api\.php.*"),
        callback=wikipedia_callback,
        content_type="application/json",
    )


def _register_medra_mocks(rsps: responses.RequestsMock) -> None:
    medra_article_xml = """<?xml version="1.0" encoding="UTF-8"?>
<ONIXDOISerialArticleWorkRegistrationMessage xmlns="http://www.editeur.org/onix/DOIMetadata/2.0">
   <Header>
      <FromCompany>mEDRA</FromCompany>
      <FromEmail>medra@medra.org</FromEmail>
      <ToCompany>PublicService</ToCompany>
      <SentDate>20260305</SentDate>
   </Header>
   <DOISerialArticleWork>
      <NotificationType>07</NotificationType>
      <DOI>10.3233/DS-210053</DOI>
      <DOIWebsiteLink>https://www.medra.org/servlet/aliasResolver?alias=iospress&amp;doi=10.3233/DS-210053</DOIWebsiteLink>
      <RegistrantName>Default Registrant</RegistrantName>
      <RegistrationAuthority>mEDRA</RegistrationAuthority>
      <SerialPublication>
         <SerialWork>
            <WorkIdentifier>
               <WorkIDType>16</WorkIDType>
               <IDValue>2451-8484</IDValue>
            </WorkIdentifier>
            <Title>
               <TitleType>01</TitleType>
               <TitleText>Data Science</TitleText>
            </Title>
            <Publisher>
               <PublishingRole>01</PublishingRole>
               <PublisherName>IOS Press</PublisherName>
            </Publisher>
            <CountryOfPublication>NL</CountryOfPublication>
         </SerialWork>
         <SerialVersion>
            <ProductIdentifier>
               <ProductIDType>07</ProductIDType>
               <IDValue>24518492</IDValue>
            </ProductIdentifier>
            <ProductForm>JD</ProductForm>
         </SerialVersion>
         <SerialVersion>
            <ProductIdentifier>
               <ProductIDType>07</ProductIDType>
               <IDValue>24518484</IDValue>
            </ProductIdentifier>
            <ProductForm>JB</ProductForm>
         </SerialVersion>
      </SerialPublication>
      <JournalIssue>
         <JournalVolumeNumber>5</JournalVolumeNumber>
         <JournalIssueNumber>2</JournalIssueNumber>
         <JournalIssueDate>
            <DateFormat>00</DateFormat>
            <Date>20220720</Date>
         </JournalIssueDate>
      </JournalIssue>
      <ContentItem>
         <TextItem>
            <TextItemType>10</TextItemType>
            <PageRun>
               <FirstPageNumber>97</FirstPageNumber>
               <LastPageNumber>138</LastPageNumber>
            </PageRun>
         </TextItem>
         <Title>
            <TitleType>01</TitleType>
            <TitleText>Packaging research artefacts with RO-Crate</TitleText>
         </Title>
         <Contributor>
            <SequenceNumber>1</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0001-9842-9718</IDValue></NameIdentifier>
            <PersonNameInverted>Soiland-Reyes, Stian</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>2</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0002-3545-944X</IDValue></NameIdentifier>
            <PersonNameInverted>Sefton, Peter</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>3</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0003-1304-1939</IDValue></NameIdentifier>
            <PersonNameInverted>Crosas, Mercè</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>4</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0003-3986-0510</IDValue></NameIdentifier>
            <PersonNameInverted>Castro, Leyla Jael</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>5</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0001-6565-5145</IDValue></NameIdentifier>
            <PersonNameInverted>Coppens, Frederik</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>6</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0002-4806-5140</IDValue></NameIdentifier>
            <PersonNameInverted>Fernández, José M.</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>7</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0003-0454-7145</IDValue></NameIdentifier>
            <PersonNameInverted>Garijo, Daniel</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>8</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0002-3079-6586</IDValue></NameIdentifier>
            <PersonNameInverted>Grüning, Björn</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>9</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0001-5383-6993</IDValue></NameIdentifier>
            <PersonNameInverted>La Rosa, Marco</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>10</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0001-8271-5429</IDValue></NameIdentifier>
            <PersonNameInverted>Leo, Simone</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>11</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0001-8131-2150</IDValue></NameIdentifier>
            <PersonNameInverted>Ó Carragáin, Eoghan</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>12</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0002-9648-6484</IDValue></NameIdentifier>
            <PersonNameInverted>Portier, Marc</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>13</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0003-1991-0533</IDValue></NameIdentifier>
            <PersonNameInverted>Trisovic, Ana</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>14</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <PersonNameInverted>RO-Crate Community</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>15</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0003-0183-6910</IDValue></NameIdentifier>
            <PersonNameInverted>Groth, Paul</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>16</SequenceNumber>
            <ContributorRole>A01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0003-1219-2137</IDValue></NameIdentifier>
            <PersonNameInverted>Goble, Carole</PersonNameInverted>
         </Contributor>
         <Contributor>
            <SequenceNumber>17</SequenceNumber>
            <ContributorRole>B01</ContributorRole>
            <NameIdentifier><NameIDType>21</NameIDType><IDValue>https://orcid.org/0000-0003-0530-4305</IDValue></NameIdentifier>
            <PersonNameInverted>Peroni, Silvio</PersonNameInverted>
         </Contributor>
         <PublicationDate>20220720</PublicationDate>
      </ContentItem>
   </DOISerialArticleWork>
</ONIXDOISerialArticleWorkRegistrationMessage>"""

    medra_book_xml = """<?xml version="1.0" encoding="UTF-8"?>
<ONIXDOIMonographicProductRegistrationMessage xmlns="http://www.editeur.org/onix/DOIMetadata/2.0">
   <Header>
      <FromCompany>mEDRA</FromCompany>
      <FromEmail>medra@medra.org</FromEmail>
      <ToCompany>PublicService</ToCompany>
      <SentDate>20260305</SentDate>
   </Header>
   <DOIMonographicProduct>
      <NotificationType>06</NotificationType>
      <DOI>10.23775/20221026</DOI>
      <DOIWebsiteLink>https://perosh.eu/wp-content/uploads/2022/10/Book-of-Abstracts_2nd-PEROSH-PWL-conference_DOI-10.23775-20221026.pdf</DOIWebsiteLink>
      <RegistrantName>PEROSH</RegistrantName>
      <RegistrationAuthority>mEDRA</RegistrationAuthority>
      <ProductForm>DA</ProductForm>
      <Title>
         <TitleType>01</TitleType>
         <TitleText>Book of Abstract: 2nd International PEROSH conference on Prolonging Working Life</TitleText>
      </Title>
      <Contributor>
         <SequenceNumber>1</SequenceNumber>
         <ContributorRole>A01</ContributorRole>
         <CorporateName>PEROSH member institutes</CorporateName>
      </Contributor>
      <Language>
         <LanguageRole>01</LanguageRole>
         <LanguageCode>eng</LanguageCode>
      </Language>
      <Publisher>
         <PublishingRole>01</PublishingRole>
         <PublisherName>PEROSH</PublisherName>
      </Publisher>
      <CountryOfPublication>SE</CountryOfPublication>
      <PublicationDate>202209</PublicationDate>
   </DOIMonographicProduct>
</ONIXDOIMonographicProductRegistrationMessage>"""

    medra_series_xml = """<?xml version="1.0" encoding="UTF-8"?>
<ONIXDOISerialTitleWorkRegistrationMessage xmlns="http://www.editeur.org/onix/DOIMetadata/2.0">
   <Header>
      <FromCompany>mEDRA</FromCompany>
      <FromEmail>medra@medra.org</FromEmail>
      <ToCompany>PublicService</ToCompany>
      <SentDate>20260305</SentDate>
   </Header>
   <DOISerialTitleWork>
      <NotificationType>07</NotificationType>
      <DOI>10.17426/58141</DOI>
      <DOIWebsiteLink>https://www.centroricercheroma.it/prodotto/laquila-oltre-i-terremoti-costruzioni-e-ricostruzioni-della-citta/</DOIWebsiteLink>
      <RegistrantName>CROMA - UNIVERSITÀ ROMA TRE</RegistrantName>
      <RegistrationAuthority>mEDRA</RegistrationAuthority>
      <SerialPublication>
         <SerialWork>
            <Title language="ita">
               <TitleType>01</TitleType>
               <TitleText>L’Aquila oltre i terremoti. Costruzioni e ricostruzioni della città a cura di Simonetta Ciranna e Manuel Vaquero Piñeiro</TitleText>
            </Title>
            <Publisher>
               <PublishingRole>01</PublishingRole>
               <PublisherIdentifier>
                  <PublisherIDType>01</PublisherIDType>
                  <IDValue>1</IDValue>
               </PublisherIdentifier>
               <PublisherName>CROMA - UNIVERSITÀ ROMA TRE</PublisherName>
            </Publisher>
            <CountryOfPublication>IT</CountryOfPublication>
         </SerialWork>
      </SerialPublication>
      <Language>
         <LanguageRole>01</LanguageRole>
         <LanguageCode>ita</LanguageCode>
      </Language>
      <PublishingStatus>04</PublishingStatus>
      <DateFirstPublished>
         <DateFormat>00</DateFormat>
         <Date>20110708</Date>
      </DateFirstPublished>
   </DOISerialTitleWork>
</ONIXDOISerialTitleWorkRegistrationMessage>"""

    medra_chapter_xml = """<?xml version="1.0" encoding="UTF-8"?>
<ONIXDOIMonographChapterWorkRegistrationMessage xmlns="http://www.editeur.org/onix/DOIMetadata/2.0">
   <Header>
      <FromCompany>mEDRA</FromCompany>
      <FromEmail>medra@medra.org</FromEmail>
      <ToCompany>PublicService</ToCompany>
      <SentDate>20260305</SentDate>
   </Header>
   <DOIMonographChapterWork>
      <NotificationType>06</NotificationType>
      <DOI>10.2357/9783739880303-105</DOI>
      <DOIWebsiteLink>https://elibrary.narr.digital/book/99.0000/9783739880303</DOIWebsiteLink>
      <DOIStructuralType>Abstraction</DOIStructuralType>
      <DOIMode>Abstract</DOIMode>
      <RegistrantName>Narr Francke Attempto</RegistrantName>
      <MonographicPublication>
         <MonographicWork>
            <WorkIdentifier>
               <WorkIDType>06</WorkIDType>
               <IDValue>10.2357/9783739880303-105</IDValue>
            </WorkIdentifier>
            <Title language="ger">
               <TitleType>01</TitleType>
               <TitleText>Bodenseeschifferpatent kompakt</TitleText>
               <Subtitle>Motorboot und Segelboot</Subtitle>
            </Title>
         </MonographicWork>
      </MonographicPublication>
      <ContentItem>
         <Title language="ger">
            <TitleType>01</TitleType>
            <TitleText>Kapitel 11: Sturmwarndienst und Seenotrettung</TitleText>
         </Title>
         <Contributor>
            <ContributorRole>A01</ContributorRole>
            <PersonNameInverted>Wassermann, Matthias</PersonNameInverted>
         </Contributor>
         <Language>
            <LanguageRole>01</LanguageRole>
            <LanguageCode>ger</LanguageCode>
         </Language>
         <PublicationDate>2020</PublicationDate>
         <CopyrightStatement>
            <CopyrightYear>2020</CopyrightYear>
            <CopyrightOwner>
               <CorporateName>Narr Francke Attempto Verlag GmbH + Co. KG</CorporateName>
            </CopyrightOwner>
         </CopyrightStatement>
      </ContentItem>
   </DOIMonographChapterWork>
</ONIXDOIMonographChapterWorkRegistrationMessage>"""

    medra_responses = {
        "10.3233/ds-210053": medra_article_xml,
        "10.23775/20221026": medra_book_xml,
        "10.17426/58141": medra_series_xml,
        "10.2357/9783739880303-105": medra_chapter_xml,
    }

    def medra_callback(request):
        url = request.url.lower()
        for doi, xml in medra_responses.items():
            if doi.lower() in url:
                return (200, {}, xml)
        return (404, {}, "")

    rsps.add_callback(
        responses.GET,
        re.compile(r"https://api\.medra\.org/metadata/.*"),
        callback=medra_callback,
        content_type="application/xml",
    )


def _register_url_mocks(rsps: responses.RequestsMock) -> None:
    valid_urls = {
        "datacite.org",
        "opencitations.net",
        "nih.gov",
        "it.wikipedia.org/wiki/Muro%20di%20Berlino",
        "it.wikipedia.org/wiki/Muro di Berlino",
    }
    invalid_paths = {"invalid_url", "not%20a%20real%20page", "not a real page"}

    def url_callback(request):
        url = request.url.lower()
        for invalid in invalid_paths:
            if invalid in url:
                return (404, {}, "")
        for valid in valid_urls:
            if valid.lower() in url:
                return (200, {}, "<html><body>Valid page</body></html>")
        return (404, {}, "")

    for domain in ["datacite.org", "opencitations.net", "nih.gov", "it.wikipedia.org"]:
        rsps.add_callback(
            responses.GET,
            re.compile(rf"https?://(www\.)?{re.escape(domain)}.*"),
            callback=url_callback,
            content_type="text/html",
        )
