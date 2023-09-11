import os
import unittest
import json

from oc_ds_converter.lib.csvmanager import CSVManager
from oc_ds_converter.jalc.jalc_processing import JalcProcessing


BASE = os.path.join('test', 'jalc_processing')
IOD = os.path.join(BASE, 'iod')
WANTED_DOIS = os.path.join(BASE, 'wanted_dois.csv')
WANTED_DOIS_FOLDER = os.path.join(BASE, 'wanted_dois')
DATA = os.path.join(BASE, 'dois_1011230-jsts172_1.json')
DATA_DIR = BASE
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
PUBLISHERS_MAPPING = os.path.join(BASE, 'publishers.csv')


class TestJalcProcessing(unittest.TestCase):

    def test_csv_creator(self):
        jalc_processor_citing = JalcProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS_FOLDER, citing=True)
        jalc_processor_cited = JalcProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS_FOLDER, citing=False)
        with open(DATA, "r", encoding="utf-8") as content:
            data = json.load(content)
        output = list()
        item = data["data"]
        tabular_data = jalc_processor_citing.csv_creator(item)
        if tabular_data:
            output.append(tabular_data)
        for citation in item['citation_list']:
            if citation.get("doi"):
                tabular_data_cited = jalc_processor_cited.csv_creator(citation)
                if tabular_data_cited:
                    output.append(tabular_data_cited)
        expected_output = [
            {'id': 'doi:10.11230/jsts.17.2_1',
             'title': 'Diamond Synthesis with Completely Closed Chamber from Solid or Liquid Carbon Sources, In-Situ Analysis of Gaseous Species and the Possible Reaction Model',
             'author': 'TAKAGI, Yoshiki [orcid:0000-0001-9597-7030]',
             'issue': '2',
             'volume': '17',
             'venue': 'The Journal of Space Technology and Science [issn:0911-551X issn:2186-4772 jid:jsts]',
             'pub_date': '2001',
             'page': '2_1-2_7',
             'type': 'journal article',
             'publisher': '特定非営利活動法人 日本ロケット協会',
             'editor': ''},
            {'id': 'doi:10.1016/0022-0248(83)90411-6',
             'title': '',
             'author': '',
             'issue': '',
             'volume': '62',
             'venue': 'Journal of Crystal Growth',
             'pub_date': '1983',
             'page': '642-642',
             'type': '',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1016/0038-1098(88)91128-3',
             'title': 'Raman spectra of diamondlike amorphous carbon films.',
             'author': '',
             'issue': '11',
             'volume': '66',
             'venue': '',
             'pub_date': '1988',
             'page': '1177-1180',
             'type': '',
             'publisher': '',
             'editor': ''}]

        self.assertEqual(output, expected_output)

    def test_orcid_finder(self):
        jalc_processor = JalcProcessing(orcid_index=IOD)
        doi = '10.11185/imt.8.380'
        orcid_found = jalc_processor.orcid_finder(doi)
        expected_output = {'0000-0002-2149-4113': 'dobashi, yoshinori'}
        self.assertEqual(orcid_found, expected_output)

    def test_get_agents_strings_list_overlapping_surnames(self):
        # The surname of one author is included in the surname of another.
        authors_list = [
            {'role': 'author',
             'name': '井崎 豊田, 理理子',
             'family': '井崎 豊田',
             'given': '理理子'},
            {'role': 'author',
             'name': '豊田, 純一朗',
             'family': '豊田',
             'given': '純一朗'}
            ]
        jalc_processor = JalcProcessing()
        csv_manager = CSVManager()
        csv_manager.data = {'10.11224/cleftpalate1976.23.2_83': {'豊田, 純一朗 [0000-0002-8210-7076]'}}
        jalc_processor.orcid_index = csv_manager
        authors_strings_list, editors_strings_list = jalc_processor.get_agents_strings_list('10.11224/cleftpalate1976.23.2_83', authors_list)
        expected_authors_list = ['井崎 豊田, 理理子', '豊田, 純一朗 [orcid:0000-0002-8210-7076]']
        expected_editors_list = []
        self.assertEqual((authors_strings_list, editors_strings_list), (expected_authors_list, expected_editors_list))

    def test_get_agents_strings_list(self):
        entity_dict = {
            "status": "OK",
            "apiType": "doi",
            "apiVersion": "1.0.0",
            "message": {
                "total": 1,
                "rows": 1,
                "totalPages": 1,
                "page": 1
            },
            "data": {
                "siteId": "SI/JST.JSTAGE",
                "content_type": "JA",
                "doi": "10.11178/jdsa.8.49",
                "url": "https://doi.org/10.11178/jdsa.8.49",
                "ra": "JaLC",
                "prefix": "10.11178",
                "site_name": "J-STAGE",
                "publisher_list": [
                    {
                        "publisher_name": "Agricultural and Forestry Research Center, University of Tsukuba",
                        "lang": "en"
                    },
                    {
                        "publisher_name": "筑波大学農林技術センター",
                        "lang": "ja"
                    }
                ],
                "title_list": [
                    {
                        "lang": "en",
                        "title": "Reformulating Agriculture and Forestry Education in the Philippines: Issues and Concerns"
                    }
                ],
                "creator_list": [
                    {
                        "sequence": "1",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "O. Cruz",
                                "first_name": "Rex Victor"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Professor, Institute of Natural Resources, College of Forestry and Natural Resources, University of the Philippines Los Ba&ntilde;os",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "2",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "B. Bantayan",
                                "first_name": "Rosario"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "University Extension Specialist 1, Department of Forest Products and Paper Science, College of Forestry and Natural Resources, University of the Philippines Los Ba&ntilde;os",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "3",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "D. Landicho",
                                "first_name": "Leila"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "University Researcher 2, Institute of Agroforestry, College of Forestry and Natural Resources, University of the Philippines Los Ba&ntilde;os",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "4",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "C. Bantayan",
                                "first_name": "Nathaniel"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Professor, Institute of Natural Resources, College of Forestry and Natural Resources, University of the Philippines Los Ba&ntilde;os",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    }
                ],
                "publication_date": {
                    "publication_year": "2013"
                },
                "relation_list": [
                    {
                        "content": "https://www.jstage.jst.go.jp/article/jdsa/8/1/8_49/_pdf",
                        "type": "URL",
                        "relation": "fullTextPdf"
                    }
                ],
                "content_language": "en",
                "updated_date": "2014-12-09",
                "article_type": "pub",
                "journal_id_list": [
                    {
                        "journal_id": "1880-3016",
                        "type": "ISSN",
                        "issn_type": "print"
                    },
                    {
                        "journal_id": "1880-3024",
                        "type": "ISSN",
                        "issn_type": "online"
                    },
                    {
                        "journal_id": "jdsa",
                        "type": "JID"
                    }
                ],
                "journal_title_name_list": [
                    {
                        "journal_title_name": "Journal of Developments in Sustainable Agriculture",
                        "type": "full",
                        "lang": "en"
                    },
                    {
                        "journal_title_name": "J. Dev. Sus. Agr.",
                        "type": "abbreviation",
                        "lang": "en"
                    }
                ],
                "journal_classification": "01",
                "journal_txt_lang": "en",
                "recorded_year": "2006-2014",
                "volume": "8",
                "issue": "1",
                "first_page": "49",
                "last_page": "62",
                "date": "2013-03-15",
                "keyword_list": [
                    {
                        "keyword": "forestry",
                        "sequence": "1",
                        "lang": "en"
                    },
                    {
                        "keyword": "agriculture",
                        "sequence": "2",
                        "lang": "en"
                    },
                    {
                        "keyword": "outcomes-based education",
                        "sequence": "3",
                        "lang": "en"
                    }
                ],
                "citation_list": [
                    {
                        "sequence": "1",
                        "original_text": "Adedoyin, O.O. and Shangodoyin, D.K., 2010. Concepts and Practices of Outcome Based Education for Effective Educational System in Botswana. Euro. J. Soc. Sci., 13 (2), 161-170."
                    },
                    {
                        "sequence": "2",
                        "original_text": "Bantayan, N.C., 2007. Forest (Bad) Science and (Mal) Practice: Can Filipino Foresters Rise Above The Pitfalls And Failures Of The Profession?. Policy Paper. Center for Integrative and Development Studies. UP Diliman, Quezon City, Philippines."
                    },
                    {
                        "sequence": "3",
                        "original_text": "BAS (Bureau of Agricultural Statistics). 2011. Philippine Agriculture in Figure. Retrieved December 15, 2012 from http://www.bas.gov.ph"
                    },
                    {
                        "sequence": "4",
                        "original_text": "Carandang, M.G., Landicho, L.D., Andrada II, R. T., Malabrigo, P.L. Jr., Angeles, A.M., Oliva, A.T., Eslava, F.E. Jr. and Regondola, M.L., 2008. Demystifying the State of Forestry Education and the Forestry Profession in the Philippines. Ecosys. Devel. J. 1 (1), 46-54."
                    },
                    {
                        "sequence": "5",
                        "original_text": "CHED Memorandum Order (CMO), 2012. Policies, Standards and Guidelines in the Establishment of an Outcomes-Based Education (OBE) System in Higher Education Institutions offering Engineering Programs. No. 37, Series of 2012."
                    },
                    {
                        "sequence": "6",
                        "original_text": "CHED Memorandum Order (CMO), 2008. Policies and Standards for Bachelor of Science in Agriculture (BSA) Program. No. 14, Series of 2008. Retrieved October 30, 2012 from http://www.ched.gov.ph/chedwww/index.php/eng/Information/CHED-Memorandum-Orders/2008-CHED-Memorandum-Orders"
                    },
                    {
                        "sequence": "7",
                        "original_text": "Cruz, R.V.O., Pulhin, J.M. and Mendoza M.D. 2011. Reinventing CFNR: Leading the Way in Integrated Tropical Forest and Natural Resource Management Education, Research and Governance (2011-2025). A Centennial Professorial Chair Lecture delivered in December 2011 at the Nicolas P. Lansigan Hall, College of Forestry and Natural Resources, University of the Philippines Los Banos, College, Laguna, Philippines."
                    },
                    {
                        "sequence": "8",
                        "doi": "10.11178/jdsa.5.47",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Enhancing Food Security in the Context of Climate Change and the Role of Higher Education Institutions in the Philippines"
                            }
                        ],
                        "volume": "5",
                        "issue": "1",
                        "first_page": "47",
                        "last_page": "63",
                        "publication_date": {
                            "publication_year": "2010"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Espaldon",
                                        "first_name": "Maria Victoria O."
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Professor, School of Environmental Science and Management, University of the Philippines Los Ba&ntilde;os, College",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "2",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Zamora",
                                        "first_name": "Oscar B."
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Professor, College of Agriculture, University of the Philippines Los Ba&ntilde;os, College",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "3",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Perez",
                                        "first_name": "Rosa T."
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Climate Change Specialist, Manila Observatory, Ateneo de Manila University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "Espaldon, M.V.O., Zamora, O.B. and Perez, R.T., 2010. Enhancing Food Security in the Context of Climate Change and the Role of Higher Education Institutions in the Philippines. J. Dev. .Sus. Agr. 5: 47-63."
                    },
                    {
                        "sequence": "9",
                        "original_text": "FAO (Food and Agriculture Organization), 2009. Food Security and Agricultural Mitigation in Developing Countries: Options for Capturing Synergies. FAO, Rome, Italy."
                    },
                    {
                        "sequence": "10",
                        "original_text": "FMB (Forest Management Bureau), 2009. Philippines Forestry Outlook Study in Asia-Pacific Forestry Sector Outlook Study II, FAO Working Paper Series. Working Paper No: APFSOS IIWP/2009/10. Retrieved on October 22, 2012 from http://www.fao.org/docrep/014/am255e/am255e00.pdf"
                    },
                    {
                        "sequence": "11",
                        "original_text": "Habito, C.F and Briones R.M., 2005. Philippine Agriculture over the Years: Performance, Policies and Pitfalls. Paper presented during the conference on &ldquo;Policies to Strengthen Productivity in the Philippines&rsquo;, June 27, 2005, Makati City, Philippines."
                    },
                    {
                        "sequence": "12",
                        "original_text": "Haub, C. and M.M. Kent, 2009. World Population Data Sheet. Retrieved October 30, 2012 from http://www.prb.org/pdf09/09wpds_eng.pdf."
                    },
                    {
                        "sequence": "13",
                        "original_text": "Killen, R., 2000. Outcomes-based education: Principles and possibilities. Unpublished manuscript, University of Newcastle, Faculty of Education. Retrieved on October 29, 2012 from http://drjj.uitm.edu.my/DRJJ/CONFERENCE/UPSI/OBEKillen.pdf"
                    },
                    {
                        "sequence": "14",
                        "original_text": "Nair, C.T.S. Undated. What does the future hold for forestry education? Retrieved October 25, 2012 from http://www.fao.org/docrep/007/y5382e/y5382e02.htm#P23_5619"
                    },
                    {
                        "sequence": "15",
                        "original_text": "NEDA (National Economic Development Authority). 2011. Philippine Development Plan 2011-2016. Retrieved December 15, 2012 from http://www.neda.gov.ph"
                    },
                    {
                        "sequence": "16",
                        "original_text": "NSCB (National Statistics and Coordination Board). 2012. Statistics on agriculture and fisheries. Retrieved December 15, 2012 from http://www.nscb.gov.ph"
                    },
                    {
                        "sequence": "17",
                        "original_text": "Osias, T., Tacardon, L. and Pedroso, L. (undated). People Beyond Numbers. The road to population stabilization of in the Philippines. Retrieved October 28, 2012 from http://www.gillespiefoundation.org/uploads/Philippines_Report.pdf"
                    },
                    {
                        "sequence": "18",
                        "original_text": "PEM (Philippines Environment Monitor). 2003. Water Quality. Published by the Department of Environment and Natural Resources, the Environmental Management Bureau and the World Bank Group, Country Office Manila. Retrieved on October 26, 2012 from http://worldbank.org/country/philippines"
                    },
                    {
                        "sequence": "19",
                        "original_text": "PFS (Philippine Forestry Statistics), 2011. Philippine Forestry Statistics, Published by Forestry Management Bureau, Department of Environment and Natural Resources, Republic of the Philippines. Retrieved on October 26, 2012 from http://forestry.denr.gov.ph/2011PFS.pdf"
                    },
                    {
                        "sequence": "20",
                        "original_text": "PIDS (Philippine Institute for Development Studies) 2011. What&rsquo;s in store for AFNR Graduates in the Philippines? Dev. Res. News. 29 (4), July-August 2011, ISSN 0115-9097."
                    },
                    {
                        "sequence": "21",
                        "original_text": "Rebugio L.L. and Camacho, L.D. Reorienting forestry education to sustainable management. J. Environ. Sci. Manag. 6 (2): 49-58."
                    },
                    {
                        "sequence": "22",
                        "original_text": "Tolentino, B., David, C., Balisacan, A. and Intal P., 2001. &ldquo;Strategic Actions to Rapidly Ensure Food Security and Rural Growth in the.&rdquo; In <I>Yellow Paper II: The Post-Erap Reform Agenda. </I>Retrieved on October 24, 2012 from http://www.aer.ph/images/stories/projects/yp2/agri.pdf"
                    },
                    {
                        "sequence": "23",
                        "original_text": "Zundel, P.E. and Needham, T.D., 2000. Outcomes-Based Forestry Education. Application in New Brusnwick. J. For., 98 (2): 30-35."
                    }
                ]
            }
        }
        entity_dict = entity_dict["data"]
        jalc_processor = JalcProcessing()
        authors_list = jalc_processor.get_authors(entity_dict)
        authors_strings_list, _ = jalc_processor.get_agents_strings_list(entity_dict["doi"], authors_list)
        expected_authors_list = ['O. Cruz, Rex Victor', 'B. Bantayan, Rosario', 'D. Landicho, Leila', 'C. Bantayan, Nathaniel']

        self.assertEqual(authors_strings_list, expected_authors_list)


    def test_get_agents_strings_list_same_family(self):
        # Two authors have the same family name and the same given name initials
        # The authors' information related to this publication are false, they have been used just for testing purposes
        entity_dict = {
            "status": "OK",
            "apiType": "doi",
            "apiVersion": "1.0.0",
            "message": {
                "total": 1,
                "rows": 1,
                "totalPages": 1,
                "page": 1
            },
            "data": {
                "siteId": "SI/JST.JSTAGE",
                "content_type": "JA",
                "doi": "10.11178/jdsa.8.49",
                "url": "https://doi.org/10.11178/jdsa.8.49",
                "ra": "JaLC",
                "prefix": "10.11178",
                "site_name": "J-STAGE",
                "publisher_list": [
                    {
                        "publisher_name": "Agricultural and Forestry Research Center, University of Tsukuba",
                        "lang": "en"
                    },
                    {
                        "publisher_name": "筑波大学農林技術センター",
                        "lang": "ja"
                    }
                ],
                "title_list": [
                    {
                        "lang": "en",
                        "title": "Reformulating Agriculture and Forestry Education in the Philippines: Issues and Concerns"
                    }
                ],
                "creator_list": [
                    {
                        "sequence": "1",
                        "type": "person",
                        "names": [
                            {
                                "lang": "ja",
                                "last_name": "豊田",
                                "first_name": "理理子"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Professor, Institute of Natural Resources, College of Forestry and Natural Resources, University of the Philippines Los Ba&ntilde;os",
                                "sequence": "1",
                                "lang": "ja"
                            }
                        ]
                    },
                    {
                        "sequence": "2",
                        "type": "person",
                        "names": [
                            {
                                "lang": "ja",
                                "last_name": "豊田",
                                "first_name": "理純一朗"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "University Extension Specialist 1, Department of Forest Products and Paper Science, College of Forestry and Natural Resources, University of the Philippines Los Ba&ntilde;os",
                                "sequence": "2",
                                "lang": "ja"
                            }
                        ]
                    }
                ],
                "publication_date": {
                    "publication_year": "2013"
                },
                "relation_list": [
                    {
                        "content": "https://www.jstage.jst.go.jp/article/jdsa/8/1/8_49/_pdf",
                        "type": "URL",
                        "relation": "fullTextPdf"
                    }
                ],
                "content_language": "en",
                "updated_date": "2014-12-09",
                "article_type": "pub",
                "journal_id_list": [
                    {
                        "journal_id": "1880-3016",
                        "type": "ISSN",
                        "issn_type": "print"
                    },
                    {
                        "journal_id": "1880-3024",
                        "type": "ISSN",
                        "issn_type": "online"
                    },
                    {
                        "journal_id": "jdsa",
                        "type": "JID"
                    }
                ],
                "journal_title_name_list": [
                    {
                        "journal_title_name": "Journal of Developments in Sustainable Agriculture",
                        "type": "full",
                        "lang": "en"
                    },
                    {
                        "journal_title_name": "J. Dev. Sus. Agr.",
                        "type": "abbreviation",
                        "lang": "en"
                    }
                ],
                "journal_classification": "01",
                "journal_txt_lang": "en",
                "recorded_year": "2006-2014",
                "volume": "8",
                "issue": "1",
                "first_page": "49",
                "last_page": "62",
                "date": "2013-03-15",
                "keyword_list": [
                    {
                        "keyword": "forestry",
                        "sequence": "1",
                        "lang": "en"
                    },
                    {
                        "keyword": "agriculture",
                        "sequence": "2",
                        "lang": "en"
                    },
                    {
                        "keyword": "outcomes-based education",
                        "sequence": "3",
                        "lang": "en"
                    }
                ],
                "citation_list": [
                    {
                        "sequence": "1",
                        "original_text": "Adedoyin, O.O. and Shangodoyin, D.K., 2010. Concepts and Practices of Outcome Based Education for Effective Educational System in Botswana. Euro. J. Soc. Sci., 13 (2), 161-170."
                    },
                    {
                        "sequence": "2",
                        "original_text": "Bantayan, N.C., 2007. Forest (Bad) Science and (Mal) Practice: Can Filipino Foresters Rise Above The Pitfalls And Failures Of The Profession?. Policy Paper. Center for Integrative and Development Studies. UP Diliman, Quezon City, Philippines."
                    },
                    {
                        "sequence": "3",
                        "original_text": "BAS (Bureau of Agricultural Statistics). 2011. Philippine Agriculture in Figure. Retrieved December 15, 2012 from http://www.bas.gov.ph"
                    },
                    {
                        "sequence": "4",
                        "original_text": "Carandang, M.G., Landicho, L.D., Andrada II, R. T., Malabrigo, P.L. Jr., Angeles, A.M., Oliva, A.T., Eslava, F.E. Jr. and Regondola, M.L., 2008. Demystifying the State of Forestry Education and the Forestry Profession in the Philippines. Ecosys. Devel. J. 1 (1), 46-54."
                    },
                    {
                        "sequence": "5",
                        "original_text": "CHED Memorandum Order (CMO), 2012. Policies, Standards and Guidelines in the Establishment of an Outcomes-Based Education (OBE) System in Higher Education Institutions offering Engineering Programs. No. 37, Series of 2012."
                    },
                    {
                        "sequence": "6",
                        "original_text": "CHED Memorandum Order (CMO), 2008. Policies and Standards for Bachelor of Science in Agriculture (BSA) Program. No. 14, Series of 2008. Retrieved October 30, 2012 from http://www.ched.gov.ph/chedwww/index.php/eng/Information/CHED-Memorandum-Orders/2008-CHED-Memorandum-Orders"
                    },
                    {
                        "sequence": "7",
                        "original_text": "Cruz, R.V.O., Pulhin, J.M. and Mendoza M.D. 2011. Reinventing CFNR: Leading the Way in Integrated Tropical Forest and Natural Resource Management Education, Research and Governance (2011-2025). A Centennial Professorial Chair Lecture delivered in December 2011 at the Nicolas P. Lansigan Hall, College of Forestry and Natural Resources, University of the Philippines Los Banos, College, Laguna, Philippines."
                    },
                    {
                        "sequence": "8",
                        "doi": "10.11178/jdsa.5.47",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Enhancing Food Security in the Context of Climate Change and the Role of Higher Education Institutions in the Philippines"
                            }
                        ],
                        "volume": "5",
                        "issue": "1",
                        "first_page": "47",
                        "last_page": "63",
                        "publication_date": {
                            "publication_year": "2010"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Espaldon",
                                        "first_name": "Maria Victoria O."
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Professor, School of Environmental Science and Management, University of the Philippines Los Ba&ntilde;os, College",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "2",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Zamora",
                                        "first_name": "Oscar B."
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Professor, College of Agriculture, University of the Philippines Los Ba&ntilde;os, College",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "3",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Perez",
                                        "first_name": "Rosa T."
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Climate Change Specialist, Manila Observatory, Ateneo de Manila University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "Espaldon, M.V.O., Zamora, O.B. and Perez, R.T., 2010. Enhancing Food Security in the Context of Climate Change and the Role of Higher Education Institutions in the Philippines. J. Dev. .Sus. Agr. 5: 47-63."
                    },
                    {
                        "sequence": "9",
                        "original_text": "FAO (Food and Agriculture Organization), 2009. Food Security and Agricultural Mitigation in Developing Countries: Options for Capturing Synergies. FAO, Rome, Italy."
                    },
                    {
                        "sequence": "10",
                        "original_text": "FMB (Forest Management Bureau), 2009. Philippines Forestry Outlook Study in Asia-Pacific Forestry Sector Outlook Study II, FAO Working Paper Series. Working Paper No: APFSOS IIWP/2009/10. Retrieved on October 22, 2012 from http://www.fao.org/docrep/014/am255e/am255e00.pdf"
                    },
                    {
                        "sequence": "11",
                        "original_text": "Habito, C.F and Briones R.M., 2005. Philippine Agriculture over the Years: Performance, Policies and Pitfalls. Paper presented during the conference on &ldquo;Policies to Strengthen Productivity in the Philippines&rsquo;, June 27, 2005, Makati City, Philippines."
                    },
                    {
                        "sequence": "12",
                        "original_text": "Haub, C. and M.M. Kent, 2009. World Population Data Sheet. Retrieved October 30, 2012 from http://www.prb.org/pdf09/09wpds_eng.pdf."
                    },
                    {
                        "sequence": "13",
                        "original_text": "Killen, R., 2000. Outcomes-based education: Principles and possibilities. Unpublished manuscript, University of Newcastle, Faculty of Education. Retrieved on October 29, 2012 from http://drjj.uitm.edu.my/DRJJ/CONFERENCE/UPSI/OBEKillen.pdf"
                    },
                    {
                        "sequence": "14",
                        "original_text": "Nair, C.T.S. Undated. What does the future hold for forestry education? Retrieved October 25, 2012 from http://www.fao.org/docrep/007/y5382e/y5382e02.htm#P23_5619"
                    },
                    {
                        "sequence": "15",
                        "original_text": "NEDA (National Economic Development Authority). 2011. Philippine Development Plan 2011-2016. Retrieved December 15, 2012 from http://www.neda.gov.ph"
                    },
                    {
                        "sequence": "16",
                        "original_text": "NSCB (National Statistics and Coordination Board). 2012. Statistics on agriculture and fisheries. Retrieved December 15, 2012 from http://www.nscb.gov.ph"
                    },
                    {
                        "sequence": "17",
                        "original_text": "Osias, T., Tacardon, L. and Pedroso, L. (undated). People Beyond Numbers. The road to population stabilization of in the Philippines. Retrieved October 28, 2012 from http://www.gillespiefoundation.org/uploads/Philippines_Report.pdf"
                    },
                    {
                        "sequence": "18",
                        "original_text": "PEM (Philippines Environment Monitor). 2003. Water Quality. Published by the Department of Environment and Natural Resources, the Environmental Management Bureau and the World Bank Group, Country Office Manila. Retrieved on October 26, 2012 from http://worldbank.org/country/philippines"
                    },
                    {
                        "sequence": "19",
                        "original_text": "PFS (Philippine Forestry Statistics), 2011. Philippine Forestry Statistics, Published by Forestry Management Bureau, Department of Environment and Natural Resources, Republic of the Philippines. Retrieved on October 26, 2012 from http://forestry.denr.gov.ph/2011PFS.pdf"
                    },
                    {
                        "sequence": "20",
                        "original_text": "PIDS (Philippine Institute for Development Studies) 2011. What&rsquo;s in store for AFNR Graduates in the Philippines? Dev. Res. News. 29 (4), July-August 2011, ISSN 0115-9097."
                    },
                    {
                        "sequence": "21",
                        "original_text": "Rebugio L.L. and Camacho, L.D. Reorienting forestry education to sustainable management. J. Environ. Sci. Manag. 6 (2): 49-58."
                    },
                    {
                        "sequence": "22",
                        "original_text": "Tolentino, B., David, C., Balisacan, A. and Intal P., 2001. &ldquo;Strategic Actions to Rapidly Ensure Food Security and Rural Growth in the.&rdquo; In <I>Yellow Paper II: The Post-Erap Reform Agenda. </I>Retrieved on October 24, 2012 from http://www.aer.ph/images/stories/projects/yp2/agri.pdf"
                    },
                    {
                        "sequence": "23",
                        "original_text": "Zundel, P.E. and Needham, T.D., 2000. Outcomes-Based Forestry Education. Application in New Brusnwick. J. For., 98 (2): 30-35."
                    }
                ]
            }
        }
        entity_dict = entity_dict["data"]
        jalc_processor = JalcProcessing()
        authors_list = jalc_processor.get_authors(entity_dict)
        authors_strings_list, _ = jalc_processor.get_agents_strings_list(entity_dict["doi"], authors_list)
        expected_authors_list = ['豊田, 理理子', '豊田, 理純一朗']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_homonyms(self):
        # Two authors have the same family name and the same given name
        # The authors' information related to this publication are false, they have been used just for testing purposes
        entity_dict = {
            "status": "OK",
            "apiType": "doi",
            "apiVersion": "1.0.0",
            "message": {
                "total": 1,
                "rows": 1,
                "totalPages": 1,
                "page": 1
            },
            "data": {
                "siteId": "SI/JST.JSTAGE",
                "content_type": "JA",
                "doi": "10.11178/jdsa.8.49",
                "url": "https://doi.org/10.11178/jdsa.8.49",
                "ra": "JaLC",
                "prefix": "10.11178",
                "site_name": "J-STAGE",
                "publisher_list": [
                    {
                        "publisher_name": "Agricultural and Forestry Research Center, University of Tsukuba",
                        "lang": "en"
                    },
                    {
                        "publisher_name": "筑波大学農林技術センター",
                        "lang": "ja"
                    }
                ],
                "title_list": [
                    {
                        "lang": "en",
                        "title": "Reformulating Agriculture and Forestry Education in the Philippines: Issues and Concerns"
                    }
                ],
                "creator_list": [
                    {
                        "sequence": "1",
                        "type": "person",
                        "names": [
                            {
                                "lang": "ja",
                                "last_name": "豊田",
                                "first_name": "理理子"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Professor, Institute of Natural Resources, College of Forestry and Natural Resources, University of the Philippines Los Ba&ntilde;os",
                                "sequence": "1",
                                "lang": "ja"
                            }
                        ]
                    },
                    {
                        "sequence": "2",
                        "type": "person",
                        "names": [
                            {
                                "lang": "ja",
                                "last_name": "豊田",
                                "first_name": "理純一朗"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "University Extension Specialist 1, Department of Forest Products and Paper Science, College of Forestry and Natural Resources, University of the Philippines Los Ba&ntilde;os",
                                "sequence": "2",
                                "lang": "ja"
                            }
                        ]
                    },
                    {
                        "sequence": "3",
                        "type": "person",
                        "names": [
                            {
                                "lang": "ja",
                                "last_name": "豊田",
                                "first_name": "理純一朗"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Climate Change Specialist, Manila Observatory, Ateneo de Manila University",
                                "sequence": "3",
                                "lang": "ja"
                            }
                        ]
                    }
                ],
                "publication_date": {
                    "publication_year": "2013"
                },
                "relation_list": [
                    {
                        "content": "https://www.jstage.jst.go.jp/article/jdsa/8/1/8_49/_pdf",
                        "type": "URL",
                        "relation": "fullTextPdf"
                    }
                ],
                "content_language": "en",
                "updated_date": "2014-12-09",
                "article_type": "pub",
                "journal_id_list": [
                    {
                        "journal_id": "1880-3016",
                        "type": "ISSN",
                        "issn_type": "print"
                    },
                    {
                        "journal_id": "1880-3024",
                        "type": "ISSN",
                        "issn_type": "online"
                    },
                    {
                        "journal_id": "jdsa",
                        "type": "JID"
                    }
                ],
                "journal_title_name_list": [
                    {
                        "journal_title_name": "Journal of Developments in Sustainable Agriculture",
                        "type": "full",
                        "lang": "en"
                    },
                    {
                        "journal_title_name": "J. Dev. Sus. Agr.",
                        "type": "abbreviation",
                        "lang": "en"
                    }
                ],
                "journal_classification": "01",
                "journal_txt_lang": "en",
                "recorded_year": "2006-2014",
                "volume": "8",
                "issue": "1",
                "first_page": "49",
                "last_page": "62",
                "date": "2013-03-15",
                "keyword_list": [
                    {
                        "keyword": "forestry",
                        "sequence": "1",
                        "lang": "en"
                    },
                    {
                        "keyword": "agriculture",
                        "sequence": "2",
                        "lang": "en"
                    },
                    {
                        "keyword": "outcomes-based education",
                        "sequence": "3",
                        "lang": "en"
                    }
                ],
                "citation_list": [
                    {
                        "sequence": "1",
                        "original_text": "Adedoyin, O.O. and Shangodoyin, D.K., 2010. Concepts and Practices of Outcome Based Education for Effective Educational System in Botswana. Euro. J. Soc. Sci., 13 (2), 161-170."
                    },
                    {
                        "sequence": "2",
                        "original_text": "Bantayan, N.C., 2007. Forest (Bad) Science and (Mal) Practice: Can Filipino Foresters Rise Above The Pitfalls And Failures Of The Profession?. Policy Paper. Center for Integrative and Development Studies. UP Diliman, Quezon City, Philippines."
                    },
                    {
                        "sequence": "3",
                        "original_text": "BAS (Bureau of Agricultural Statistics). 2011. Philippine Agriculture in Figure. Retrieved December 15, 2012 from http://www.bas.gov.ph"
                    },
                    {
                        "sequence": "4",
                        "original_text": "Carandang, M.G., Landicho, L.D., Andrada II, R. T., Malabrigo, P.L. Jr., Angeles, A.M., Oliva, A.T., Eslava, F.E. Jr. and Regondola, M.L., 2008. Demystifying the State of Forestry Education and the Forestry Profession in the Philippines. Ecosys. Devel. J. 1 (1), 46-54."
                    },
                    {
                        "sequence": "5",
                        "original_text": "CHED Memorandum Order (CMO), 2012. Policies, Standards and Guidelines in the Establishment of an Outcomes-Based Education (OBE) System in Higher Education Institutions offering Engineering Programs. No. 37, Series of 2012."
                    },
                    {
                        "sequence": "6",
                        "original_text": "CHED Memorandum Order (CMO), 2008. Policies and Standards for Bachelor of Science in Agriculture (BSA) Program. No. 14, Series of 2008. Retrieved October 30, 2012 from http://www.ched.gov.ph/chedwww/index.php/eng/Information/CHED-Memorandum-Orders/2008-CHED-Memorandum-Orders"
                    },
                    {
                        "sequence": "7",
                        "original_text": "Cruz, R.V.O., Pulhin, J.M. and Mendoza M.D. 2011. Reinventing CFNR: Leading the Way in Integrated Tropical Forest and Natural Resource Management Education, Research and Governance (2011-2025). A Centennial Professorial Chair Lecture delivered in December 2011 at the Nicolas P. Lansigan Hall, College of Forestry and Natural Resources, University of the Philippines Los Banos, College, Laguna, Philippines."
                    },
                    {
                        "sequence": "8",
                        "doi": "10.11178/jdsa.5.47",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Enhancing Food Security in the Context of Climate Change and the Role of Higher Education Institutions in the Philippines"
                            }
                        ],
                        "volume": "5",
                        "issue": "1",
                        "first_page": "47",
                        "last_page": "63",
                        "publication_date": {
                            "publication_year": "2010"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Espaldon",
                                        "first_name": "Maria Victoria O."
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Professor, School of Environmental Science and Management, University of the Philippines Los Ba&ntilde;os, College",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "2",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Zamora",
                                        "first_name": "Oscar B."
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Professor, College of Agriculture, University of the Philippines Los Ba&ntilde;os, College",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "3",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Perez",
                                        "first_name": "Rosa T."
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Climate Change Specialist, Manila Observatory, Ateneo de Manila University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "Espaldon, M.V.O., Zamora, O.B. and Perez, R.T., 2010. Enhancing Food Security in the Context of Climate Change and the Role of Higher Education Institutions in the Philippines. J. Dev. .Sus. Agr. 5: 47-63."
                    },
                    {
                        "sequence": "9",
                        "original_text": "FAO (Food and Agriculture Organization), 2009. Food Security and Agricultural Mitigation in Developing Countries: Options for Capturing Synergies. FAO, Rome, Italy."
                    },
                    {
                        "sequence": "10",
                        "original_text": "FMB (Forest Management Bureau), 2009. Philippines Forestry Outlook Study in Asia-Pacific Forestry Sector Outlook Study II, FAO Working Paper Series. Working Paper No: APFSOS IIWP/2009/10. Retrieved on October 22, 2012 from http://www.fao.org/docrep/014/am255e/am255e00.pdf"
                    },
                    {
                        "sequence": "11",
                        "original_text": "Habito, C.F and Briones R.M., 2005. Philippine Agriculture over the Years: Performance, Policies and Pitfalls. Paper presented during the conference on &ldquo;Policies to Strengthen Productivity in the Philippines&rsquo;, June 27, 2005, Makati City, Philippines."
                    },
                    {
                        "sequence": "12",
                        "original_text": "Haub, C. and M.M. Kent, 2009. World Population Data Sheet. Retrieved October 30, 2012 from http://www.prb.org/pdf09/09wpds_eng.pdf."
                    },
                    {
                        "sequence": "13",
                        "original_text": "Killen, R., 2000. Outcomes-based education: Principles and possibilities. Unpublished manuscript, University of Newcastle, Faculty of Education. Retrieved on October 29, 2012 from http://drjj.uitm.edu.my/DRJJ/CONFERENCE/UPSI/OBEKillen.pdf"
                    },
                    {
                        "sequence": "14",
                        "original_text": "Nair, C.T.S. Undated. What does the future hold for forestry education? Retrieved October 25, 2012 from http://www.fao.org/docrep/007/y5382e/y5382e02.htm#P23_5619"
                    },
                    {
                        "sequence": "15",
                        "original_text": "NEDA (National Economic Development Authority). 2011. Philippine Development Plan 2011-2016. Retrieved December 15, 2012 from http://www.neda.gov.ph"
                    },
                    {
                        "sequence": "16",
                        "original_text": "NSCB (National Statistics and Coordination Board). 2012. Statistics on agriculture and fisheries. Retrieved December 15, 2012 from http://www.nscb.gov.ph"
                    },
                    {
                        "sequence": "17",
                        "original_text": "Osias, T., Tacardon, L. and Pedroso, L. (undated). People Beyond Numbers. The road to population stabilization of in the Philippines. Retrieved October 28, 2012 from http://www.gillespiefoundation.org/uploads/Philippines_Report.pdf"
                    },
                    {
                        "sequence": "18",
                        "original_text": "PEM (Philippines Environment Monitor). 2003. Water Quality. Published by the Department of Environment and Natural Resources, the Environmental Management Bureau and the World Bank Group, Country Office Manila. Retrieved on October 26, 2012 from http://worldbank.org/country/philippines"
                    },
                    {
                        "sequence": "19",
                        "original_text": "PFS (Philippine Forestry Statistics), 2011. Philippine Forestry Statistics, Published by Forestry Management Bureau, Department of Environment and Natural Resources, Republic of the Philippines. Retrieved on October 26, 2012 from http://forestry.denr.gov.ph/2011PFS.pdf"
                    },
                    {
                        "sequence": "20",
                        "original_text": "PIDS (Philippine Institute for Development Studies) 2011. What&rsquo;s in store for AFNR Graduates in the Philippines? Dev. Res. News. 29 (4), July-August 2011, ISSN 0115-9097."
                    },
                    {
                        "sequence": "21",
                        "original_text": "Rebugio L.L. and Camacho, L.D. Reorienting forestry education to sustainable management. J. Environ. Sci. Manag. 6 (2): 49-58."
                    },
                    {
                        "sequence": "22",
                        "original_text": "Tolentino, B., David, C., Balisacan, A. and Intal P., 2001. &ldquo;Strategic Actions to Rapidly Ensure Food Security and Rural Growth in the.&rdquo; In <I>Yellow Paper II: The Post-Erap Reform Agenda. </I>Retrieved on October 24, 2012 from http://www.aer.ph/images/stories/projects/yp2/agri.pdf"
                    },
                    {
                        "sequence": "23",
                        "original_text": "Zundel, P.E. and Needham, T.D., 2000. Outcomes-Based Forestry Education. Application in New Brusnwick. J. For., 98 (2): 30-35."
                    }
                ]
            }
        }
        entity_dict = entity_dict["data"]
        jalc_processor = JalcProcessing()
        authors_list = jalc_processor.get_authors(entity_dict)
        authors_strings_list, _ = jalc_processor.get_agents_strings_list(entity_dict["doi"], authors_list)
        expected_authors_list = ['豊田, 理理子', '豊田, 理純一朗', '豊田, 理純一朗']
        self.assertEqual(authors_strings_list, expected_authors_list)


    def test_get_agents_strings_list_inverted_names(self):
        # One author with an ORCID has as a name the surname of another
        # The authors' information related to this publication are false, they have been used just for testing purposes

        entity_dict = {
            "status": "OK",
            "apiType": "doi",
            "apiVersion": "1.0.0",
            "message": {
                "total": 1,
                "rows": 1,
                "totalPages": 1,
                "page": 1
            },
            "data": {
                "siteId": "SI/JST.JSTAGE",
                "content_type": "JA",
                "doi": "10.11230/jsts.17.2_1",
                "url": "https://doi.org/10.11230/jsts.17.2_1",
                "ra": "JaLC",
                "prefix": "10.11230",
                "site_name": "J-STAGE",
                "publisher_list": [
                    {
                        "publisher_name": "Japanese Rocket Society",
                        "lang": "en"
                    },
                    {
                        "publisher_name": "特定非営利活動法人　日本ロケット協会",
                        "lang": "ja"
                    }
                ],
                "title_list": [
                    {
                        "lang": "en",
                        "title": "Diamond Synthesis with Completely Closed Chamber from Solid or Liquid Carbon Sources, In-Situ Analysis of Gaseous Species and the Possible Reaction Model"
                    }
                ],
                "creator_list": [
                    {
                        "sequence": "1",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "TAKAGI",
                                "first_name": "Yoshiki"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Teikyo University of Science and Technology",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "2",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "Yoshiki",
                                "first_name": "TAKAGI"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Teikyo University of Science and Technology",
                                "sequence": "2",
                                "lang": "en"
                            }
                        ]
                    }
                ],
                "publication_date": {
                    "publication_year": "2001"
                },
                "relation_list": [
                    {
                        "content": "https://www.jstage.jst.go.jp/article/jsts/17/2/17_2_1/_pdf",
                        "type": "URL",
                        "relation": "fullTextPdf"
                    }
                ],
                "content_language": "en",
                "updated_date": "2014-12-09",
                "article_type": "pub",
                "journal_id_list": [
                    {
                        "journal_id": "0911-551X",
                        "type": "ISSN",
                        "issn_type": "print"
                    },
                    {
                        "journal_id": "2186-4772",
                        "type": "ISSN",
                        "issn_type": "online"
                    },
                    {
                        "journal_id": "jsts",
                        "type": "JID"
                    }
                ],
                "journal_title_name_list": [
                    {
                        "journal_title_name": "The Journal of Space Technology and Science",
                        "type": "full",
                        "lang": "en"
                    },
                    {
                        "journal_title_name": "JSTS",
                        "type": "abbreviation",
                        "lang": "en"
                    }
                ],
                "journal_classification": "01",
                "journal_txt_lang": "en",
                "recorded_year": "1985-2013",
                "volume": "17",
                "issue": "2",
                "first_page": "2_1",
                "last_page": "2_7",
                "date": "2013-08-21",
                "keyword_list": [
                    {
                        "keyword": "Microgravity",
                        "sequence": "1",
                        "lang": "en"
                    },
                    {
                        "keyword": "Diamond synthesis",
                        "sequence": "2",
                        "lang": "en"
                    }
                ],
                "citation_list": [
                    {
                        "sequence": "1",
                        "doi": "10.1063/1.1656693",
                        "volume": "39",
                        "first_page": "2915",
                        "publication_date": {
                            "publication_year": "1968"
                        },
                        "original_text": "1) J. C. Angus, H. A. Will and W S. Stanko: Journal of Applied Physics 39, 2915 (1968)."
                    },
                    {
                        "sequence": "2",
                        "doi": "10.1016/0022-0248(83)90411-6",
                        "journal_title_name_list": [
                            {
                                "journal_title_name": "Journal of Crystal Growth",
                                "lang": "ja"
                            }
                        ],
                        "volume": "62",
                        "first_page": "642",
                        "publication_date": {
                            "publication_year": "1983"
                        },
                        "content_language": "ja",
                        "original_text": "2) M. Kamo, Y. Sato, S. Matsumoto, H. Setaka: Journal of Crystal Growth 62, 642 (1983)."
                    },
                    {
                        "sequence": "3",
                        "original_text": "3) Genchi. Y, Yasuda and T. Komiyama. H: International Chemical Engineering 32(3), 560-569 (1992)."
                    },
                    {
                        "sequence": "3",
                        "original_text": "4) Tsang. R S. May: International Chemical Engineering 32(3), 560-569 (1992)."
                    },
                    {
                        "sequence": "4",
                        "original_text": "3) Genchi. Y, Yasuda and T. Komiyama. H: International Chemical Engineering 32(3), 560-569 (1992)."
                    },
                    {
                        "sequence": "4",
                        "original_text": "4) Tsang. R S. May: International Chemical Engineering 32(3), 560-569 (1992)."
                    },
                    {
                        "sequence": "5",
                        "original_text": "5) Y. Hirose: Proceeding of 1 st International Conference for New Diamond Science and Technology P.51, KTK Terra Publish. (Tokyo, 1988)."
                    },
                    {
                        "sequence": "6",
                        "original_text": "6) Y. Takagi, S. Sato, K. Kaigawa, A. B. Sawaoka and L. L. Regel: Microgravity Quaterly, 2(1), 39-42 (1992)."
                    },
                    {
                        "sequence": "7",
                        "original_text": "7) S. Sato, K. Kaigawa, Y. Takagi, Y. Hirose and A. B. Sawaoka: Journal of the Japan Society of Microgravity Application 10(1), 38-45 (1993)."
                    },
                    {
                        "sequence": "8",
                        "original_text": "8) Y. Tanabe, K. Kaigawa, Y. Takagi and A. B. Sawaoka: Journal of the Japan Society of Microgravity Application 11(2), 71-79 ( 1994)."
                    },
                    {
                        "sequence": "9",
                        "original_text": "9) Y. Takagi, K. Hibiya and H. Takeuchi: Journal of the Japan Society of Microgravity Application 13(4), 225-233 (1996)."
                    },
                    {
                        "sequence": "10",
                        "original_text": "10) Y. Takagi, L. L. Regel and W R Wilcox: Transaction of the Materials Research Society of Japan 24(4), 513-518 (1999)."
                    },
                    {
                        "sequence": "11",
                        "original_text": "11) Y. Takagi, L. L. Regel and W R Wilcox: Journal, of the Japan Society of Microgravity Application 15(3), 140-145 (1998)."
                    },
                    {
                        "sequence": "12",
                        "original_text": "12) Y. Takagi, M. Suzuki, H. Abe and Y. Inatomi: Journal, of the Japan Society of Microgravity Application 17(3), 159-165 (2000)."
                    },
                    {
                        "sequence": "13",
                        "original_text": "13) K. Fabisiak, et al., Diamond and Related Materials, 1(1992) pp. 77-82."
                    },
                    {
                        "sequence": "14",
                        "original_text": "14) M. Uede &amp; Y. Takagi: J. Mater. Res., 16 (11), 3069-3072 (2001)."
                    },
                    {
                        "sequence": "15",
                        "original_text": "15) M. Okoshi, et al., Applied Surface Science, 154-155 (2000) pp.376-381."
                    },
                    {
                        "sequence": "16",
                        "original_text": "16) F. Onishi, R Hayashi, Y. Takagi, Proceedings of 2001 MRS Spring meeting, San Francisco,(submitted)."
                    },
                    {
                        "sequence": "17",
                        "doi": "10.1016/0038-1098(88)91128-3",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Raman spectra of diamondlike amorphous carbon films."
                            }
                        ],
                        "volume": "66",
                        "issue": "11",
                        "first_page": "1177",
                        "last_page": "1180",
                        "publication_date": {
                            "publication_year": "1988"
                        },
                        "content_language": "en",
                        "original_text": "17) M. Yoshikawa, G. Katagiri, H. Ishida and A. Ishitani: Solid State Communication, 66, 1177 (1988)."
                    },
                    {
                        "sequence": "18",
                        "original_text": "18) R Gat and J. C. Angus: Journal of Applied Physics 74, 5981-5989 (1993)."
                    }
                ]
            }
        }
        entity_dict = entity_dict["data"]
        jalc_processor = JalcProcessing(orcid_index=IOD)
        authors_list = jalc_processor.get_authors(entity_dict)
        authors_strings_list, _ = jalc_processor.get_agents_strings_list(entity_dict["doi"], authors_list)
        expected_authors_list = ['TAKAGI, Yoshiki [orcid:0000-0001-9597-7030]', 'Yoshiki, TAKAGI']

        self.assertEqual(authors_strings_list, expected_authors_list)


    def test_get_publisher_name_cited_with_mapping1(self):
        # The prefix is in the publishers' mapping
        jalc_processor = JalcProcessing(publishers_filepath_jalc=PUBLISHERS_MAPPING, citing=False)
        item_dict = {
            "sequence": "11",
            "doi": "10.1093/comjnl/4.4.332",
            "content_language": "en",
            "original_text": "11) FRANCIS J. G. F. The QR transformation, parts I and II. Computer Journal.  (1962)  vol.4, 265-271,  p.332-345.  doi:10.1093/comjnl/4.4.332"
        }
        publisher_name = jalc_processor.get_publisher_name(item_dict)
        self.assertEqual(publisher_name, 'Oxford University Press (OUP) [crossref:286]')

    def test_get_publisher_name_cited_with_mapping2(self):
        # The prefix is not in the publishers' mapping
        jalc_processor = JalcProcessing(publishers_filepath_jalc=PUBLISHERS_MAPPING, citing=False)
        item_dict = {
            "sequence": "3",
            "doi": "10.2307/1252042",
            "journal_title_name_list": [
                {
                    "journal_title_name": "Journal of Marketing",
                    "lang": "en"
                }
            ],
            "title_list": [
                {
                    "lang": "en",
                    "title": "The impact of physical surroundings on customers and employees"
                }
            ],
            "volume": "56",
            "issue": "2",
            "first_page": "57",
            "last_page": "71",
            "publication_date": {
                "publication_year": "1992"
            },
            "creator_list": [
                {
                    "sequence": "1",
                    "type": "person",
                    "names": [
                        {
                            "lang": "en",
                            "last_name": "Bitner",
                            "first_name": "M. J."
                        }
                    ]
                }
            ],
            "content_language": "en",
            "original_text": "Bitner, M. J. (1992), &quot;Servicecapes : The Impact of Physical Surroundings on Customers and Employees, &quot; Journal of Marketing, Vol. 56, pp. 57-71."
        }
        publisher_name = jalc_processor.get_publisher_name(item_dict)
        self.assertEqual(publisher_name, '')

    def test_get_publisher_name_cited_without_mapping(self):
        item_dict = {
            "sequence": "11",
            "doi": "10.1093/comjnl/4.4.332",
            "content_language": "en",
            "original_text": "11) FRANCIS J. G. F. The QR transformation, parts I and II. Computer Journal.  (1962)  vol.4, 265-271,  p.332-345.  doi:10.1093/comjnl/4.4.332"
        }
        jalc_processor = JalcProcessing(citing = False)
        publisher_name = jalc_processor.get_publisher_name(item_dict)
        self.assertEqual(publisher_name, '')

    def test_get_venue(self):
        with open(DATA, "r", encoding="utf-8") as content:
            data = json.load(content)
        item_dict = data["data"]
        jalc_processor = JalcProcessing()
        venue_name = jalc_processor.get_venue(item_dict)
        self.assertEqual(venue_name, 'The Journal of Space Technology and Science [issn:0911-551X issn:2186-4772 jid:jsts]')

    def test_to_validated_venue_id_list(self):
        id_dict_list_1 = [{
            "journal_id": "1880-3016",
            "type": "ISSN",
            "issn_type": "print"
        },
            {
                "journal_id": "1880-3024",
                "type": "ISSN",
                "issn_type": "online"
            },
            {
                "journal_id": "jdsa",
                "type": "JID"
            }]
        id_dict_list_2 = [{
            "journal_id": "1880-3016",
            "type": "ISSN",
            "issn_type": "print"
        },
            {
                "journal_id": "1880-3024",
                "type": "ISSN",
                "issn_type": "online"
            },
            {
                "journal_id": "jdsa1623",
                "type": "JID"
            }]

        expected1 = ["issn:1880-3016", "issn:1880-3024", "jid:jdsa"]
        expected2 = ["issn:1880-3016", "issn:1880-3024"]

        jalc_processor = JalcProcessing()
        outp = jalc_processor.to_validated_venue_id_list(id_dict_list_1)
        # the JID id is not valid
        outp2 = jalc_processor.to_validated_venue_id_list(id_dict_list_2)

        self.assertEqual(outp, expected1)
        self.assertEqual(outp2, expected2)

    def test_get_venue_without_full(self):
        item_dict = {
            "status": "OK",
            "apiType": "doi",
            "apiVersion": "1.0.0",
            "message": {
                "total": 1,
                "rows": 1,
                "totalPages": 1,
                "page": 1
            },
            "data": {
                "siteId": "SI/JST.JSTAGE",
                "content_type": "JA",
                "doi": "10.11230/jsts.17.1_11",
                "url": "https://doi.org/10.11230/jsts.17.1_11",
                "ra": "JaLC",
                "prefix": "10.11230",
                "site_name": "J-STAGE",
                "publisher_list": [
                    {
                        "publisher_name": "Japanese Rocket Society",
                        "lang": "en"
                    },
                    {
                        "publisher_name": "特定非営利活動法人　日本ロケット協会",
                        "lang": "ja"
                    }
                ],
                "title_list": [
                    {
                        "lang": "en",
                        "title": "Development of an Airtight Recirculating Zooplankton Culture Device for Closed Ecological Recirculating Aquaculture System (CERAS)"
                    }
                ],
                "creator_list": [
                    {
                        "sequence": "1",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "OMORI",
                                "first_name": "Katsunori"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "National Aerospace Laboratory of Japan",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "2",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "WATANABE",
                                "first_name": "Shigeki"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Tokyo University of Fisheries",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "3",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "ENDO",
                                "first_name": "Masato"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Tokyo University of Fisheries",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "4",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "TAKEUCHI",
                                "first_name": "Toshio"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Tokyo University of Fisheries",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "5",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "OGUCHI",
                                "first_name": "Mitsuo"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "National Aerospace Laboratory of Japan",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    }
                ],
                "publication_date": {
                    "publication_year": "2001"
                },
                "relation_list": [
                    {
                        "content": "https://www.jstage.jst.go.jp/article/jsts/17/1/17_1_11/_pdf",
                        "type": "URL",
                        "relation": "fullTextPdf"
                    }
                ],
                "content_language": "en",
                "updated_date": "2014-12-09",
                "article_type": "pub",
                "journal_id_list": [
                    {
                        "journal_id": "0911-551X",
                        "type": "ISSN",
                        "issn_type": "print"
                    },
                    {
                        "journal_id": "2186-4772",
                        "type": "ISSN",
                        "issn_type": "online"
                    },
                    {
                        "journal_id": "jsts",
                        "type": "JID"
                    }
                ],
                "journal_title_name_list": [
                    {
                        "journal_title_name": "JSTS",
                        "type": "abbreviation",
                        "lang": "en"
                    }
                ],
                "journal_classification": "01",
                "journal_txt_lang": "en",
                "recorded_year": "1985-2013",
                "volume": "17",
                "issue": "1",
                "first_page": "1_11",
                "last_page": "1_17",
                "date": "2013-08-21",
                "keyword_list": [
                    {
                        "keyword": "CERAS",
                        "sequence": "1",
                        "lang": "en"
                    },
                    {
                        "keyword": "Closed Ecological Recirculating Aquaculture System",
                        "sequence": "2",
                        "lang": "en"
                    },
                    {
                        "keyword": "Airtight Recirculating Zooplankton Culture Device",
                        "sequence": "3",
                        "lang": "en"
                    }
                ],
                "citation_list": [
                    {
                        "sequence": "1",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Japan's activities on CELSS in space."
                            }
                        ],
                        "volume": "96",
                        "first_page": "605",
                        "last_page": "616",
                        "publication_date": {
                            "publication_year": "1997"
                        },
                        "content_language": "en",
                        "original_text": "1) S. Kibe and K. Suzuki: Japan&rsquo;s Activities on CELSS in Space. Adv. in the Astronautical Sci., 96, 605-616 (1997)."
                    },
                    {
                        "sequence": "2",
                        "doi": "10.11450/seitaikogaku1989.10.1",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Study on the Development of Closed Ecological Recirculating Aquaculture System(CERAS). I. Development of Fish-rearing Closed Tank."
                            }
                        ],
                        "volume": "10",
                        "issue": "1",
                        "first_page": "1",
                        "last_page": "4",
                        "publication_date": {
                            "publication_year": "1997"
                        },
                        "content_language": "ja",
                        "original_text": "2) T. Takeuchi, S. Noto, G. Yoshizaki, M. Toyobe, R. Kanki, M. Oguchi, S Kibe, and H. Tanaka: Study on the Development of Closed Ecological Recirculating Aquaculture System (CERAS) 1. Development of Fish-rearing Closed Tank. CELSS J., 10(1), 1-4 (1997)."
                    },
                    {
                        "sequence": "3",
                        "volume": "352",
                        "first_page": "59",
                        "publication_date": {
                            "publication_year": "1997"
                        },
                        "original_text": "3) M. I. Gladyshev, A. P. Tolomeev and A. G. Degermendzhi: Dependence on Growth Rate of Moina macrocopa on the Concentration of Chlorella vulgaris in Differential-flow and Closed Fermenters. Doklady Biological. Sci., 352, 59-61 (1997)."
                    },
                    {
                        "sequence": "4",
                        "volume": "25",
                        "first_page": "1",
                        "publication_date": {
                            "publication_year": "1991"
                        },
                        "original_text": "4) J. Urabe: Effect of Food Concentration on Growth, Reproduction and Survivorship of Bosmina longirostris (Cladocera): an Experiment Study. Freshwater Biol., 25, 1-8 (1991)."
                    },
                    {
                        "sequence": "5",
                        "original_text": "5) K. Omori, T. Takeuchi, and M. Oguchi: The Trial Model of the Recirculating Zooplankton-culture System. Eco-Engineering, 14(2), (2002) in press."
                    },
                    {
                        "sequence": "6",
                        "volume": "25",
                        "issue": "4",
                        "first_page": "134",
                        "publication_date": {
                            "publication_year": "1978"
                        },
                        "original_text": "6) T. Mochizuki, H. Shimizu, M. Tanaka, and K. Endo: Studies on Growth of Zooplankton. 1. Growth Rate of Batch Culture. Aqriculture, 25(4), 134-137 (1978)."
                    },
                    {
                        "sequence": "7",
                        "volume": "7",
                        "issue": "11",
                        "first_page": "44",
                        "publication_date": {
                            "publication_year": "1970"
                        },
                        "original_text": "7) S. Kitamura: Nutrition and Production of Daphnid. Fish Culture. 7(11), 44-49 (1970)."
                    },
                    {
                        "sequence": "8",
                        "volume": "18",
                        "issue": "7",
                        "first_page": "66",
                        "publication_date": {
                            "publication_year": "1981"
                        },
                        "original_text": "8) A. Oka: The Culture of Freshwater Daphnid. Fish Culture, 18(7), 66-68 (1981)."
                    },
                    {
                        "sequence": "9",
                        "first_page": "737pp",
                        "publication_date": {
                            "publication_year": "1991"
                        },
                        "original_text": "9) Agriculture, Forestry and Fisheries Research Council Secretariat, Ministry of Agriculture, Forestry and Fisheries (ed.): Biomass Conversion Program -Utilize Abundant Resources-, KORIN Publishing, Co., Ltd., Tokyo, 737pp (1991)."
                    }
                ]
            }
        }
        jalc_processor = JalcProcessing()
        venue_name = jalc_processor.get_venue(item_dict["data"])
        self.assertEqual(venue_name, 'JSTS [issn:0911-551X issn:2186-4772 jid:jsts]')

    def test_get_pages_with_underscore(self):
        # first_page: 1_34, last_page: 1_48
        item_dict = {
            "status": "OK",
            "apiType": "doi",
            "apiVersion": "1.0.0",
            "message": {
                "total": 1,
                "rows": 1,
                "totalPages": 1,
                "page": 1
            },
            "data": {
                "siteId": "SI/JST.JSTAGE",
                "content_type": "JA",
                "doi": "10.11230/jsts.18.1_34",
                "url": "https://doi.org/10.11230/jsts.18.1_34",
                "ra": "JaLC",
                "prefix": "10.11230",
                "site_name": "J-STAGE",
                "publisher_list": [
                    {
                        "publisher_name": "Japanese Rocket Society",
                        "lang": "en"
                    },
                    {
                        "publisher_name": "特定非営利活動法人　日本ロケット協会",
                        "lang": "ja"
                    }
                ],
                "title_list": [
                    {
                        "lang": "en",
                        "title": "Atomic Oxygen Protections in Polymeric Systems: A Review"
                    }
                ],
                "creator_list": [
                    {
                        "sequence": "1",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "RIMDUSIT",
                                "first_name": "Sarawut"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Chulalongkorn University",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "2",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "YOKOTA",
                                "first_name": "Rikio"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Chulalongkorn University",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    }
                ],
                "publication_date": {
                    "publication_year": "2002"
                },
                "relation_list": [
                    {
                        "content": "https://www.jstage.jst.go.jp/article/jsts/18/1/18_1_34/_pdf",
                        "type": "URL",
                        "relation": "fullTextPdf"
                    }
                ],
                "content_language": "en",
                "updated_date": "2014-12-09",
                "article_type": "pub",
                "journal_id_list": [
                    {
                        "journal_id": "0911-551X",
                        "type": "ISSN",
                        "issn_type": "print"
                    },
                    {
                        "journal_id": "2186-4772",
                        "type": "ISSN",
                        "issn_type": "online"
                    },
                    {
                        "journal_id": "jsts",
                        "type": "JID"
                    }
                ],
                "journal_title_name_list": [
                    {
                        "journal_title_name": "The Journal of Space Technology and Science",
                        "type": "full",
                        "lang": "en"
                    },
                    {
                        "journal_title_name": "JSTS",
                        "type": "abbreviation",
                        "lang": "en"
                    }
                ],
                "journal_classification": "01",
                "journal_txt_lang": "en",
                "recorded_year": "1985-2013",
                "volume": "18",
                "issue": "1",
                "first_page": "1_34",
                "last_page": "1_48",
                "date": "2013-08-18",
                "keyword_list": [
                    {
                        "keyword": "Atomic Oxygen",
                        "sequence": "1",
                        "lang": "en"
                    },
                    {
                        "keyword": "Immiscible Blends",
                        "sequence": "2",
                        "lang": "en"
                    },
                    {
                        "keyword": "Phosphorus-containing Polyimide",
                        "sequence": "3",
                        "lang": "en"
                    },
                    {
                        "keyword": "Silicon-containing Polyimide",
                        "sequence": "4",
                        "lang": "en"
                    },
                    {
                        "keyword": "MLI",
                        "sequence": "5",
                        "lang": "en"
                    }
                ],
                "citation_list": [
                    {
                        "sequence": "1",
                        "original_text": "1) R. Yokota, Aerospace Materials, Chapter 5, B. Cantor, H. Assender, and P. Grant, eds., Institute ofPhysics Publishing, Bristol (2001) p. 47."
                    },
                    {
                        "sequence": "2",
                        "original_text": "2) C.K. Krishnaprakas, K. Badari Narayana, and Pradip Dutta, Cryogenics, 40 (2000) p.431."
                    },
                    {
                        "sequence": "3",
                        "doi": "10.2494/photopolymer.12.209",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Recent Trends and Space Applications of Poylmides."
                            }
                        ],
                        "volume": "12",
                        "issue": "2",
                        "first_page": "209",
                        "last_page": "216",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "YOKOTA",
                                        "first_name": "RIKIO"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "3) R. Yokota, J. Photopolym. Sci. Tech., 12 (1999) p.209."
                    },
                    {
                        "sequence": "4",
                        "original_text": "4) E.M. Silverman, Environmental Effects on Spacecraft: LEO Materials Selection Guide, NASA Contractor Report 4661Part1 and Part 2 (1995)."
                    },
                    {
                        "sequence": "5",
                        "first_page": "1999",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "original_text": "5) D. Dooling and M.M. Finckenor, Material Selection Guidelines to Limit Atomic Oxygen Effects on Spacecraft Surfaces, NASA/TP-1999-209260 (1999)."
                    },
                    {
                        "sequence": "6",
                        "original_text": "6) E. Grossman, and I. Gouzman, Nuclear Instruments cind Methods in Physics Research B (2003) in press."
                    },
                    {
                        "sequence": "7",
                        "original_text": "7) R. Yokota, A. Ohnishi, Y. Hashimoto, K. Toki, S. Kuroda, K. Akahori, and H. Nagano, Proc. 7th Materials in Space Environment, Toulouse, France, 16-20 Jurie 1997, p.293."
                    },
                    {
                        "sequence": "8",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Atomic oxygen effects on polymer-based materials."
                            }
                        ],
                        "volume": "69",
                        "issue": "8/9",
                        "first_page": "1190",
                        "last_page": "1208",
                        "publication_date": {
                            "publication_year": "1991"
                        },
                        "content_language": "en",
                        "original_text": "8) RC. Tennyson, Can. J. Phys., 69 (1991) p.1190."
                    },
                    {
                        "sequence": "9",
                        "original_text": "9) R.H. Hansen, J.V. Pascale, T. De Benedictis, and P.M. Rentzepis, J. Polym. Sci.: Part A, 3 (1965) p.2205."
                    },
                    {
                        "sequence": "10",
                        "doi": "10.1007/BF00354389",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Effect of low earth orbit atomic oxygen on spacecraft materials."
                            }
                        ],
                        "volume": "30",
                        "issue": "2",
                        "first_page": "281",
                        "last_page": "307",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "10) M. Raja Reddy, J. Mat. Sci., 30 (1995) p.281."
                    },
                    {
                        "sequence": "11",
                        "doi": "10.1007/BF00354390",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Atomic oxygen resistant coatings for low earth orbit space structures."
                            }
                        ],
                        "volume": "30",
                        "issue": "2",
                        "first_page": "308",
                        "last_page": "320",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "11) S. Packirisamy, D. Schwam, and M.H. Litt, J. Mat. Sci., 30 (1995) p.308."
                    },
                    {
                        "sequence": "12",
                        "original_text": "12) L.L. Fewell, J. Appl. Polym. Sci., 41 (1990) p.391."
                    },
                    {
                        "sequence": "13",
                        "original_text": "13) M. Raja Reddy, N. Srinivasamurthy, and B.L. Agrawal, ESA J., 16 (1992) p.193."
                    },
                    {
                        "sequence": "14",
                        "doi": "10.1088/0954-0083/11/1/013",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Protection of polymetric materials from atomic oxygen."
                            }
                        ],
                        "volume": "11",
                        "issue": "1",
                        "first_page": "157",
                        "last_page": "165",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "content_language": "en",
                        "original_text": "14) RC. Tennyson, High Perform. Polym., 11 (1999) p.157."
                    },
                    {
                        "sequence": "15",
                        "original_text": "15) Y. Huang, J. Liu, I. Ball, and T.K. Minton, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1788."
                    },
                    {
                        "sequence": "16",
                        "original_text": "16) B.A. Banks, and R. Demko, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.820."
                    },
                    {
                        "sequence": "17",
                        "original_text": "17) M.L. lliingsworth, J.A. Betancourt, L. He, Y. Chen, J.A. Terschak, B.A. Banks, S.K. Rutledge, and M. Cales, NASAITM-2001-211099 (2001)."
                    },
                    {
                        "sequence": "18",
                        "original_text": "18) R.L. Kiefer, RA. Anderson, M.H.Y. Kim, and S.A. Thibeault, Nuclear Instruments and Methods in Physics Research B, (2003) in press."
                    },
                    {
                        "sequence": "19",
                        "volume": "2002",
                        "issue": "14",
                        "first_page": "2002",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "original_text": "19) C. Park, Z. Ounaies, K.A. Watson, K. Pawlowski, S.E. Lowther, J.W. Connell, E.J. Siochi, J.S. Harrison, and T.L. St Clair, NASA/CR-2002-211940 (2002)."
                    },
                    {
                        "sequence": "20",
                        "original_text": "20) Y. Huang, J. Liu, I. Ball, and T.K. Minton, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1788."
                    },
                    {
                        "sequence": "21",
                        "doi": "10.1177/0954008303015002003",
                        "volume": "15",
                        "first_page": "181",
                        "publication_date": {
                            "publication_year": "2003"
                        },
                        "original_text": "21) C.M. Thompson, J.G. Smith, Jr., andJ.W. Connell, High Perform. Polym., 15 (2003) p.181."
                    },
                    {
                        "sequence": "22",
                        "original_text": "22) K.A. Watson, and J.W. Connell, Proc. Fluoropolymer 2000, Savanah, Georgia, October 15.18, 2000."
                    },
                    {
                        "sequence": "23",
                        "original_text": "23) J.G. Smith Jr., J.W. Connell, and P.M. Hergenrother, Polymer, 35 (1994) p.2834."
                    },
                    {
                        "sequence": "24",
                        "original_text": "24) K.K. de Groh, B.A. Banks, and R. Demko, Proc. 47th Int. SAMPE Symp. Exh., B.M. Rasmussen, L.A. Pilato, and H.S. Kliger, Eds., Long Beach, Califonia, May 12-16, 2002, p.1279."
                    },
                    {
                        "sequence": "25",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "25) K.A. Watson, F.L. Palmteri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "25",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "46) K.A. Watson, F.L. Palmieri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "26",
                        "doi": "10.1246/cl.1997.333",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "In situ Formed Three Layer Film by Isomerization of Fluorinated Polyisoimide in Polyethersulfone as a Matrix Polymer."
                            }
                        ],
                        "issue": "4",
                        "first_page": "333",
                        "last_page": "334",
                        "publication_date": {
                            "publication_year": "1997"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Mochizuki",
                                        "first_name": "Amane"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "2",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Yamada",
                                        "first_name": "Kazuo"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "3",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Ueda",
                                        "first_name": "Mitsuru"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "4",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Yokota",
                                        "first_name": "Rikio"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Institute of Space and Astronautical Science",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "26) A. Mochizuki, K. Yamada, M. Ueda, and R. Yokota, Chem. Letts. (1997) 333."
                    },
                    {
                        "sequence": "27",
                        "original_text": "27) K.K. de Groh, J.R. Gaier, R.L. Hall, M.J. Norris, M.P. Espe, and D.R. Cato, Effects of Heating on Teflon FEP Thermal Control Material from the Hubble Space Telescope, NASAITM-2000-209085 (2000)."
                    },
                    {
                        "sequence": "28",
                        "original_text": "28) K.K. de Groh, and D.C. Smith, Investigation of Teflon FEP Embrittlement on Spacecraft in Low Earth Orbit, NASAITM-113153 (1997)."
                    },
                    {
                        "sequence": "29",
                        "doi": "10.1126/science.2563171",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Effects of buried ionizable amino acids on the reduction potential of recombinant myoglobin."
                            }
                        ],
                        "volume": "243",
                        "issue": "4887",
                        "first_page": "69",
                        "last_page": "72",
                        "publication_date": {
                            "publication_year": "1989"
                        },
                        "content_language": "en",
                        "original_text": "29) McGrath, D. Chen, and J.E. McGrath, Polyimides: Materials, Chemistry and Characterization, C. Feger, M.M. Khojasteh, and J.E. McGrath, eds., Elsevier Science Publishers B.V., Amsterdam (1989) p.69."
                    },
                    {
                        "sequence": "30",
                        "original_text": "30) J. Visentine, W. Kinard, D. Brinker, B.A Banks, and K. Albyn, J. Spacecraft and Rockets, 39 (2002) p.187."
                    },
                    {
                        "sequence": "31",
                        "original_text": "31) B.A. Banks, K.K. de Groh, S.K. Rutledge, and C.A. Haytas, Consequences of Atomic Oxygen Interaction with Silicone and Silicone Contamination on Surfaces in Low Earth Orbit, NASAITM-1999-209179 (1999)."
                    },
                    {
                        "sequence": "32",
                        "original_text": "32) N. Furukawa, Y. Yamada, M. Furukawa, M. Yuasa, and Y. Kimura, J. Polym. Sci., Part A: Polym. Chem., 35 (1997) p.2239."
                    },
                    {
                        "sequence": "33",
                        "original_text": "33) NASDA Rep. No. AU9-R02-K108 (2003)."
                    },
                    {
                        "sequence": "34",
                        "original_text": "34) G.B. Hoflund, RI. Gonzalez, and S.H. Phillips, J. Adhesion Sci. Technol., 15 (2001) p.1199."
                    },
                    {
                        "sequence": "35",
                        "original_text": "35) S.H. Phillips, RI. Gonzalez, R.L. Blanski, B.D. Viers, and G.B. Hoflund, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1488."
                    },
                    {
                        "sequence": "36",
                        "original_text": "36) RI. Ganzalez, Ph.D. Disseration, University ofFlorida, Gainesville, Florida (2002)."
                    },
                    {
                        "sequence": "37",
                        "original_text": "37) RI. Gonzalex, G.B. Holfund, S.A. Svejda, S.H. Phillips, and B.V. Viers, submitted to Macromolecules."
                    },
                    {
                        "sequence": "38",
                        "original_text": "38) J.E. McGrath et.al. US Pat. 5,420,225 (1995), US Pat. 5,407,528 (1995), US Pat. 5,387,629 (1995), and US. Pat. 5,134,207 (1992)."
                    },
                    {
                        "sequence": "39",
                        "doi": "10.1016/0032-3861(95)90668-R",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Oxygen plasma-resistant phenylphosphine oxide-containing polyimides and poly(arylene ether heterocycle)s. 1."
                            }
                        ],
                        "volume": "36",
                        "issue": "1",
                        "first_page": "5",
                        "last_page": "11",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "39) J.W. Connell, J.G. Smith Jr., and P.M. Hergenrother, Polymer, 36 (1995) p.5."
                    },
                    {
                        "sequence": "40",
                        "doi": "10.1016/0032-3861(95)90669-S",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Oxgen plasma-resistant phenylphosphine oxide-containing polyimides and poly(arylene ether heterocycle)s. 2."
                            }
                        ],
                        "volume": "36",
                        "issue": "1",
                        "first_page": "13",
                        "last_page": "19",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "40) J.W. Connell, J.G. Smith Jr., and P.M. Hergenrother, Polymer, 36 (1995) p.13."
                    },
                    {
                        "sequence": "41",
                        "original_text": "41) P. Schuler, R. Haghighat, and H. Mojazza, High Perform. Polym., 11 (1999) p.113."
                    },
                    {
                        "sequence": "42",
                        "original_text": "42) J.W. Connell, The Effect of Low Earth Orbit Atomic Oxygen Exposure on Phenylphosphine Oxide-Containing Polymers, 44th International SAMPE Symposium and Exhibition, Long Beach, California, May 23-27, 1999."
                    },
                    {
                        "sequence": "43",
                        "doi": "10.1088/0954-0083/11/1/008",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Electrically conductive space-durable polymeric films for spacecraft thermal and charge control."
                            }
                        ],
                        "volume": "11",
                        "issue": "1",
                        "first_page": "101",
                        "last_page": "111",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "content_language": "en",
                        "original_text": "43) J. Lennhoff, G. Harris, J. Vaughn, D. Edwards, and J. Zwiener, High Perform. Polym., 11 (1999) p.101."
                    },
                    {
                        "sequence": "44",
                        "original_text": "44) T.C. Chang,, K.H. Wu, and Y.S. Chiu, Polym. Degrad Stability, 63 (1999) p.103."
                    },
                    {
                        "sequence": "45",
                        "doi": "10.1016/S0032-3861(02)00362-2",
                        "original_text": "45) P.M. Hergenrother, K.A. Watson, J.G. Smith Jr., J.W. Connell, and R. Yokota, Polymer, 43 (2002) p.5077."
                    },
                    {
                        "sequence": "46",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "25) K.A. Watson, F.L. Palmteri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "46",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "46) K.A. Watson, F.L. Palmieri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "47",
                        "original_text": "47) K.A. Watson, and J.W. Connell, Space Environmentally Stable Polyimides and Copolyimides, 45th International SAMPE Symposium and Exhibit, Long Beach, California, May 21-25, 2000."
                    },
                    {
                        "sequence": "48",
                        "original_text": "48) C. M. Thompson, J. G. Smith, Jr., K. A. Watson and J. W. Connell, Polyimides Containing Pendent Phosphine Oxide Groups for Space Applications, 34th International SAMPE Technical Conference, Baltimore, Maryland, November 4-7, 2002."
                    },
                    {
                        "sequence": "49",
                        "original_text": "49) H. Zhuang, Ph.D. Dissertation, Virginia Polytechnic Institute and State University, Blacksburg, Virginir (1998)."
                    },
                    {
                        "sequence": "50",
                        "original_text": "50) S. Wang, Ph.D. Dissertation, Virginia Polytechnic Institute and State University, Blacksburg, Virginir (2000)."
                    },
                    {
                        "sequence": "51",
                        "original_text": "51) S. Y. Lu, and I. Hamerton, Prag. Polym. Sci., 27 (2002) p.1661."
                    }
                ]
            }
        }
        jalc_processor = JalcProcessing()
        pages = jalc_processor.get_jalc_pages(item_dict["data"])
        self.assertEqual(pages, '1_34-1_48')


    def test_get_pages_wrong_letter(self):
        #first_page: 1_34b
        item_dict = {
            "status": "OK",
            "apiType": "doi",
            "apiVersion": "1.0.0",
            "message": {
                "total": 1,
                "rows": 1,
                "totalPages": 1,
                "page": 1
            },
            "data": {
                "siteId": "SI/JST.JSTAGE",
                "content_type": "JA",
                "doi": "10.11230/jsts.18.1_34",
                "url": "https://doi.org/10.11230/jsts.18.1_34",
                "ra": "JaLC",
                "prefix": "10.11230",
                "site_name": "J-STAGE",
                "publisher_list": [
                    {
                        "publisher_name": "Japanese Rocket Society",
                        "lang": "en"
                    },
                    {
                        "publisher_name": "特定非営利活動法人　日本ロケット協会",
                        "lang": "ja"
                    }
                ],
                "title_list": [
                    {
                        "lang": "en",
                        "title": "Atomic Oxygen Protections in Polymeric Systems: A Review"
                    }
                ],
                "creator_list": [
                    {
                        "sequence": "1",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "RIMDUSIT",
                                "first_name": "Sarawut"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Chulalongkorn University",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "2",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "YOKOTA",
                                "first_name": "Rikio"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Chulalongkorn University",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    }
                ],
                "publication_date": {
                    "publication_year": "2002"
                },
                "relation_list": [
                    {
                        "content": "https://www.jstage.jst.go.jp/article/jsts/18/1/18_1_34/_pdf",
                        "type": "URL",
                        "relation": "fullTextPdf"
                    }
                ],
                "content_language": "en",
                "updated_date": "2014-12-09",
                "article_type": "pub",
                "journal_id_list": [
                    {
                        "journal_id": "0911-551X",
                        "type": "ISSN",
                        "issn_type": "print"
                    },
                    {
                        "journal_id": "2186-4772",
                        "type": "ISSN",
                        "issn_type": "online"
                    },
                    {
                        "journal_id": "jsts",
                        "type": "JID"
                    }
                ],
                "journal_title_name_list": [
                    {
                        "journal_title_name": "The Journal of Space Technology and Science",
                        "type": "full",
                        "lang": "en"
                    },
                    {
                        "journal_title_name": "JSTS",
                        "type": "abbreviation",
                        "lang": "en"
                    }
                ],
                "journal_classification": "01",
                "journal_txt_lang": "en",
                "recorded_year": "1985-2013",
                "volume": "18",
                "issue": "1",
                "first_page": "1_34b",
                "last_page": "1_48",
                "date": "2013-08-18",
                "keyword_list": [
                    {
                        "keyword": "Atomic Oxygen",
                        "sequence": "1",
                        "lang": "en"
                    },
                    {
                        "keyword": "Immiscible Blends",
                        "sequence": "2",
                        "lang": "en"
                    },
                    {
                        "keyword": "Phosphorus-containing Polyimide",
                        "sequence": "3",
                        "lang": "en"
                    },
                    {
                        "keyword": "Silicon-containing Polyimide",
                        "sequence": "4",
                        "lang": "en"
                    },
                    {
                        "keyword": "MLI",
                        "sequence": "5",
                        "lang": "en"
                    }
                ],
                "citation_list": [
                    {
                        "sequence": "1",
                        "original_text": "1) R. Yokota, Aerospace Materials, Chapter 5, B. Cantor, H. Assender, and P. Grant, eds., Institute ofPhysics Publishing, Bristol (2001) p. 47."
                    },
                    {
                        "sequence": "2",
                        "original_text": "2) C.K. Krishnaprakas, K. Badari Narayana, and Pradip Dutta, Cryogenics, 40 (2000) p.431."
                    },
                    {
                        "sequence": "3",
                        "doi": "10.2494/photopolymer.12.209",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Recent Trends and Space Applications of Poylmides."
                            }
                        ],
                        "volume": "12",
                        "issue": "2",
                        "first_page": "209",
                        "last_page": "216",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "YOKOTA",
                                        "first_name": "RIKIO"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "3) R. Yokota, J. Photopolym. Sci. Tech., 12 (1999) p.209."
                    },
                    {
                        "sequence": "4",
                        "original_text": "4) E.M. Silverman, Environmental Effects on Spacecraft: LEO Materials Selection Guide, NASA Contractor Report 4661Part1 and Part 2 (1995)."
                    },
                    {
                        "sequence": "5",
                        "first_page": "1999",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "original_text": "5) D. Dooling and M.M. Finckenor, Material Selection Guidelines to Limit Atomic Oxygen Effects on Spacecraft Surfaces, NASA/TP-1999-209260 (1999)."
                    },
                    {
                        "sequence": "6",
                        "original_text": "6) E. Grossman, and I. Gouzman, Nuclear Instruments cind Methods in Physics Research B (2003) in press."
                    },
                    {
                        "sequence": "7",
                        "original_text": "7) R. Yokota, A. Ohnishi, Y. Hashimoto, K. Toki, S. Kuroda, K. Akahori, and H. Nagano, Proc. 7th Materials in Space Environment, Toulouse, France, 16-20 Jurie 1997, p.293."
                    },
                    {
                        "sequence": "8",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Atomic oxygen effects on polymer-based materials."
                            }
                        ],
                        "volume": "69",
                        "issue": "8/9",
                        "first_page": "1190",
                        "last_page": "1208",
                        "publication_date": {
                            "publication_year": "1991"
                        },
                        "content_language": "en",
                        "original_text": "8) RC. Tennyson, Can. J. Phys., 69 (1991) p.1190."
                    },
                    {
                        "sequence": "9",
                        "original_text": "9) R.H. Hansen, J.V. Pascale, T. De Benedictis, and P.M. Rentzepis, J. Polym. Sci.: Part A, 3 (1965) p.2205."
                    },
                    {
                        "sequence": "10",
                        "doi": "10.1007/BF00354389",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Effect of low earth orbit atomic oxygen on spacecraft materials."
                            }
                        ],
                        "volume": "30",
                        "issue": "2",
                        "first_page": "281",
                        "last_page": "307",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "10) M. Raja Reddy, J. Mat. Sci., 30 (1995) p.281."
                    },
                    {
                        "sequence": "11",
                        "doi": "10.1007/BF00354390",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Atomic oxygen resistant coatings for low earth orbit space structures."
                            }
                        ],
                        "volume": "30",
                        "issue": "2",
                        "first_page": "308",
                        "last_page": "320",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "11) S. Packirisamy, D. Schwam, and M.H. Litt, J. Mat. Sci., 30 (1995) p.308."
                    },
                    {
                        "sequence": "12",
                        "original_text": "12) L.L. Fewell, J. Appl. Polym. Sci., 41 (1990) p.391."
                    },
                    {
                        "sequence": "13",
                        "original_text": "13) M. Raja Reddy, N. Srinivasamurthy, and B.L. Agrawal, ESA J., 16 (1992) p.193."
                    },
                    {
                        "sequence": "14",
                        "doi": "10.1088/0954-0083/11/1/013",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Protection of polymetric materials from atomic oxygen."
                            }
                        ],
                        "volume": "11",
                        "issue": "1",
                        "first_page": "157",
                        "last_page": "165",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "content_language": "en",
                        "original_text": "14) RC. Tennyson, High Perform. Polym., 11 (1999) p.157."
                    },
                    {
                        "sequence": "15",
                        "original_text": "15) Y. Huang, J. Liu, I. Ball, and T.K. Minton, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1788."
                    },
                    {
                        "sequence": "16",
                        "original_text": "16) B.A. Banks, and R. Demko, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.820."
                    },
                    {
                        "sequence": "17",
                        "original_text": "17) M.L. lliingsworth, J.A. Betancourt, L. He, Y. Chen, J.A. Terschak, B.A. Banks, S.K. Rutledge, and M. Cales, NASAITM-2001-211099 (2001)."
                    },
                    {
                        "sequence": "18",
                        "original_text": "18) R.L. Kiefer, RA. Anderson, M.H.Y. Kim, and S.A. Thibeault, Nuclear Instruments and Methods in Physics Research B, (2003) in press."
                    },
                    {
                        "sequence": "19",
                        "volume": "2002",
                        "issue": "14",
                        "first_page": "2002",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "original_text": "19) C. Park, Z. Ounaies, K.A. Watson, K. Pawlowski, S.E. Lowther, J.W. Connell, E.J. Siochi, J.S. Harrison, and T.L. St Clair, NASA/CR-2002-211940 (2002)."
                    },
                    {
                        "sequence": "20",
                        "original_text": "20) Y. Huang, J. Liu, I. Ball, and T.K. Minton, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1788."
                    },
                    {
                        "sequence": "21",
                        "doi": "10.1177/0954008303015002003",
                        "volume": "15",
                        "first_page": "181",
                        "publication_date": {
                            "publication_year": "2003"
                        },
                        "original_text": "21) C.M. Thompson, J.G. Smith, Jr., andJ.W. Connell, High Perform. Polym., 15 (2003) p.181."
                    },
                    {
                        "sequence": "22",
                        "original_text": "22) K.A. Watson, and J.W. Connell, Proc. Fluoropolymer 2000, Savanah, Georgia, October 15.18, 2000."
                    },
                    {
                        "sequence": "23",
                        "original_text": "23) J.G. Smith Jr., J.W. Connell, and P.M. Hergenrother, Polymer, 35 (1994) p.2834."
                    },
                    {
                        "sequence": "24",
                        "original_text": "24) K.K. de Groh, B.A. Banks, and R. Demko, Proc. 47th Int. SAMPE Symp. Exh., B.M. Rasmussen, L.A. Pilato, and H.S. Kliger, Eds., Long Beach, Califonia, May 12-16, 2002, p.1279."
                    },
                    {
                        "sequence": "25",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "25) K.A. Watson, F.L. Palmteri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "25",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "46) K.A. Watson, F.L. Palmieri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "26",
                        "doi": "10.1246/cl.1997.333",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "In situ Formed Three Layer Film by Isomerization of Fluorinated Polyisoimide in Polyethersulfone as a Matrix Polymer."
                            }
                        ],
                        "issue": "4",
                        "first_page": "333",
                        "last_page": "334",
                        "publication_date": {
                            "publication_year": "1997"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Mochizuki",
                                        "first_name": "Amane"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "2",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Yamada",
                                        "first_name": "Kazuo"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "3",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Ueda",
                                        "first_name": "Mitsuru"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "4",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Yokota",
                                        "first_name": "Rikio"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Institute of Space and Astronautical Science",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "26) A. Mochizuki, K. Yamada, M. Ueda, and R. Yokota, Chem. Letts. (1997) 333."
                    },
                    {
                        "sequence": "27",
                        "original_text": "27) K.K. de Groh, J.R. Gaier, R.L. Hall, M.J. Norris, M.P. Espe, and D.R. Cato, Effects of Heating on Teflon FEP Thermal Control Material from the Hubble Space Telescope, NASAITM-2000-209085 (2000)."
                    },
                    {
                        "sequence": "28",
                        "original_text": "28) K.K. de Groh, and D.C. Smith, Investigation of Teflon FEP Embrittlement on Spacecraft in Low Earth Orbit, NASAITM-113153 (1997)."
                    },
                    {
                        "sequence": "29",
                        "doi": "10.1126/science.2563171",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Effects of buried ionizable amino acids on the reduction potential of recombinant myoglobin."
                            }
                        ],
                        "volume": "243",
                        "issue": "4887",
                        "first_page": "69",
                        "last_page": "72",
                        "publication_date": {
                            "publication_year": "1989"
                        },
                        "content_language": "en",
                        "original_text": "29) McGrath, D. Chen, and J.E. McGrath, Polyimides: Materials, Chemistry and Characterization, C. Feger, M.M. Khojasteh, and J.E. McGrath, eds., Elsevier Science Publishers B.V., Amsterdam (1989) p.69."
                    },
                    {
                        "sequence": "30",
                        "original_text": "30) J. Visentine, W. Kinard, D. Brinker, B.A Banks, and K. Albyn, J. Spacecraft and Rockets, 39 (2002) p.187."
                    },
                    {
                        "sequence": "31",
                        "original_text": "31) B.A. Banks, K.K. de Groh, S.K. Rutledge, and C.A. Haytas, Consequences of Atomic Oxygen Interaction with Silicone and Silicone Contamination on Surfaces in Low Earth Orbit, NASAITM-1999-209179 (1999)."
                    },
                    {
                        "sequence": "32",
                        "original_text": "32) N. Furukawa, Y. Yamada, M. Furukawa, M. Yuasa, and Y. Kimura, J. Polym. Sci., Part A: Polym. Chem., 35 (1997) p.2239."
                    },
                    {
                        "sequence": "33",
                        "original_text": "33) NASDA Rep. No. AU9-R02-K108 (2003)."
                    },
                    {
                        "sequence": "34",
                        "original_text": "34) G.B. Hoflund, RI. Gonzalez, and S.H. Phillips, J. Adhesion Sci. Technol., 15 (2001) p.1199."
                    },
                    {
                        "sequence": "35",
                        "original_text": "35) S.H. Phillips, RI. Gonzalez, R.L. Blanski, B.D. Viers, and G.B. Hoflund, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1488."
                    },
                    {
                        "sequence": "36",
                        "original_text": "36) RI. Ganzalez, Ph.D. Disseration, University ofFlorida, Gainesville, Florida (2002)."
                    },
                    {
                        "sequence": "37",
                        "original_text": "37) RI. Gonzalex, G.B. Holfund, S.A. Svejda, S.H. Phillips, and B.V. Viers, submitted to Macromolecules."
                    },
                    {
                        "sequence": "38",
                        "original_text": "38) J.E. McGrath et.al. US Pat. 5,420,225 (1995), US Pat. 5,407,528 (1995), US Pat. 5,387,629 (1995), and US. Pat. 5,134,207 (1992)."
                    },
                    {
                        "sequence": "39",
                        "doi": "10.1016/0032-3861(95)90668-R",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Oxygen plasma-resistant phenylphosphine oxide-containing polyimides and poly(arylene ether heterocycle)s. 1."
                            }
                        ],
                        "volume": "36",
                        "issue": "1",
                        "first_page": "5",
                        "last_page": "11",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "39) J.W. Connell, J.G. Smith Jr., and P.M. Hergenrother, Polymer, 36 (1995) p.5."
                    },
                    {
                        "sequence": "40",
                        "doi": "10.1016/0032-3861(95)90669-S",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Oxgen plasma-resistant phenylphosphine oxide-containing polyimides and poly(arylene ether heterocycle)s. 2."
                            }
                        ],
                        "volume": "36",
                        "issue": "1",
                        "first_page": "13",
                        "last_page": "19",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "40) J.W. Connell, J.G. Smith Jr., and P.M. Hergenrother, Polymer, 36 (1995) p.13."
                    },
                    {
                        "sequence": "41",
                        "original_text": "41) P. Schuler, R. Haghighat, and H. Mojazza, High Perform. Polym., 11 (1999) p.113."
                    },
                    {
                        "sequence": "42",
                        "original_text": "42) J.W. Connell, The Effect of Low Earth Orbit Atomic Oxygen Exposure on Phenylphosphine Oxide-Containing Polymers, 44th International SAMPE Symposium and Exhibition, Long Beach, California, May 23-27, 1999."
                    },
                    {
                        "sequence": "43",
                        "doi": "10.1088/0954-0083/11/1/008",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Electrically conductive space-durable polymeric films for spacecraft thermal and charge control."
                            }
                        ],
                        "volume": "11",
                        "issue": "1",
                        "first_page": "101",
                        "last_page": "111",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "content_language": "en",
                        "original_text": "43) J. Lennhoff, G. Harris, J. Vaughn, D. Edwards, and J. Zwiener, High Perform. Polym., 11 (1999) p.101."
                    },
                    {
                        "sequence": "44",
                        "original_text": "44) T.C. Chang,, K.H. Wu, and Y.S. Chiu, Polym. Degrad Stability, 63 (1999) p.103."
                    },
                    {
                        "sequence": "45",
                        "doi": "10.1016/S0032-3861(02)00362-2",
                        "original_text": "45) P.M. Hergenrother, K.A. Watson, J.G. Smith Jr., J.W. Connell, and R. Yokota, Polymer, 43 (2002) p.5077."
                    },
                    {
                        "sequence": "46",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "25) K.A. Watson, F.L. Palmteri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "46",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "46) K.A. Watson, F.L. Palmieri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "47",
                        "original_text": "47) K.A. Watson, and J.W. Connell, Space Environmentally Stable Polyimides and Copolyimides, 45th International SAMPE Symposium and Exhibit, Long Beach, California, May 21-25, 2000."
                    },
                    {
                        "sequence": "48",
                        "original_text": "48) C. M. Thompson, J. G. Smith, Jr., K. A. Watson and J. W. Connell, Polyimides Containing Pendent Phosphine Oxide Groups for Space Applications, 34th International SAMPE Technical Conference, Baltimore, Maryland, November 4-7, 2002."
                    },
                    {
                        "sequence": "49",
                        "original_text": "49) H. Zhuang, Ph.D. Dissertation, Virginia Polytechnic Institute and State University, Blacksburg, Virginir (1998)."
                    },
                    {
                        "sequence": "50",
                        "original_text": "50) S. Wang, Ph.D. Dissertation, Virginia Polytechnic Institute and State University, Blacksburg, Virginir (2000)."
                    },
                    {
                        "sequence": "51",
                        "original_text": "51) S. Y. Lu, and I. Hamerton, Prag. Polym. Sci., 27 (2002) p.1661."
                    }
                ]
            }
        }
        jalc_processor = JalcProcessing()
        pages = jalc_processor.get_jalc_pages(item_dict["data"])
        self.assertEqual(pages, '1_34-1_48')

    def test_get_pages_just_one_page(self):
        item_dict = {
            "status": "OK",
            "apiType": "doi",
            "apiVersion": "1.0.0",
            "message": {
                "total": 1,
                "rows": 1,
                "totalPages": 1,
                "page": 1
            },
            "data": {
                "siteId": "SI/JST.JSTAGE",
                "content_type": "JA",
                "doi": "10.11230/jsts.18.1_34",
                "url": "https://doi.org/10.11230/jsts.18.1_34",
                "ra": "JaLC",
                "prefix": "10.11230",
                "site_name": "J-STAGE",
                "publisher_list": [
                    {
                        "publisher_name": "Japanese Rocket Society",
                        "lang": "en"
                    },
                    {
                        "publisher_name": "特定非営利活動法人　日本ロケット協会",
                        "lang": "ja"
                    }
                ],
                "title_list": [
                    {
                        "lang": "en",
                        "title": "Atomic Oxygen Protections in Polymeric Systems: A Review"
                    }
                ],
                "creator_list": [
                    {
                        "sequence": "1",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "RIMDUSIT",
                                "first_name": "Sarawut"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Chulalongkorn University",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "2",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "YOKOTA",
                                "first_name": "Rikio"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Chulalongkorn University",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    }
                ],
                "publication_date": {
                    "publication_year": "2002"
                },
                "relation_list": [
                    {
                        "content": "https://www.jstage.jst.go.jp/article/jsts/18/1/18_1_34/_pdf",
                        "type": "URL",
                        "relation": "fullTextPdf"
                    }
                ],
                "content_language": "en",
                "updated_date": "2014-12-09",
                "article_type": "pub",
                "journal_id_list": [
                    {
                        "journal_id": "0911-551X",
                        "type": "ISSN",
                        "issn_type": "print"
                    },
                    {
                        "journal_id": "2186-4772",
                        "type": "ISSN",
                        "issn_type": "online"
                    },
                    {
                        "journal_id": "jsts",
                        "type": "JID"
                    }
                ],
                "journal_title_name_list": [
                    {
                        "journal_title_name": "The Journal of Space Technology and Science",
                        "type": "full",
                        "lang": "en"
                    },
                    {
                        "journal_title_name": "JSTS",
                        "type": "abbreviation",
                        "lang": "en"
                    }
                ],
                "journal_classification": "01",
                "journal_txt_lang": "en",
                "recorded_year": "1985-2013",
                "volume": "18",
                "issue": "1",
                "first_page": "1_34",
                "date": "2013-08-18",
                "keyword_list": [
                    {
                        "keyword": "Atomic Oxygen",
                        "sequence": "1",
                        "lang": "en"
                    },
                    {
                        "keyword": "Immiscible Blends",
                        "sequence": "2",
                        "lang": "en"
                    },
                    {
                        "keyword": "Phosphorus-containing Polyimide",
                        "sequence": "3",
                        "lang": "en"
                    },
                    {
                        "keyword": "Silicon-containing Polyimide",
                        "sequence": "4",
                        "lang": "en"
                    },
                    {
                        "keyword": "MLI",
                        "sequence": "5",
                        "lang": "en"
                    }
                ],
                "citation_list": [
                    {
                        "sequence": "1",
                        "original_text": "1) R. Yokota, Aerospace Materials, Chapter 5, B. Cantor, H. Assender, and P. Grant, eds., Institute ofPhysics Publishing, Bristol (2001) p. 47."
                    },
                    {
                        "sequence": "2",
                        "original_text": "2) C.K. Krishnaprakas, K. Badari Narayana, and Pradip Dutta, Cryogenics, 40 (2000) p.431."
                    },
                    {
                        "sequence": "3",
                        "doi": "10.2494/photopolymer.12.209",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Recent Trends and Space Applications of Poylmides."
                            }
                        ],
                        "volume": "12",
                        "issue": "2",
                        "first_page": "209",
                        "last_page": "216",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "YOKOTA",
                                        "first_name": "RIKIO"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "3) R. Yokota, J. Photopolym. Sci. Tech., 12 (1999) p.209."
                    },
                    {
                        "sequence": "4",
                        "original_text": "4) E.M. Silverman, Environmental Effects on Spacecraft: LEO Materials Selection Guide, NASA Contractor Report 4661Part1 and Part 2 (1995)."
                    },
                    {
                        "sequence": "5",
                        "first_page": "1999",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "original_text": "5) D. Dooling and M.M. Finckenor, Material Selection Guidelines to Limit Atomic Oxygen Effects on Spacecraft Surfaces, NASA/TP-1999-209260 (1999)."
                    },
                    {
                        "sequence": "6",
                        "original_text": "6) E. Grossman, and I. Gouzman, Nuclear Instruments cind Methods in Physics Research B (2003) in press."
                    },
                    {
                        "sequence": "7",
                        "original_text": "7) R. Yokota, A. Ohnishi, Y. Hashimoto, K. Toki, S. Kuroda, K. Akahori, and H. Nagano, Proc. 7th Materials in Space Environment, Toulouse, France, 16-20 Jurie 1997, p.293."
                    },
                    {
                        "sequence": "8",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Atomic oxygen effects on polymer-based materials."
                            }
                        ],
                        "volume": "69",
                        "issue": "8/9",
                        "first_page": "1190",
                        "last_page": "1208",
                        "publication_date": {
                            "publication_year": "1991"
                        },
                        "content_language": "en",
                        "original_text": "8) RC. Tennyson, Can. J. Phys., 69 (1991) p.1190."
                    },
                    {
                        "sequence": "9",
                        "original_text": "9) R.H. Hansen, J.V. Pascale, T. De Benedictis, and P.M. Rentzepis, J. Polym. Sci.: Part A, 3 (1965) p.2205."
                    },
                    {
                        "sequence": "10",
                        "doi": "10.1007/BF00354389",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Effect of low earth orbit atomic oxygen on spacecraft materials."
                            }
                        ],
                        "volume": "30",
                        "issue": "2",
                        "first_page": "281",
                        "last_page": "307",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "10) M. Raja Reddy, J. Mat. Sci., 30 (1995) p.281."
                    },
                    {
                        "sequence": "11",
                        "doi": "10.1007/BF00354390",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Atomic oxygen resistant coatings for low earth orbit space structures."
                            }
                        ],
                        "volume": "30",
                        "issue": "2",
                        "first_page": "308",
                        "last_page": "320",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "11) S. Packirisamy, D. Schwam, and M.H. Litt, J. Mat. Sci., 30 (1995) p.308."
                    },
                    {
                        "sequence": "12",
                        "original_text": "12) L.L. Fewell, J. Appl. Polym. Sci., 41 (1990) p.391."
                    },
                    {
                        "sequence": "13",
                        "original_text": "13) M. Raja Reddy, N. Srinivasamurthy, and B.L. Agrawal, ESA J., 16 (1992) p.193."
                    },
                    {
                        "sequence": "14",
                        "doi": "10.1088/0954-0083/11/1/013",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Protection of polymetric materials from atomic oxygen."
                            }
                        ],
                        "volume": "11",
                        "issue": "1",
                        "first_page": "157",
                        "last_page": "165",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "content_language": "en",
                        "original_text": "14) RC. Tennyson, High Perform. Polym., 11 (1999) p.157."
                    },
                    {
                        "sequence": "15",
                        "original_text": "15) Y. Huang, J. Liu, I. Ball, and T.K. Minton, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1788."
                    },
                    {
                        "sequence": "16",
                        "original_text": "16) B.A. Banks, and R. Demko, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.820."
                    },
                    {
                        "sequence": "17",
                        "original_text": "17) M.L. lliingsworth, J.A. Betancourt, L. He, Y. Chen, J.A. Terschak, B.A. Banks, S.K. Rutledge, and M. Cales, NASAITM-2001-211099 (2001)."
                    },
                    {
                        "sequence": "18",
                        "original_text": "18) R.L. Kiefer, RA. Anderson, M.H.Y. Kim, and S.A. Thibeault, Nuclear Instruments and Methods in Physics Research B, (2003) in press."
                    },
                    {
                        "sequence": "19",
                        "volume": "2002",
                        "issue": "14",
                        "first_page": "2002",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "original_text": "19) C. Park, Z. Ounaies, K.A. Watson, K. Pawlowski, S.E. Lowther, J.W. Connell, E.J. Siochi, J.S. Harrison, and T.L. St Clair, NASA/CR-2002-211940 (2002)."
                    },
                    {
                        "sequence": "20",
                        "original_text": "20) Y. Huang, J. Liu, I. Ball, and T.K. Minton, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1788."
                    },
                    {
                        "sequence": "21",
                        "doi": "10.1177/0954008303015002003",
                        "volume": "15",
                        "first_page": "181",
                        "publication_date": {
                            "publication_year": "2003"
                        },
                        "original_text": "21) C.M. Thompson, J.G. Smith, Jr., andJ.W. Connell, High Perform. Polym., 15 (2003) p.181."
                    },
                    {
                        "sequence": "22",
                        "original_text": "22) K.A. Watson, and J.W. Connell, Proc. Fluoropolymer 2000, Savanah, Georgia, October 15.18, 2000."
                    },
                    {
                        "sequence": "23",
                        "original_text": "23) J.G. Smith Jr., J.W. Connell, and P.M. Hergenrother, Polymer, 35 (1994) p.2834."
                    },
                    {
                        "sequence": "24",
                        "original_text": "24) K.K. de Groh, B.A. Banks, and R. Demko, Proc. 47th Int. SAMPE Symp. Exh., B.M. Rasmussen, L.A. Pilato, and H.S. Kliger, Eds., Long Beach, Califonia, May 12-16, 2002, p.1279."
                    },
                    {
                        "sequence": "25",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "25) K.A. Watson, F.L. Palmteri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "25",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "46) K.A. Watson, F.L. Palmieri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "26",
                        "doi": "10.1246/cl.1997.333",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "In situ Formed Three Layer Film by Isomerization of Fluorinated Polyisoimide in Polyethersulfone as a Matrix Polymer."
                            }
                        ],
                        "issue": "4",
                        "first_page": "333",
                        "last_page": "334",
                        "publication_date": {
                            "publication_year": "1997"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Mochizuki",
                                        "first_name": "Amane"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "2",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Yamada",
                                        "first_name": "Kazuo"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "3",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Ueda",
                                        "first_name": "Mitsuru"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "4",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Yokota",
                                        "first_name": "Rikio"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Institute of Space and Astronautical Science",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "26) A. Mochizuki, K. Yamada, M. Ueda, and R. Yokota, Chem. Letts. (1997) 333."
                    },
                    {
                        "sequence": "27",
                        "original_text": "27) K.K. de Groh, J.R. Gaier, R.L. Hall, M.J. Norris, M.P. Espe, and D.R. Cato, Effects of Heating on Teflon FEP Thermal Control Material from the Hubble Space Telescope, NASAITM-2000-209085 (2000)."
                    },
                    {
                        "sequence": "28",
                        "original_text": "28) K.K. de Groh, and D.C. Smith, Investigation of Teflon FEP Embrittlement on Spacecraft in Low Earth Orbit, NASAITM-113153 (1997)."
                    },
                    {
                        "sequence": "29",
                        "doi": "10.1126/science.2563171",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Effects of buried ionizable amino acids on the reduction potential of recombinant myoglobin."
                            }
                        ],
                        "volume": "243",
                        "issue": "4887",
                        "first_page": "69",
                        "last_page": "72",
                        "publication_date": {
                            "publication_year": "1989"
                        },
                        "content_language": "en",
                        "original_text": "29) McGrath, D. Chen, and J.E. McGrath, Polyimides: Materials, Chemistry and Characterization, C. Feger, M.M. Khojasteh, and J.E. McGrath, eds., Elsevier Science Publishers B.V., Amsterdam (1989) p.69."
                    },
                    {
                        "sequence": "30",
                        "original_text": "30) J. Visentine, W. Kinard, D. Brinker, B.A Banks, and K. Albyn, J. Spacecraft and Rockets, 39 (2002) p.187."
                    },
                    {
                        "sequence": "31",
                        "original_text": "31) B.A. Banks, K.K. de Groh, S.K. Rutledge, and C.A. Haytas, Consequences of Atomic Oxygen Interaction with Silicone and Silicone Contamination on Surfaces in Low Earth Orbit, NASAITM-1999-209179 (1999)."
                    },
                    {
                        "sequence": "32",
                        "original_text": "32) N. Furukawa, Y. Yamada, M. Furukawa, M. Yuasa, and Y. Kimura, J. Polym. Sci., Part A: Polym. Chem., 35 (1997) p.2239."
                    },
                    {
                        "sequence": "33",
                        "original_text": "33) NASDA Rep. No. AU9-R02-K108 (2003)."
                    },
                    {
                        "sequence": "34",
                        "original_text": "34) G.B. Hoflund, RI. Gonzalez, and S.H. Phillips, J. Adhesion Sci. Technol., 15 (2001) p.1199."
                    },
                    {
                        "sequence": "35",
                        "original_text": "35) S.H. Phillips, RI. Gonzalez, R.L. Blanski, B.D. Viers, and G.B. Hoflund, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1488."
                    },
                    {
                        "sequence": "36",
                        "original_text": "36) RI. Ganzalez, Ph.D. Disseration, University ofFlorida, Gainesville, Florida (2002)."
                    },
                    {
                        "sequence": "37",
                        "original_text": "37) RI. Gonzalex, G.B. Holfund, S.A. Svejda, S.H. Phillips, and B.V. Viers, submitted to Macromolecules."
                    },
                    {
                        "sequence": "38",
                        "original_text": "38) J.E. McGrath et.al. US Pat. 5,420,225 (1995), US Pat. 5,407,528 (1995), US Pat. 5,387,629 (1995), and US. Pat. 5,134,207 (1992)."
                    },
                    {
                        "sequence": "39",
                        "doi": "10.1016/0032-3861(95)90668-R",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Oxygen plasma-resistant phenylphosphine oxide-containing polyimides and poly(arylene ether heterocycle)s. 1."
                            }
                        ],
                        "volume": "36",
                        "issue": "1",
                        "first_page": "5",
                        "last_page": "11",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "39) J.W. Connell, J.G. Smith Jr., and P.M. Hergenrother, Polymer, 36 (1995) p.5."
                    },
                    {
                        "sequence": "40",
                        "doi": "10.1016/0032-3861(95)90669-S",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Oxgen plasma-resistant phenylphosphine oxide-containing polyimides and poly(arylene ether heterocycle)s. 2."
                            }
                        ],
                        "volume": "36",
                        "issue": "1",
                        "first_page": "13",
                        "last_page": "19",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "40) J.W. Connell, J.G. Smith Jr., and P.M. Hergenrother, Polymer, 36 (1995) p.13."
                    },
                    {
                        "sequence": "41",
                        "original_text": "41) P. Schuler, R. Haghighat, and H. Mojazza, High Perform. Polym., 11 (1999) p.113."
                    },
                    {
                        "sequence": "42",
                        "original_text": "42) J.W. Connell, The Effect of Low Earth Orbit Atomic Oxygen Exposure on Phenylphosphine Oxide-Containing Polymers, 44th International SAMPE Symposium and Exhibition, Long Beach, California, May 23-27, 1999."
                    },
                    {
                        "sequence": "43",
                        "doi": "10.1088/0954-0083/11/1/008",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Electrically conductive space-durable polymeric films for spacecraft thermal and charge control."
                            }
                        ],
                        "volume": "11",
                        "issue": "1",
                        "first_page": "101",
                        "last_page": "111",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "content_language": "en",
                        "original_text": "43) J. Lennhoff, G. Harris, J. Vaughn, D. Edwards, and J. Zwiener, High Perform. Polym., 11 (1999) p.101."
                    },
                    {
                        "sequence": "44",
                        "original_text": "44) T.C. Chang,, K.H. Wu, and Y.S. Chiu, Polym. Degrad Stability, 63 (1999) p.103."
                    },
                    {
                        "sequence": "45",
                        "doi": "10.1016/S0032-3861(02)00362-2",
                        "original_text": "45) P.M. Hergenrother, K.A. Watson, J.G. Smith Jr., J.W. Connell, and R. Yokota, Polymer, 43 (2002) p.5077."
                    },
                    {
                        "sequence": "46",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "25) K.A. Watson, F.L. Palmteri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "46",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "46) K.A. Watson, F.L. Palmieri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "47",
                        "original_text": "47) K.A. Watson, and J.W. Connell, Space Environmentally Stable Polyimides and Copolyimides, 45th International SAMPE Symposium and Exhibit, Long Beach, California, May 21-25, 2000."
                    },
                    {
                        "sequence": "48",
                        "original_text": "48) C. M. Thompson, J. G. Smith, Jr., K. A. Watson and J. W. Connell, Polyimides Containing Pendent Phosphine Oxide Groups for Space Applications, 34th International SAMPE Technical Conference, Baltimore, Maryland, November 4-7, 2002."
                    },
                    {
                        "sequence": "49",
                        "original_text": "49) H. Zhuang, Ph.D. Dissertation, Virginia Polytechnic Institute and State University, Blacksburg, Virginir (1998)."
                    },
                    {
                        "sequence": "50",
                        "original_text": "50) S. Wang, Ph.D. Dissertation, Virginia Polytechnic Institute and State University, Blacksburg, Virginir (2000)."
                    },
                    {
                        "sequence": "51",
                        "original_text": "51) S. Y. Lu, and I. Hamerton, Prag. Polym. Sci., 27 (2002) p.1661."
                    }
                ]
            }
        }
        jalc_processor = JalcProcessing()
        pages = jalc_processor.get_jalc_pages(item_dict["data"])
        self.assertEqual(pages, '1_34-1_34')

    def test_get_pages_non_roman_letters(self):
        item_dict = {
            "status": "OK",
            "apiType": "doi",
            "apiVersion": "1.0.0",
            "message": {
                "total": 1,
                "rows": 1,
                "totalPages": 1,
                "page": 1
            },
            "data": {
                "siteId": "SI/JST.JSTAGE",
                "content_type": "JA",
                "doi": "10.11230/jsts.18.1_34",
                "url": "https://doi.org/10.11230/jsts.18.1_34",
                "ra": "JaLC",
                "prefix": "10.11230",
                "site_name": "J-STAGE",
                "publisher_list": [
                    {
                        "publisher_name": "Japanese Rocket Society",
                        "lang": "en"
                    },
                    {
                        "publisher_name": "特定非営利活動法人　日本ロケット協会",
                        "lang": "ja"
                    }
                ],
                "title_list": [
                    {
                        "lang": "en",
                        "title": "Atomic Oxygen Protections in Polymeric Systems: A Review"
                    }
                ],
                "creator_list": [
                    {
                        "sequence": "1",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "RIMDUSIT",
                                "first_name": "Sarawut"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Chulalongkorn University",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    },
                    {
                        "sequence": "2",
                        "type": "person",
                        "names": [
                            {
                                "lang": "en",
                                "last_name": "YOKOTA",
                                "first_name": "Rikio"
                            }
                        ],
                        "affiliation_list": [
                            {
                                "affiliation_name": "Chulalongkorn University",
                                "sequence": "1",
                                "lang": "en"
                            }
                        ]
                    }
                ],
                "publication_date": {
                    "publication_year": "2002"
                },
                "relation_list": [
                    {
                        "content": "https://www.jstage.jst.go.jp/article/jsts/18/1/18_1_34/_pdf",
                        "type": "URL",
                        "relation": "fullTextPdf"
                    }
                ],
                "content_language": "en",
                "updated_date": "2014-12-09",
                "article_type": "pub",
                "journal_id_list": [
                    {
                        "journal_id": "0911-551X",
                        "type": "ISSN",
                        "issn_type": "print"
                    },
                    {
                        "journal_id": "2186-4772",
                        "type": "ISSN",
                        "issn_type": "online"
                    },
                    {
                        "journal_id": "jsts",
                        "type": "JID"
                    }
                ],
                "journal_title_name_list": [
                    {
                        "journal_title_name": "The Journal of Space Technology and Science",
                        "type": "full",
                        "lang": "en"
                    },
                    {
                        "journal_title_name": "JSTS",
                        "type": "abbreviation",
                        "lang": "en"
                    }
                ],
                "journal_classification": "01",
                "journal_txt_lang": "en",
                "recorded_year": "1985-2013",
                "volume": "18",
                "issue": "1",
                "first_page": "kio",
                "last_page": "jpj",
                "date": "2013-08-18",
                "keyword_list": [
                    {
                        "keyword": "Atomic Oxygen",
                        "sequence": "1",
                        "lang": "en"
                    },
                    {
                        "keyword": "Immiscible Blends",
                        "sequence": "2",
                        "lang": "en"
                    },
                    {
                        "keyword": "Phosphorus-containing Polyimide",
                        "sequence": "3",
                        "lang": "en"
                    },
                    {
                        "keyword": "Silicon-containing Polyimide",
                        "sequence": "4",
                        "lang": "en"
                    },
                    {
                        "keyword": "MLI",
                        "sequence": "5",
                        "lang": "en"
                    }
                ],
                "citation_list": [
                    {
                        "sequence": "1",
                        "original_text": "1) R. Yokota, Aerospace Materials, Chapter 5, B. Cantor, H. Assender, and P. Grant, eds., Institute ofPhysics Publishing, Bristol (2001) p. 47."
                    },
                    {
                        "sequence": "2",
                        "original_text": "2) C.K. Krishnaprakas, K. Badari Narayana, and Pradip Dutta, Cryogenics, 40 (2000) p.431."
                    },
                    {
                        "sequence": "3",
                        "doi": "10.2494/photopolymer.12.209",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Recent Trends and Space Applications of Poylmides."
                            }
                        ],
                        "volume": "12",
                        "issue": "2",
                        "first_page": "209",
                        "last_page": "216",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "YOKOTA",
                                        "first_name": "RIKIO"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "3) R. Yokota, J. Photopolym. Sci. Tech., 12 (1999) p.209."
                    },
                    {
                        "sequence": "4",
                        "original_text": "4) E.M. Silverman, Environmental Effects on Spacecraft: LEO Materials Selection Guide, NASA Contractor Report 4661Part1 and Part 2 (1995)."
                    },
                    {
                        "sequence": "5",
                        "first_page": "1999",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "original_text": "5) D. Dooling and M.M. Finckenor, Material Selection Guidelines to Limit Atomic Oxygen Effects on Spacecraft Surfaces, NASA/TP-1999-209260 (1999)."
                    },
                    {
                        "sequence": "6",
                        "original_text": "6) E. Grossman, and I. Gouzman, Nuclear Instruments cind Methods in Physics Research B (2003) in press."
                    },
                    {
                        "sequence": "7",
                        "original_text": "7) R. Yokota, A. Ohnishi, Y. Hashimoto, K. Toki, S. Kuroda, K. Akahori, and H. Nagano, Proc. 7th Materials in Space Environment, Toulouse, France, 16-20 Jurie 1997, p.293."
                    },
                    {
                        "sequence": "8",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Atomic oxygen effects on polymer-based materials."
                            }
                        ],
                        "volume": "69",
                        "issue": "8/9",
                        "first_page": "1190",
                        "last_page": "1208",
                        "publication_date": {
                            "publication_year": "1991"
                        },
                        "content_language": "en",
                        "original_text": "8) RC. Tennyson, Can. J. Phys., 69 (1991) p.1190."
                    },
                    {
                        "sequence": "9",
                        "original_text": "9) R.H. Hansen, J.V. Pascale, T. De Benedictis, and P.M. Rentzepis, J. Polym. Sci.: Part A, 3 (1965) p.2205."
                    },
                    {
                        "sequence": "10",
                        "doi": "10.1007/BF00354389",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Effect of low earth orbit atomic oxygen on spacecraft materials."
                            }
                        ],
                        "volume": "30",
                        "issue": "2",
                        "first_page": "281",
                        "last_page": "307",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "10) M. Raja Reddy, J. Mat. Sci., 30 (1995) p.281."
                    },
                    {
                        "sequence": "11",
                        "doi": "10.1007/BF00354390",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Atomic oxygen resistant coatings for low earth orbit space structures."
                            }
                        ],
                        "volume": "30",
                        "issue": "2",
                        "first_page": "308",
                        "last_page": "320",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "11) S. Packirisamy, D. Schwam, and M.H. Litt, J. Mat. Sci., 30 (1995) p.308."
                    },
                    {
                        "sequence": "12",
                        "original_text": "12) L.L. Fewell, J. Appl. Polym. Sci., 41 (1990) p.391."
                    },
                    {
                        "sequence": "13",
                        "original_text": "13) M. Raja Reddy, N. Srinivasamurthy, and B.L. Agrawal, ESA J., 16 (1992) p.193."
                    },
                    {
                        "sequence": "14",
                        "doi": "10.1088/0954-0083/11/1/013",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Protection of polymetric materials from atomic oxygen."
                            }
                        ],
                        "volume": "11",
                        "issue": "1",
                        "first_page": "157",
                        "last_page": "165",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "content_language": "en",
                        "original_text": "14) RC. Tennyson, High Perform. Polym., 11 (1999) p.157."
                    },
                    {
                        "sequence": "15",
                        "original_text": "15) Y. Huang, J. Liu, I. Ball, and T.K. Minton, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1788."
                    },
                    {
                        "sequence": "16",
                        "original_text": "16) B.A. Banks, and R. Demko, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.820."
                    },
                    {
                        "sequence": "17",
                        "original_text": "17) M.L. lliingsworth, J.A. Betancourt, L. He, Y. Chen, J.A. Terschak, B.A. Banks, S.K. Rutledge, and M. Cales, NASAITM-2001-211099 (2001)."
                    },
                    {
                        "sequence": "18",
                        "original_text": "18) R.L. Kiefer, RA. Anderson, M.H.Y. Kim, and S.A. Thibeault, Nuclear Instruments and Methods in Physics Research B, (2003) in press."
                    },
                    {
                        "sequence": "19",
                        "volume": "2002",
                        "issue": "14",
                        "first_page": "2002",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "original_text": "19) C. Park, Z. Ounaies, K.A. Watson, K. Pawlowski, S.E. Lowther, J.W. Connell, E.J. Siochi, J.S. Harrison, and T.L. St Clair, NASA/CR-2002-211940 (2002)."
                    },
                    {
                        "sequence": "20",
                        "original_text": "20) Y. Huang, J. Liu, I. Ball, and T.K. Minton, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1788."
                    },
                    {
                        "sequence": "21",
                        "doi": "10.1177/0954008303015002003",
                        "volume": "15",
                        "first_page": "181",
                        "publication_date": {
                            "publication_year": "2003"
                        },
                        "original_text": "21) C.M. Thompson, J.G. Smith, Jr., andJ.W. Connell, High Perform. Polym., 15 (2003) p.181."
                    },
                    {
                        "sequence": "22",
                        "original_text": "22) K.A. Watson, and J.W. Connell, Proc. Fluoropolymer 2000, Savanah, Georgia, October 15.18, 2000."
                    },
                    {
                        "sequence": "23",
                        "original_text": "23) J.G. Smith Jr., J.W. Connell, and P.M. Hergenrother, Polymer, 35 (1994) p.2834."
                    },
                    {
                        "sequence": "24",
                        "original_text": "24) K.K. de Groh, B.A. Banks, and R. Demko, Proc. 47th Int. SAMPE Symp. Exh., B.M. Rasmussen, L.A. Pilato, and H.S. Kliger, Eds., Long Beach, Califonia, May 12-16, 2002, p.1279."
                    },
                    {
                        "sequence": "25",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "25) K.A. Watson, F.L. Palmteri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "25",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "46) K.A. Watson, F.L. Palmieri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "26",
                        "doi": "10.1246/cl.1997.333",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "In situ Formed Three Layer Film by Isomerization of Fluorinated Polyisoimide in Polyethersulfone as a Matrix Polymer."
                            }
                        ],
                        "issue": "4",
                        "first_page": "333",
                        "last_page": "334",
                        "publication_date": {
                            "publication_year": "1997"
                        },
                        "creator_list": [
                            {
                                "sequence": "1",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Mochizuki",
                                        "first_name": "Amane"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "2",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Yamada",
                                        "first_name": "Kazuo"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "3",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Ueda",
                                        "first_name": "Mitsuru"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Department of Materials Science and Engineering, Faculty of Engineering, Yamagata University",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            },
                            {
                                "sequence": "4",
                                "type": "person",
                                "names": [
                                    {
                                        "lang": "en",
                                        "last_name": "Yokota",
                                        "first_name": "Rikio"
                                    }
                                ],
                                "affiliation_list": [
                                    {
                                        "affiliation_name": "Institute of Space and Astronautical Science",
                                        "sequence": "1",
                                        "lang": "en"
                                    }
                                ]
                            }
                        ],
                        "content_language": "en",
                        "original_text": "26) A. Mochizuki, K. Yamada, M. Ueda, and R. Yokota, Chem. Letts. (1997) 333."
                    },
                    {
                        "sequence": "27",
                        "original_text": "27) K.K. de Groh, J.R. Gaier, R.L. Hall, M.J. Norris, M.P. Espe, and D.R. Cato, Effects of Heating on Teflon FEP Thermal Control Material from the Hubble Space Telescope, NASAITM-2000-209085 (2000)."
                    },
                    {
                        "sequence": "28",
                        "original_text": "28) K.K. de Groh, and D.C. Smith, Investigation of Teflon FEP Embrittlement on Spacecraft in Low Earth Orbit, NASAITM-113153 (1997)."
                    },
                    {
                        "sequence": "29",
                        "doi": "10.1126/science.2563171",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Effects of buried ionizable amino acids on the reduction potential of recombinant myoglobin."
                            }
                        ],
                        "volume": "243",
                        "issue": "4887",
                        "first_page": "69",
                        "last_page": "72",
                        "publication_date": {
                            "publication_year": "1989"
                        },
                        "content_language": "en",
                        "original_text": "29) McGrath, D. Chen, and J.E. McGrath, Polyimides: Materials, Chemistry and Characterization, C. Feger, M.M. Khojasteh, and J.E. McGrath, eds., Elsevier Science Publishers B.V., Amsterdam (1989) p.69."
                    },
                    {
                        "sequence": "30",
                        "original_text": "30) J. Visentine, W. Kinard, D. Brinker, B.A Banks, and K. Albyn, J. Spacecraft and Rockets, 39 (2002) p.187."
                    },
                    {
                        "sequence": "31",
                        "original_text": "31) B.A. Banks, K.K. de Groh, S.K. Rutledge, and C.A. Haytas, Consequences of Atomic Oxygen Interaction with Silicone and Silicone Contamination on Surfaces in Low Earth Orbit, NASAITM-1999-209179 (1999)."
                    },
                    {
                        "sequence": "32",
                        "original_text": "32) N. Furukawa, Y. Yamada, M. Furukawa, M. Yuasa, and Y. Kimura, J. Polym. Sci., Part A: Polym. Chem., 35 (1997) p.2239."
                    },
                    {
                        "sequence": "33",
                        "original_text": "33) NASDA Rep. No. AU9-R02-K108 (2003)."
                    },
                    {
                        "sequence": "34",
                        "original_text": "34) G.B. Hoflund, RI. Gonzalez, and S.H. Phillips, J. Adhesion Sci. Technol., 15 (2001) p.1199."
                    },
                    {
                        "sequence": "35",
                        "original_text": "35) S.H. Phillips, RI. Gonzalez, R.L. Blanski, B.D. Viers, and G.B. Hoflund, Proc. SAMPE 2002, May 12-16, 2002, Long Beach, California, p.1488."
                    },
                    {
                        "sequence": "36",
                        "original_text": "36) RI. Ganzalez, Ph.D. Disseration, University ofFlorida, Gainesville, Florida (2002)."
                    },
                    {
                        "sequence": "37",
                        "original_text": "37) RI. Gonzalex, G.B. Holfund, S.A. Svejda, S.H. Phillips, and B.V. Viers, submitted to Macromolecules."
                    },
                    {
                        "sequence": "38",
                        "original_text": "38) J.E. McGrath et.al. US Pat. 5,420,225 (1995), US Pat. 5,407,528 (1995), US Pat. 5,387,629 (1995), and US. Pat. 5,134,207 (1992)."
                    },
                    {
                        "sequence": "39",
                        "doi": "10.1016/0032-3861(95)90668-R",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Oxygen plasma-resistant phenylphosphine oxide-containing polyimides and poly(arylene ether heterocycle)s. 1."
                            }
                        ],
                        "volume": "36",
                        "issue": "1",
                        "first_page": "5",
                        "last_page": "11",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "39) J.W. Connell, J.G. Smith Jr., and P.M. Hergenrother, Polymer, 36 (1995) p.5."
                    },
                    {
                        "sequence": "40",
                        "doi": "10.1016/0032-3861(95)90669-S",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Oxgen plasma-resistant phenylphosphine oxide-containing polyimides and poly(arylene ether heterocycle)s. 2."
                            }
                        ],
                        "volume": "36",
                        "issue": "1",
                        "first_page": "13",
                        "last_page": "19",
                        "publication_date": {
                            "publication_year": "1995"
                        },
                        "content_language": "en",
                        "original_text": "40) J.W. Connell, J.G. Smith Jr., and P.M. Hergenrother, Polymer, 36 (1995) p.13."
                    },
                    {
                        "sequence": "41",
                        "original_text": "41) P. Schuler, R. Haghighat, and H. Mojazza, High Perform. Polym., 11 (1999) p.113."
                    },
                    {
                        "sequence": "42",
                        "original_text": "42) J.W. Connell, The Effect of Low Earth Orbit Atomic Oxygen Exposure on Phenylphosphine Oxide-Containing Polymers, 44th International SAMPE Symposium and Exhibition, Long Beach, California, May 23-27, 1999."
                    },
                    {
                        "sequence": "43",
                        "doi": "10.1088/0954-0083/11/1/008",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Electrically conductive space-durable polymeric films for spacecraft thermal and charge control."
                            }
                        ],
                        "volume": "11",
                        "issue": "1",
                        "first_page": "101",
                        "last_page": "111",
                        "publication_date": {
                            "publication_year": "1999"
                        },
                        "content_language": "en",
                        "original_text": "43) J. Lennhoff, G. Harris, J. Vaughn, D. Edwards, and J. Zwiener, High Perform. Polym., 11 (1999) p.101."
                    },
                    {
                        "sequence": "44",
                        "original_text": "44) T.C. Chang,, K.H. Wu, and Y.S. Chiu, Polym. Degrad Stability, 63 (1999) p.103."
                    },
                    {
                        "sequence": "45",
                        "doi": "10.1016/S0032-3861(02)00362-2",
                        "original_text": "45) P.M. Hergenrother, K.A. Watson, J.G. Smith Jr., J.W. Connell, and R. Yokota, Polymer, 43 (2002) p.5077."
                    },
                    {
                        "sequence": "46",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "25) K.A. Watson, F.L. Palmteri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "46",
                        "doi": "10.1021/ma0201779",
                        "title_list": [
                            {
                                "lang": "en",
                                "title": "Space Environmentally Stable Polyimides and Copolyimides Derived from [2,4-Bis(3-aminophenoxy)phenyl]diphenylphosphine Oxide."
                            }
                        ],
                        "volume": "35",
                        "issue": "13",
                        "first_page": "4968",
                        "last_page": "4974",
                        "publication_date": {
                            "publication_year": "2002"
                        },
                        "content_language": "en",
                        "original_text": "46) K.A. Watson, F.L. Palmieri, and J.W. Connell, Macromolecules, 35 (2002) p.4968."
                    },
                    {
                        "sequence": "47",
                        "original_text": "47) K.A. Watson, and J.W. Connell, Space Environmentally Stable Polyimides and Copolyimides, 45th International SAMPE Symposium and Exhibit, Long Beach, California, May 21-25, 2000."
                    },
                    {
                        "sequence": "48",
                        "original_text": "48) C. M. Thompson, J. G. Smith, Jr., K. A. Watson and J. W. Connell, Polyimides Containing Pendent Phosphine Oxide Groups for Space Applications, 34th International SAMPE Technical Conference, Baltimore, Maryland, November 4-7, 2002."
                    },
                    {
                        "sequence": "49",
                        "original_text": "49) H. Zhuang, Ph.D. Dissertation, Virginia Polytechnic Institute and State University, Blacksburg, Virginir (1998)."
                    },
                    {
                        "sequence": "50",
                        "original_text": "50) S. Wang, Ph.D. Dissertation, Virginia Polytechnic Institute and State University, Blacksburg, Virginir (2000)."
                    },
                    {
                        "sequence": "51",
                        "original_text": "51) S. Y. Lu, and I. Hamerton, Prag. Polym. Sci., 27 (2002) p.1661."
                    }
                ]
            }
        }
        jalc_processor = JalcProcessing()
        pages = jalc_processor.get_jalc_pages(item_dict["data"])
        self.assertEqual(pages, '')
        
    def test_get_ja_with_japanese(self):
        list_input = [{"publisher_name": "Japanese Rocket Society", "lang": "en"}, {"publisher_name": "特定非営利活動法人　日本ロケット協会", "lang": "ja"}]
        jalc_processor = JalcProcessing()
        ja = jalc_processor.get_ja(list_input)
        expected_out = [{"publisher_name": "特定非営利活動法人　日本ロケット協会", "lang": "ja"}]
        self.assertEqual(ja, expected_out)

    def test_get_ja_without_japanese(self):
        list_input = [{"lang": "en", "title": "Ozone Measurement by MT-135 Rocket"}]
        jalc_processor = JalcProcessing()
        en = jalc_processor.get_ja(list_input)
        expected_out = [{"lang": "en", "title": "Ozone Measurement by MT-135 Rocket"}]
        self.assertEqual(en, expected_out)


if __name__ == '__main__':
    unittest.main()