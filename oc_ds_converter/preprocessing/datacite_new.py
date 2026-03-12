import glob
import gzip
import json
import os
import os.path
import tarfile
from os import listdir, makedirs
from os.path import exists, join, split

from oc_ds_converter.preprocessing.base import Preprocessing


class DatacitePreProcessing(Preprocessing):
    """This class aims at pre-processing the DataCite Public Data File.
    The Data File is supplied as a TAR archive, containing JSONLines formatted metadata records and CSVs with some
    supplemental information for easier filtering.
    The folders within the Data File are used to group each record by the month it was last updated,
    following the convention `updated_YYYY-MM`.
    Inside each folder, individual files are compressed with GZIP, to allow for targeted and/or parallel extraction.
    Each `part_XXXX.jsonl` file contains up to 10,000 metadata records, one per line, with each line being a valid self-contained JSON document.

    The class splits the original nldJSON in many JSON files, each one containing the number of entities specified in input by the user.
    Further, the class discards those entities that are not involved in citations"""

    def __init__(self, input_tar, output_dir, interval, state_file=None, filter=None):
        self._req_type = ".json"
        self._input_tar = input_tar
        self._output_dir = output_dir
        self._needed_info = ["relationType", "relatedIdentifierType", "relatedIdentifier"]
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._interval = interval
        if filter:
            self._filter = filter
        else:
            self._filter = ["references", "isreferencedby", "cites", "iscitedby"]
        # Checkpoint file path
        if state_file:
            self._state_file = state_file
        else:
            self._state_file = join(self._output_dir, "processing_state.json")
        super(DatacitePreProcessing, self).__init__()

    def load_checkpoint(self):
        """Loads the last processing state if available."""
        if exists(self._state_file):
            with open(self._state_file, 'r') as f:
                state = json.load(f)
                print(f"Resuming from count {state['count']} and {len(state['processed_files'])} files.")
                return set(state['processed_files']), state['count']
        return set(), 0

    def save_checkpoint(self, processed_files, count):
        """Saves the current list of processed files and global count."""
        with open(self._state_file, 'w') as f:
            json.dump({
                "processed_files": list(processed_files),
                "count": count
            }, f)

    def split_input(self):

        #initialize state
        processed_files_set, global_count = self.load_checkpoint()

        # Files that have been read but their data is still in the 'data' buffer (not yet written)
        pending_files = []

        with tarfile.open(self._input_tar, 'r') as tar:
            data=[]

            # 1. find jsonl gz archives
            jsonl_gz_files = [member for member in tar.getmembers()
                              if member.name.endswith('.jsonl.gz') and member.isfile()]

            # 2. Iterate through each member
            for member in jsonl_gz_files:

                #skip already fully processed files
                if member.name in processed_files_set:
                    continue

                print(f"Processing: {member.name}")
                pending_files.append(member.name)

                # 3. Extract the file object from the tar archive
                f_obj = tar.extractfile(member)

                if f_obj is not None:
                    # 4. Read the extracted file object as a gzip stream
                    with gzip.open(f_obj, mode='rt', encoding='utf-8') as f:
                        for line in f:
                            try:
                                linedict = json.loads(line)

                                #filter for entities without dois
                                if 'id' not in linedict or 'type' not in linedict:
                                    continue
                                if linedict['type'] != "dois":
                                    continue

                                #filter for entities not involved in citations
                                attributes = linedict["attributes"]
                                rel_ids = attributes.get("relatedIdentifiers")

                                if rel_ids:
                                    match_found = False
                                    for ref in rel_ids:
                                        if all(elem in ref for elem in self._needed_info):
                                            relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower()
                                            relationType = str(ref["relationType"]).lower()
                                            relatedIdentifier = str(ref["relatedIdentifier"])
                                            #ignore also entities with self citations
                                            if relatedIdentifierType == "doi" and relationType in self._filter and relatedIdentifier != linedict['id']:
                                                match_found = True
                                                break

                                    if match_found:
                                        data.append(linedict)
                                        global_count += 1
                                        new_data = self.splitted_to_file(global_count, self._interval, self._output_dir,
                                                                         data)
                                        if len(new_data) == 0 and len(data) > 0:
                                            safe_to_commit = pending_files[:-1]
                                            if safe_to_commit:
                                                processed_files_set.update(safe_to_commit)
                                                self.save_checkpoint(processed_files_set, global_count)
                                                pending_files = [member.name]
                                        data = new_data
                            except json.JSONDecodeError:
                                continue
            if data:
                print(f"Flushing final {len(data)} entities (count {global_count}).")
                self.splitted_to_file(global_count, 1, self._output_dir, data)  # interval=1 forces write

            if pending_files:
                processed_files_set.update(pending_files)
                self.save_checkpoint(processed_files_set, global_count)
                print(f"Completed all files. Total entities: {global_count}")



    def splitted_to_file(self, cur_n, target_n, out_dir, data, headers=None):
        makedirs(out_dir, exist_ok=True)
        dict_to_json = dict()
        #check if the interval is reached
        if int(cur_n) != 0 and int(cur_n) % int(target_n) == 0 and data:

            filename = "jSonFile_" + str(cur_n // target_n) + self._req_type
            file_path = os.path.join(out_dir, filename)

            print(f"Writing {filename}")
            with open(file_path, 'w', encoding="utf8") as json_file:
                dict_to_json["data"] = data
                json.dump(dict_to_json, json_file, ensure_ascii=False, indent=2)

            return []
        else:
            return data



def find_publisher_id_in_dump(needed_info, input_tar, filter):
    with (tarfile.open(input_tar, 'r') as tar):
        data = []

        # 1. find jsonl gz archives
        jsonl_gz_files = [member for member in tar.getmembers()
                          if member.name.endswith('.jsonl.gz') and member.isfile()]

        # 2. Iterate through each member
        for member in jsonl_gz_files:

            print(f"Processing: {member.name}")

            # 3. Extract the file object from the tar archive
            f_obj = tar.extractfile(member)

            if f_obj is not None:
                # 4. Read the extracted file object as a gzip stream
                with gzip.open(f_obj, mode='rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            linedict = json.loads(line)
                            # filter for entities without dois
                            if 'id' not in linedict or 'type' not in linedict:
                                continue
                            if linedict['type'] != "dois":
                                continue

                            # filter for entities not involved in citations
                            attributes = linedict["attributes"]
                            rel_ids = attributes.get("relatedIdentifiers")

                            if rel_ids:
                                match_found = False
                                for ref in rel_ids:
                                    if all(elem in ref for elem in needed_info):
                                        relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower()
                                        relationType = str(ref["relationType"]).lower()
                                        if relatedIdentifierType == "doi" and relationType in filter:
                                            match_found = True
                                            break
                                if match_found:
                                    container = attributes.get('container')
                                    publisher_id = attributes.get('publisher').get('publisherIdentifier')
                                    publisher_id_schema = attributes.get('publisher').get('publisherIdentifierScheme')
                                    if publisher_id_schema:
                                        if publisher_id_schema.replace(" ", "").lower() == 'crossreffunderid' or "crossref" in publisher_id_schema.replace(" ", "").lower():
                                            print(linedict, member.name)
                                            return linedict

                        except json.JSONDecodeError:
                            continue

'''
{'id': '10.13133/978-88-98533-15-2', 'type': 'dois', 'attributes': {'container': {'firstPage': '13. Studi umanistici. Serie Antichistica', 'type': 'Series', 'title': 'Collana Studi e Ricerche'}, 'reason': None, 'prefix': '10.13133', 'citationsOverTime': [], 'registered': '2014-03-26T21:56:24Z', 'language': 'it', 'source': None, 'suffix': '978-88-98533-15-2', 'relatedItems': [], 'descriptions': [], 'sizes': ['2,37 MB', '177 pages'], 'versionOfCount': 0, 'relatedIdentifiers': [{'relationType': 'IsCitedBy', 'relatedIdentifier': '10.13133/978-88-98533-15-2', 'relatedIdentifierType': 'DOI'}, {'relationType': 'Cites', 'relatedIdentifier': 'http://digilab-epub.uniroma1.it/index.php/Antichistica/article/view/121/110', 'relatedIdentifierType': 'URL'}], 'created': '2014-03-26T21:56:24Z', 'dates': [{'date': '2014', 'dateType': 'Issued'}], 'published': '2014', 'geoLocations': [], 'partCount': 0, 'publicationYear': 2014, 'partOfCount': 0, 'updated': '2020-08-19T15:38:17Z', 'formats': ['PDF'], 'fundingReferences': [], 'creators': [{'nameType': 'Personal', 'affiliation': [], 'givenName': 'Maria', 'familyName': 'Broggiato', 'name': 'Broggiato, Maria'}], 'schemaVersion': None, 'versionCount': 0, 'metadataVersion': 1, 'citationCount': 0, 'types': {'schemaOrg': 'ScholarlyArticle', 'resourceTypeGeneral': 'Text', 'citeproc': 'article-journal', 'bibtex': 'article', 'ris': 'RPRT'}, 'isActive': True, 'viewsOverTime': [], 'identifiers': [], 'subjects': [], 'titles': [{'title': 'Filologia e interpretazione a Pergamo. La scuola di Cratete'}], 'url': 'http://www.editricesapienza.it/node/7614', 'downloadCount': 0, 'rightsList': [], 'contentUrl': None, 'contributors': [{'affiliation': [], 'name': "Digital Publishing Division Of DigiLab (Centro Interdipartimentale Di Ricerca E Servizi)-La Sapienza Universita' Di Roma"}, {'affiliation': [], 'name': "Digital Publishing Division Of DigiLab (Centro Interdipartimentale Di Ricerca E Servizi)-La Sapienza Universita' Di Roma"}, {'nameType': 'Personal', 'affiliation': [], 'givenName': 'Gianfranco', 'familyName': 'Crupi', 'name': 'Crupi, Gianfranco', 'contributorType': 'DataManager'}], 'referenceCount': 0, 'viewCount': 0, 'downloadsOverTime': [], 'doi': '10.13133/978-88-98533-15-2', 'publisher': {'name': 'Sapienza Università Editrice'}, 'version': None, 'state': 'findable', 'alternateIdentifiers': []}, 'relationships': {'client': {'data': {'id': 'crui.uniroma1', 'type': 'clients'}}, 'provider': {'data': {'id': 'romauno', 'type': 'providers'}}, 'media': {'data': []}, 'references': {'data': []}, 'citations': {'data': []}, 'parts': {'data': []}, 'partOf': {'data': []}, 'versions': {'data': []}, 'versionOf': {'data': []}}} dois/updated_2020-08/part_0100.jsonl.gz
'''
'''
{'id': '10.25620/ci-01_02', 'type': 'dois', 'attributes': {'container': {'volume': 'Cultural Inquiry 1', 'identifier': 'https://doi.org/10.25620/ci-01', 'lastPage': '45', 'firstPage': '13', 'identifierType': 'DOI', 'type': 'Series', 'title': 'Tension/\u200bSpannung'}, 'reason': None, 'prefix': '10.25620', 'citationsOverTime': [], 'registered': '2018-11-14T22:47:16Z', 'language': 'en', 'source': 'fabrica', 'suffix': 'ci-01_02', 'relatedItems': [], 'descriptions': [{'descriptionType': 'Abstract', 'description': 'The article sketches a critical paradigm for interdisciplinary work that is centred on tension as a highly ambiguous and ultimately deeply paradoxical notion. It highlights that a unifying account of what tension is or a systematic classification of its diverse meanings would risk resolving tensions between different approaches and privileging a particular mode of doing so. Successively focussing on aesthetic, socio-political, and physical tensions, the essay articulates tension rather as a broad umbrella term that is stretched by multi-perspectival articulations, unified through its intensive surface tension, and at the same time full of transformative and generative potentials. In particular, it proposes that tensions between different cultural or disciplinary fields can be made productive by inducing tensions within each field so that different fields can be related to each other on the basis of tension rather than some substantial commonality.'}, {'descriptionType': 'SeriesInformation', 'description': 'Tension/\u200bSpannung, Cultural Inquiry 1, 13-45'}, {'descriptionType': 'Other', 'description': 'Christoph F. E. Holzhey, ‘Tension In/Between Aesthetics, Politics, and Physics’, in /Tension/\u200bSpannung/, ed. by Christoph F. E. Holzhey, Cultural Inquiry, 1 (Vienna: Turia + Kant, 2010), pp. 13–45 &gt;'}], 'sizes': [], 'versionOfCount': 0, 'relatedIdentifiers': [{'relationType': 'IsPartOf', 'relatedIdentifier': '10.25620/ci-01', 'relatedIdentifierType': 'DOI'}, {'relationType': 'IsPartOf', 'relatedIdentifier': '978-3-85132-616-1', 'relatedIdentifierType': 'ISBN'}, {'relationType': 'IsPartOf', 'relatedIdentifier': '2627-728X', 'relatedIdentifierType': 'ISSN'}, {'relationType': 'IsPartOf', 'relatedIdentifier': '2627-731X', 'relatedIdentifierType': 'ISSN'}, {'relationType': 'IsPartOf', 'relatedIdentifier': '10.25620/ci-print', 'relatedIdentifierType': 'DOI'}, {'relationType': 'IsPartOf', 'relatedIdentifier': '10.25620/ci-online', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1177/019145378701200202', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1163/156852884x00201', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1353/mln.2003.0062', 'relatedIdentifierType': 'DOI'}], 'created': '2018-11-14T22:47:15Z', 'dates': [{'date': '2010', 'dateType': 'Issued'}], 'published': '2010', 'geoLocations': [], 'partCount': 0, 'publicationYear': 2010, 'partOfCount': 3, 'updated': '2021-01-01T22:17:19Z', 'formats': [], 'fundingReferences': [], 'creators': [{'givenName': 'Christoph F. E.', 'familyName': 'Holzhey', 'name': 'Holzhey, Christoph F. E.', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0002-1312-1878'}]}], 'schemaVersion': 'http://datacite.org/schema/kernel-4', 'versionCount': 0, 'metadataVersion': 5, 'citationCount': 0, 'types': {'schemaOrg': 'ScholarlyArticle', 'resourceTypeGeneral': 'Text', 'citeproc': 'article-journal', 'bibtex': 'article', 'ris': 'RPRT'}, 'isActive': True, 'viewsOverTime': [], 'identifiers': [], 'subjects': [{'subject': 'tension'}, {'subject': 'aesthetics'}, {'subject': 'politics'}, {'subject': 'physics'}, {'subject': 'field theory'}, {'subject': 'intensity'}], 'titles': [{'title': 'Tension In/Between Aesthetics, Politics, and Physics'}], 'url': 'https://oa.ici-berlin.org/repository/doi/10.25620/ci-01_02', 'downloadCount': 0, 'rightsList': [{'rights': '© by the author(s)'}, {'rightsIdentifierScheme': 'SPDX', 'rightsUri': 'https://creativecommons.org/licenses/by-sa/4.0/legalcode', 'schemeUri': 'https://spdx.org/licenses/', 'rights': 'Creative Commons Attribution Share Alike 4.0 International', 'rightsIdentifier': 'cc-by-sa-4.0'}], 'contentUrl': None, 'contributors': [{'givenName': 'Christoph F. E.', 'familyName': 'Holzhey', 'name': 'Holzhey, Christoph F. E.', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0002-1312-1878'}], 'contributorType': 'Editor'}], 'referenceCount': 3, 'viewCount': 0, 'downloadsOverTime': [], 'doi': '10.25620/ci-01_02', 'publisher': {'name': 'Turia + Kant'}, 'version': None, 'state': 'findable', 'alternateIdentifiers': []}, 'relationships': {'client': {'data': {'id': 'subgoe.iciber', 'type': 'clients'}}, 'provider': {'data': {'id': 'icib', 'type': 'providers'}}, 'media': {'data': []}, 'references': {'data': [{'id': '10.1177/019145378701200202', 'type': 'dois'}, {'id': '10.1163/156852884x00201', 'type': 'dois'}, {'id': '10.1353/mln.2003.0062', 'type': 'dois'}]}, 'citations': {'data': []}, 'parts': {'data': []}, 'partOf': {'data': [{'id': '10.25620/ci-01', 'type': 'dois'}, {'id': '10.25620/ci-print', 'type': 'dois'}, {'id': '10.25620/ci-online', 'type': 'dois'}]}, 'versions': {'data': []}, 'versionOf': {'data': []}}} dois/updated_2021-01/part_0003.jsonl.gz
'''
'''
{'id': '10.34780/7510-t906', 'type': 'dois', 'attributes': {'container': {'identifier': '2701-5572', 'firstPage': '2021', 'identifierType': 'ISSN', 'type': 'Series', 'title': 'Journal of Global Archaeology'}, 'reason': None, 'prefix': '10.34780', 'citationsOverTime': [], 'registered': '2021-06-07T10:39:06Z', 'language': 'en', 'source': 'fabricaForm', 'suffix': '7510-t906', 'relatedItems': [], 'descriptions': [{'descriptionType': 'SeriesInformation', 'description': 'Journal of Global Archaeology, 2021'}, {'descriptionType': 'SeriesInformation', 'description': 'Journal of Global Archaeology, 2021'}, {'descriptionType': 'Abstract', 'description': 'The kingdom of Eswatini provides a rich archaeological sequence covering all time periods from the Early Stone Age to the Iron Age. For over 27 years though, no or very little archaeological research was conducted in the country. In the scope of a new project funded by the German Research Foundation (DFG) we aim to re-excavate and re-date Lion Cavern, the potentially oldest ochre mine in the world. In addition, we conduct a largescale geological survey for outcrops of ochre and test their geochemical signatures for comparative studies with archaeological ochre pieces from MSA and LSA assemblages in Eswatini. Here we present a review of the research history of the kingdom and some preliminary results from our ongoing project.', 'lang': 'en'}], 'sizes': ['§ 1–12'], 'versionOfCount': 0, 'relatedIdentifiers': [{'relationType': 'IsPartOf', 'relatedIdentifier': '2701-5572', 'relatedIdentifierType': 'ISSN'}, {'relationType': 'IsPartOf', 'relatedIdentifierType': 'DOI'}, {'relationType': 'HasMetadata', 'relatedIdentifier': 'https://zenon.dainst.org/Record/002035353', 'relatedIdentifierType': 'URL'}, {'relationType': 'References', 'relatedIdentifier': '10.2307/3888317', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1086/204793', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1086/338292', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1111/arcm.12202', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1006/jasc.2000.0638', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.2307/3888015', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.3213/2191-5784-10199', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1016/j.jhevol.2005.06.007', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1017/s0003598x00113298', 'relatedIdentifierType': 'DOI'}], 'created': '2021-05-11T13:11:58Z', 'dates': [{'date': '2021', 'dateType': 'Issued'}], 'published': '2021', 'geoLocations': [], 'partCount': 0, 'publicationYear': 2021, 'partOfCount': 0, 'updated': '2021-07-30T12:39:50Z', 'formats': [], 'fundingReferences': [], 'creators': [{'nameType': 'Personal', 'affiliation': [{'affiliationIdentifier': 'https://ror.org/03a1kwz48', 'name': 'University of Tübingen, Senckenberg Centre for Human Evolution and Palaeoenvironment', 'affiliationIdentifierScheme': 'ROR'}], 'givenName': 'Gregor D.', 'familyName': 'Bader', 'name': 'Bader, Gregor D.', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0003-0621-9209'}]}, {'nameType': 'Personal', 'affiliation': [{'affiliationIdentifier': 'https://ror.org/02vrphe47', 'name': 'Swaziland National Trust Commission', 'affiliationIdentifierScheme': 'ROR'}], 'givenName': 'Bob', 'familyName': 'Forrester', 'name': 'Forrester, Bob'}, {'nameType': 'Personal', 'affiliation': [{'affiliationIdentifier': 'https://ror.org/041qv0h25', 'name': 'Deutsches Archäologisches Institut, Kommission für Archäologie Außereuropäischer Kulturen', 'affiliationIdentifierScheme': 'ROR'}], 'givenName': 'Lisa', 'familyName': 'Ehlers', 'name': 'Ehlers, Lisa'}, {'nameType': 'Personal', 'affiliation': [{'affiliationIdentifier': 'https://ror.org/03zga2b32', 'name': 'University of Bergen, SFF Centre for Early Sapiens Behaviour', 'affiliationIdentifierScheme': 'ROR'}], 'givenName': 'Elizabeth', 'familyName': 'Velliky', 'name': 'Velliky, Elizabeth', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0002-3019-5377'}]}, {'nameType': 'Personal', 'affiliation': [{'affiliationIdentifier': 'https://ror.org/02ymw8z06', 'name': 'University of Missouri, Archaeometry Laboratory', 'affiliationIdentifierScheme': 'ROR'}], 'givenName': 'Brandy Lee', 'familyName': 'MacDonald', 'name': 'MacDonald, Brandy Lee', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0003-2887-4351'}]}, {'nameType': 'Personal', 'affiliation': [{'affiliationIdentifier': 'https://ror.org/041qv0h25', 'name': 'Deutsches Archäologisches Institut, Kommission für Archäologie Außereuropäischer Kulturen', 'affiliationIdentifierScheme': 'ROR'}], 'givenName': 'Jörg', 'familyName': 'Linstädter', 'name': 'Linstädter, Jörg', 'nameIdentifiers': [{'nameIdentifierScheme': 'ORCID', 'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0002-7931-3178'}]}], 'schemaVersion': 'http://datacite.org/schema/kernel-4', 'versionCount': 0, 'metadataVersion': 2, 'citationCount': 0, 'types': {'schemaOrg': 'ScholarlyArticle', 'resourceTypeGeneral': 'Text', 'citeproc': 'article-journal', 'bibtex': 'article', 'ris': 'RPRT', 'resourceType': 'Article'}, 'isActive': True, 'viewsOverTime': [], 'identifiers': [], 'subjects': [{'subject': 'Eswatini'}, {'subject': 'Lion Cavern'}, {'subject': 'Ochre'}, {'subject': 'Provenance tracing'}], 'titles': [{'lang': 'en', 'title': 'The Forgotten Kingdom. New investigations in the prehistory of Eswatini'}], 'url': 'https://publications.dainst.org/journals/index.php/joga/article/view/3559', 'downloadCount': 0, 'rightsList': [], 'contentUrl': None, 'contributors': [], 'referenceCount': 9, 'viewCount': 0, 'downloadsOverTime': [], 'doi': '10.34780/7510-t906', 'publisher': {'name': 'Deutsches Archäologisches Institut'}, 'version': None, 'state': 'findable', 'alternateIdentifiers': []}, 'relationships': {'client': {'data': {'id': 'dai.avnrkz', 'type': 'clients'}}, 'provider': {'data': {'id': 'dai', 'type': 'providers'}}, 'media': {'data': []}, 'references': {'data': [{'id': '10.2307/3888317', 'type': 'dois'}, {'id': '10.1086/204793', 'type': 'dois'}, {'id': '10.1086/338292', 'type': 'dois'}, {'id': '10.1111/arcm.12202', 'type': 'dois'}, {'id': '10.1006/jasc.2000.0638', 'type': 'dois'}, {'id': '10.2307/3888015', 'type': 'dois'}, {'id': '10.3213/2191-5784-10199', 'type': 'dois'}, {'id': '10.1016/j.jhevol.2005.06.007', 'type': 'dois'}, {'id': '10.1017/s0003598x00113298', 'type': 'dois'}]}, 'citations': {'data': []}, 'parts': {'data': []}, 'partOf': {'data': []}, 'versions': {'data': []}, 'versionOf': {'data': []}}} dois/updated_2021-07/part_0020.jsonl.gz
'''
#publisher
'''{'id': '10.60804/bpmz-jb79', 'type': 'dois', 'attributes': {'container': {}, 'reason': None, 'prefix': '10.60804', 'citationsOverTime': [], 'registered': '2024-01-24T14:51:17Z', 'language': None, 'source': 'fabricaForm', 'suffix': 'bpmz-jb79', 'relatedItems': [], 'descriptions': [], 'sizes': [], 'versionOfCount': 0, 'relatedIdentifiers': [{'relationType': 'Cites', 'resourceTypeGeneral': None, 'schemeType': None, 'schemeUri': None, 'relatedIdentifier': 'https://doi.org/10.5281/zenodo.10562429', 'relatedIdentifierType': 'DOI', 'relatedMetadataScheme': None}], 'created': '2024-01-24T14:51:17Z', 'dates': [], 'published': '2024', 'geoLocations': [], 'partCount': 0, 'publicationYear': 2024, 'partOfCount': 0, 'updated': '2024-01-24T14:51:18Z', 'formats': [], 'fundingReferences': [], 'creators': [{'nameType': 'Organizational', 'affiliation': [], 'givenName': None, 'familyName': None, 'name': 'Make Data Count', 'nameIdentifiers': [{'nameIdentifierScheme': 'Other', 'schemeUri': None, 'nameIdentifier': 'https://makedatacount.org/'}]}], 'schemaVersion': 'http://datacite.org/schema/kernel-4', 'versionCount': 0, 'metadataVersion': 0, 'citationCount': 0, 'types': {'schemaOrg': 'CreativeWork', 'resourceTypeGeneral': 'Other', 'citeproc': 'article', 'bibtex': 'misc', 'ris': 'GEN', 'resourceType': 'Blog'}, 'isActive': True, 'viewsOverTime': [], 'identifiers': [], 'subjects': [{'subject': 'Data Citations'}], 'titles': [{'titleType': None, 'lang': 'en', 'title': 'GREI data citation recommendations to support consistent practices to collect, expose and aggregate citations to open data'}], 'url': 'https://makedatacount.org/grei-data-citation-recommendations/', 'downloadCount': 0, 'rightsList': [], 'contentUrl': None, 'contributors': [{'nameType': None, 'affiliation': [{'affiliationIdentifier': None, 'schemeUri': None, 'name': None, 'affiliationIdentifierScheme': None}], 'givenName': None, 'familyName': None, 'name': 'Generalist Repository Ecosystem Initiative (GREI)', 'nameIdentifiers': [{'nameIdentifierScheme': None, 'schemeUri': None, 'nameIdentifier': None}], 'contributorType': 'Other'}], 'referenceCount': 1, 'viewCount': 0, 'downloadsOverTime': [], 'doi': '10.60804/bpmz-jb79', 'publisher': {'publisherIdentifierScheme': 'ROR', 'schemeUri': 'https://ror.org', 'name': 'DataCite', 'publisherIdentifier': 'https://ror.org/04wxnsj81'}, 'version': None, 'state': 'findable', 'alternateIdentifiers': []}, 'relationships': {'client': {'data': {'id': 'datacite.vdpwbc', 'type': 'clients'}}, 'provider': {'data': {'id': 'datacite', 'type': 'providers'}}, 'media': {'data': []}, 'references': {'data': [{'id': '10.5281/zenodo.10562429', 'type': 'dois'}]}, 'citations': {'data': []}, 'parts': {'data': []}, 'partOf': {'data': []}, 'versions': {'data': []}, 'versionOf': {'data': []}}} dois/updated_2024-01/part_0046.jsonl.gz'''

needed_info = ["relationType", "relatedIdentifierType", "relatedIdentifier"]
input_tar = "/media/marta/T7 Touch/NUOVO_DUMP_DATACITE/DataCite_Public_Data_File_2025.tar"
filter = ["references", "isreferencedby", "cites", "iscitedby"]
print(find_publisher_id_in_dump(needed_info, input_tar, filter))

'''dcp = DatacitePreProcessing(input_tar="/media/marta/T7 Touch/NUOVO_DUMP_DATACITE/datastructure/datastructure.tar", output_dir="/media/marta/T7 Touch/NUOVO_DUMP_DATACITE/out_preprocessing_sample", interval=100)
dcp.split_input()'''