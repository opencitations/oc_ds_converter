# -*- coding: utf-8 -*-
import csv
import os
import pathlib
import zipfile
from os.path import isdir, basename, exists
from os import walk, sep, makedirs
import langdetect
import zstandard as zstd
import json
from tqdm import tqdm
import tarfile
import unicodedata
from collections import defaultdict
from langdetect import detect, detect_langs
from oc_ds_converter.jalc.jalc_processing import JalcProcessing
class CountMetadataLang:

    def get_all_files_by_type(self, i_dir_or_compr: str, req_type: str):
        result = []
        targz_fd = None

        if isdir(i_dir_or_compr):

            for cur_dir, cur_subdir, cur_files in walk(i_dir_or_compr):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(os.path.join(cur_dir, cur_file))
        elif i_dir_or_compr.endswith("tar.gz"):
            targz_fd = tarfile.open(i_dir_or_compr, "r:gz", encoding="utf-8")
            for cur_file in targz_fd:
                if cur_file.name.endswith(req_type) and not basename(cur_file.name).startswith("."):
                    result.append(cur_file)
            targz_fd.close()
        elif i_dir_or_compr.endswith(".tar"):
            dest_dir = i_dir_or_compr.replace('.tar', '') + "_decompr_zip_dir"
            targz_fd = tarfile.open(i_dir_or_compr, "r:*", encoding="utf-8")
            targz_fd.extractall(dest_dir)

            for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(cur_dir + sep + cur_file)

            targz_fd.close()


        elif i_dir_or_compr.endswith("zip"):
            with zipfile.ZipFile(i_dir_or_compr, 'r') as zip_ref:
                dest_dir = i_dir_or_compr.replace('.zip', '') + "_decompr_zip_dir"
                if not exists(dest_dir):
                    makedirs(dest_dir)
                zip_ref.extractall(dest_dir)
            for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(cur_dir + sep + cur_file)

        elif i_dir_or_compr.endswith("zst"):
            input_file = pathlib.Path(i_dir_or_compr)
            dest_dir = i_dir_or_compr.split(".")[0] + "_decompr_zst_dir"
            with open(input_file, 'rb') as compressed:
                decomp = zstd.ZstdDecompressor()
                if not exists(dest_dir):
                    makedirs(dest_dir)
                output_path = pathlib.Path(dest_dir) / input_file.stem
                if not exists(output_path):
                    with open(output_path, 'wb') as destination:
                        decomp.copy_stream(compressed, destination)
            for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(cur_dir + sep + cur_file)
        else:
            print("It is not possible to process the input path.", i_dir_or_compr)
        return result, targz_fd

    def count_publisher_lang(self, source_dict, citing=True):
        count_en_ja_citing = 0
        count_en_ja_cited = 0
        count_en_citing = 0
        count_en_cited = 0
        count_ja_citing = 0
        count_ja_cited = 0
        for entity_dict in source_dict:
            data = entity_dict["data"]
            if citing:
                if data.get('publisher_list'):
                    # if the name of the publisher is given both in japanese and in english
                    pub_list = data['publisher_list']
                    pub_lang = [item['lang'] for item in pub_list if 'lang' in item]
                    if 'en' in pub_lang and 'ja' in pub_lang:
                        count_en_ja_citing += 1
                    else:
                        if 'en' in pub_lang:
                            count_en_citing += 1
                        elif 'ja' in pub_lang:
                            count_ja_citing += 1
            else:
                if data.get('citation_list'):
                    cit_list_entities = [x for x in data["citation_list"]]
                    for cit in cit_list_entities:
                        if cit.get('publisher_list'):
                            pub_list_cit = cit['publisher_list']
                            pub_lang = [item['lang'] for item in pub_list_cit if 'lang' in item]
                            if 'en' in pub_lang and 'ja' in pub_lang:
                                count_en_ja_cited += 1
                            else:
                                if 'en' in pub_lang:
                                    count_en_cited += 1
                                elif 'ja' in pub_lang:
                                    count_ja_cited += 1
        if citing:
            return count_en_ja_citing, count_en_citing, count_ja_citing
        else:
            return count_en_ja_cited, count_en_cited, count_ja_cited

    def count_journal_title_lang(self, source_dict, citing=True):
        count_en_ja_citing = 0
        count_en_ja_cited = 0
        count_en_citing = 0
        count_en_cited = 0
        count_ja_citing = 0
        count_ja_cited = 0
        for entity_dict in source_dict:
            data = entity_dict["data"]
            if citing:
                if data.get('journal_title_name_list'):
                    candidate_venues = data['journal_title_name_list']
                    full_venue = [item for item in candidate_venues if 'type' in item if item['type'] == 'full']
                    if full_venue:
                        full_venue_lang = set(item['lang'] for item in full_venue if 'lang' in item)
                        if 'en' in full_venue_lang and 'ja' in full_venue_lang:
                            count_en_ja_citing += 1
                        else:
                            if 'en' in full_venue_lang:
                                count_en_citing += 1
                            elif 'ja' in full_venue_lang:
                                count_ja_citing += 1
                    else:
                        abbr_venue = [item for item in candidate_venues if 'type' in item if item['type'] == 'abbreviation']
                        if abbr_venue:
                            abbr_venue_lang = set(item['lang'] for item in abbr_venue if 'lang' in item)
                            if 'en' in abbr_venue_lang and 'ja' in abbr_venue_lang:
                                count_en_ja_citing += 1
                            else:
                                if 'en' in abbr_venue_lang:
                                    count_en_citing += 1
                                elif 'ja' in abbr_venue_lang:
                                    count_ja_citing += 1
                        else:
                            venues = [item for item in candidate_venues]
                            if venues:
                                lang_venue = [venue['lang'] for venue in venues if 'lang' in venue]
                                if 'en' in lang_venue and 'ja' in lang_venue:
                                    count_en_ja_citing += 1
                                else:
                                    if 'en' in lang_venue:
                                        count_en_citing += 1
                                    elif 'ja' in lang_venue:
                                        count_ja_citing += 1

            else:
                if data.get('citation_list'):
                    cit_list_entities = [x for x in data["citation_list"]]
                    for cit in cit_list_entities:
                        if cit.get('journal_title_name_list'):
                            candidate_venues = cit['journal_title_name_list']
                            full_venue = [item for item in candidate_venues if 'type' in item if item['type'] == 'full']
                            if full_venue:
                                full_venue_lang = set(item['lang'] for item in full_venue if 'lang' in item)
                                if 'en' in full_venue_lang and 'ja' in full_venue_lang:
                                    count_en_ja_cited += 1
                                else:
                                    if 'en' in full_venue_lang:
                                        count_en_cited += 1
                                    elif 'ja' in full_venue_lang:
                                        count_ja_cited += 1
                            else:
                                abbr_venue = [item for item in candidate_venues if 'type' in item if
                                              item['type'] == 'abbreviation']
                                if abbr_venue:
                                    abbr_venue_lang = set(item['lang'] for item in abbr_venue if 'lang' in item)
                                    if 'en' in abbr_venue_lang and 'ja' in abbr_venue_lang:
                                        count_en_ja_cited += 1
                                    else:
                                        if 'en' in abbr_venue_lang:
                                            count_en_cited += 1
                                        elif 'ja' in abbr_venue_lang:
                                            count_ja_cited += 1
                                else:
                                    venues = [item for item in candidate_venues]
                                    if venues:
                                        lang_venue = [venue['lang'] for venue in venues if 'lang' in venue]
                                        if 'en' in lang_venue and 'ja' in lang_venue:
                                            count_en_ja_cited += 1
                                        else:
                                            if 'en' in lang_venue:
                                                count_en_cited += 1
                                            elif 'ja' in lang_venue:
                                                count_ja_cited += 1



        if citing:
            return count_en_ja_citing, count_en_citing, count_ja_citing
        else:
            return count_en_ja_cited, count_en_cited, count_ja_cited



    def count_title_lang(self, source_dict, citing=True):
        count_en_ja_citing = 0
        count_en_ja_cited = 0
        count_en_citing = 0
        count_en_cited = 0
        count_ja_citing = 0
        count_ja_cited = 0
        for entity_dict in source_dict:
            data = entity_dict["data"]
            if citing:
                if data.get('title_list'):
                    title_list = data['title_list']
                    title_lang = [item['lang'] for item in title_list if 'lang' in item]
                    if 'en' in title_lang and 'ja' in title_lang:
                        count_en_ja_citing += 1
                    else:
                        if 'en' in title_lang:
                            count_en_citing += 1
                        elif 'ja' in title_lang:
                            count_ja_citing += 1
            else:
                if data.get('citation_list'):
                    cit_list_entities = [x for x in data["citation_list"]]
                    for cit in cit_list_entities:
                        if cit.get('title_list'):
                            title_list = cit['title_list']
                            title_lang = [item['lang'] for item in title_list if 'lang' in item]
                            if 'en' in title_lang and 'ja' in title_lang:
                                count_en_ja_cited += 1
                            else:
                                if 'en' in title_lang:
                                    count_en_cited += 1
                                elif 'ja' in title_lang:
                                    count_ja_cited += 1
        if citing:
            return count_en_ja_citing, count_en_citing, count_ja_citing
        else:
            return count_en_ja_cited, count_en_cited, count_ja_cited


    def count_creator_names_lang(self, source_dict, citing=True):
        count_en_ja_citing = 0
        count_en_ja_cited = 0
        count_en_citing = 0
        count_en_cited = 0
        count_ja_citing = 0
        count_ja_cited = 0
        for entity_dict in source_dict:
            data = entity_dict["data"]
            if citing:
                if data.get('creator_list'):
                    creator_list = data['creator_list']
                    for creator_dict in creator_list:
                        if creator_dict.get('names'):
                            names_list = creator_dict['names']
                            names_lang = [creator['lang'] for creator in names_list if 'lang' in creator]
                            if 'en' in names_lang and 'ja' in names_lang:
                                count_en_ja_citing += 1
                            else:
                                if 'en' in names_lang:
                                    count_en_citing += 1
                                elif 'ja' in names_lang:
                                    count_ja_citing += 1
            else:
                if data.get('citation_list'):
                    cit_list_entities = [x for x in data["citation_list"]]
                    for cit in cit_list_entities:
                        if cit.get('creator_list'):
                            creator_list = cit['creator_list']
                            for creator_dict in creator_list:
                                if creator_dict.get('names'):
                                    names_list = creator_dict['names']
                                    names_lang = [creator['lang'] for creator in names_list if 'lang' in creator]
                                    if 'en' in names_lang and 'ja' in names_lang:
                                        count_en_ja_cited += 1
                                    else:
                                        if 'en' in names_lang:
                                            count_en_cited += 1
                                        elif 'ja' in names_lang:
                                            count_ja_cited += 1
        if citing:
            return count_en_ja_citing, count_en_citing, count_ja_citing
        else:
            return count_en_ja_cited, count_en_cited, count_ja_cited

    def call_functions_for_all_zips(self, list_of_zips, funct_list: list, csv_file:str, citing=True, cited=True):
        data_to_be_saved = []
        for func in funct_list:
            count_en_ja_citing, count_en_citing, count_ja_citing = 0, 0, 0
            count_en_ja_cited, count_en_cited, count_ja_cited = 0, 0, 0
            for zip_file in tqdm(list_of_zips):
                zip_f = zipfile.ZipFile(zip_file)
                source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
                source_dict = []
                # here I create a list containing all the json in the zip folder as dictionaries
                for json_file in source_data:
                    f = zip_f.open(json_file, 'r')
                    my_dict = json.load(f)
                    source_dict.append(my_dict)
                if citing and cited:
                    result_citing = func(source_dict, True)
                    result_cited = func(source_dict, False)
                    count_en_ja_citing += result_citing[0]
                    count_en_citing += result_citing[1]
                    count_ja_citing += result_citing[2]
                    count_en_ja_cited += result_cited[0]
                    count_en_cited += result_cited[1]
                    count_ja_cited += result_cited[2]
                else:
                    if citing:
                        result_citing = func(source_dict, True)
                        count_en_ja_citing += result_citing[0]
                        count_en_citing += result_citing[1]
                        count_ja_citing += result_citing[2]
                    else:
                        result_cited = func(source_dict, False)
                        count_en_ja_cited += result_cited[0]
                        count_en_cited += result_cited[1]
                        count_ja_cited += result_cited[2]

            if citing and cited:
                dict_result1 = {"en_ja_citing": count_en_ja_citing, "en_citing": count_en_citing, "ja_citing": count_ja_citing, "en_ja_cited": count_en_ja_cited, "en_cited": count_en_cited, "ja_cited": count_ja_cited}
                data_to_be_saved.append(dict_result1)
            else:
                if citing:
                    dict_result2 = {"en_ja_citing": count_en_ja_citing, "en_citing": count_en_citing, "ja_citing": count_ja_citing}
                    data_to_be_saved.append(dict_result2)
                else:
                    dict_result3 = {"en_ja_cited": count_en_ja_cited, "en_cited": count_en_cited, "ja_cited": count_ja_cited}
                    data_to_be_saved.append(dict_result3)
        try:
            with open(csv_file, 'w', newline='') as csvf:
                labels = ["en_ja_citing", "en_citing", "ja_citing", "en_ja_cited", "en_cited", "ja_cited"]
                writer = csv.DictWriter(csvf, fieldnames=labels)
                writer.writeheader()
                for elem in data_to_be_saved:
                    writer.writerow(elem)
        except IOError:
            print("I/O error")



    def serialize_publisher_list(self, publisher_list):
        # Serialize the list of dictionaries into a hashable form
        list_publishers_key = []
        for item in publisher_list:
            list_publishers_key.append(item['publisher_name'])
        return ",".join(list_publishers_key)


    def find_publisher_list_with_more_than_two_dictionaries(self, input_dir, output_json_path):
        list_of_zips = self.find_zip_subfiles(input_dir)
        all_publisher_dict = defaultdict(list)
        for zip_file in tqdm(list_of_zips):
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)

            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if 'publisher_list' in data:
                        if len(data['publisher_list']) > 2:
                            for pub_dict in data['publisher_list']:
                                pub_dict['publisher_name'] = unicodedata.normalize('NFKC', pub_dict['publisher_name'])
                            serialized_publisher_list = self.serialize_publisher_list(data['publisher_list'])
                            # Append DOI to the corresponding serialized publisher_list in the dictionary
                            if serialized_publisher_list in all_publisher_dict:
                                all_publisher_dict[serialized_publisher_list].append(data['doi'])
                            else:
                                all_publisher_dict[serialized_publisher_list] = [data['doi']]
        # Convert defaultdict to a regular dictionary for JSON serialization
        all_publisher_dict = dict(all_publisher_dict)


        # Save the result to a JSON file
        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            json.dump(all_publisher_dict, json_file, ensure_ascii=False, indent=2)

        return all_publisher_dict

    def is_key_in_list(self, key, dictionary_list):
        for dictionary in dictionary_list:
            if key in dictionary:
                dictionary[key] += 1
                return True  # Key found in at least one dictionary
        return False  # Key not found in any dictionary

    def find_all_lang_values_citing(self, input_dir):
        list_of_zips = self.find_zip_subfiles(input_dir)
        tot_lang_dict={"publishers": dict(), "titles": dict(), "journal_titles": dict(), "authors": dict()}
        for zip_file in tqdm(list_of_zips):
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            lang_dict = {"publishers": dict(), "titles": dict(), "journal_titles": dict(), "authors": dict()}
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if 'publisher_list' in data:
                        for pub_dict in data['publisher_list']:
                           if pub_dict.get("lang"):
                                lang = pub_dict["lang"].lower()
                                if not lang in lang_dict["publishers"].keys():
                                    lang_dict["publishers"][lang] = 1
                                else:
                                    lang_dict["publishers"][lang] += 1
                    if 'title_list' in data:
                        title_list = data['title_list']
                        for title in title_list:
                            if title.get("lang"):
                                lang = title["lang"].lower()
                                if not lang in lang_dict["titles"].keys():
                                    lang_dict["titles"][lang] = 1
                                else:
                                    lang_dict["titles"][lang] += 1
                    if 'creator_list' in data:
                        creator_list = data['creator_list']
                        for creator_dict in creator_list:
                            names_list = creator_dict['names']
                            for creator in names_list:
                                if creator.get("lang"):
                                    lang = creator["lang"].lower()
                                    if not lang in lang_dict["authors"].keys():
                                        lang_dict["authors"][lang] = 1
                                    else:
                                        lang_dict["authors"][lang] += 1
                    if data.get('journal_title_name_list'):
                        venues = data['journal_title_name_list']
                        before_venues = [item for item in venues if 'type' in item if item['type'] == 'before']
                        venues = [item for item in venues if item not in before_venues]
                        full_venues = [item for item in venues if 'type' in item if item['type'] == 'full']
                        if full_venues:
                            venues = full_venues
                        for venue in venues:
                            if venue.get("lang"):
                                lang = venue["lang"].lower()
                                if not lang in lang_dict["journal_titles"]:
                                    lang_dict["journal_titles"][lang] = 1
                                else:
                                    lang_dict["journal_titles"][lang] += 1

            # Update tot_lang_dict by adding lang_dict values
            for key, value in lang_dict.items():
                for lang in value:
                    if lang in tot_lang_dict[key]:
                        tot_lang_dict[key][lang] += value[lang]
                    else:
                        tot_lang_dict[key][lang] = value[lang]


        return tot_lang_dict

    def find_all_lang_values_cited(self, input_dir):
        list_of_zips = self.find_zip_subfiles(input_dir)
        tot_lang_dict = {"publishers": dict(), "titles": dict(), "journal_titles": dict(), "authors": dict()}
        for zip_file in tqdm(list_of_zips):
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            lang_dict = {"publishers": dict(), "titles": dict(), "journal_titles": dict(), "authors": dict()}
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if data.get("citation_list"):
                        cit_list_entities = [x for x in data["citation_list"] if x.get('doi')]
                        if cit_list_entities:
                            for citation in cit_list_entities:
                                if 'publisher_list' in citation:
                                    for pub_dict in citation['publisher_list']:
                                       if pub_dict.get("lang"):
                                            lang = pub_dict["lang"].lower()
                                            if not lang in lang_dict["publishers"].keys():
                                                lang_dict["publishers"][lang] = 1
                                            else:
                                                lang_dict["publishers"][lang] += 1
                                if 'title_list' in citation:
                                    title_list = citation['title_list']
                                    for title in title_list:
                                        if title.get("lang"):
                                            lang = title["lang"].lower()
                                            if not lang in lang_dict["titles"].keys():
                                                lang_dict["titles"][lang] = 1
                                            else:
                                                lang_dict["titles"][lang] += 1
                                if 'creator_list' in citation:
                                    creator_list = citation['creator_list']
                                    for creator_dict in creator_list:
                                        names_list = creator_dict['names']
                                        for creator in names_list:
                                            if creator.get("lang"):
                                                lang = creator["lang"].lower()
                                                if not lang in lang_dict["authors"].keys():
                                                    lang_dict["authors"][lang] = 1
                                                else:
                                                    lang_dict["authors"][lang] += 1
                                if citation.get('journal_title_name_list'):
                                    venues = citation['journal_title_name_list']
                                    before_venues = [item for item in venues if 'type' in item if item['type'] == 'before']
                                    venues = [item for item in venues if item not in before_venues]
                                    full_venues = [item for item in venues if 'type' in item if item['type'] == 'full']
                                    if full_venues:
                                        venues = full_venues
                                    for venue in venues:
                                        if venue.get("lang"):
                                            lang = venue["lang"].lower()
                                            if not lang in lang_dict["journal_titles"]:
                                                lang_dict["journal_titles"][lang] = 1
                                            else:
                                                lang_dict["journal_titles"][lang] += 1

            # Update tot_lang_dict by adding lang_dict values
            for key, value in lang_dict.items():
                for lang in value:
                    if lang in tot_lang_dict[key]:
                        tot_lang_dict[key][lang] += value[lang]
                    else:
                        tot_lang_dict[key][lang] = value[lang]


        return tot_lang_dict

    def get_ja(cls, field: list):
        """This method accepts as parameter a list containing dictionaries with the key "lang".
        If a metadata is originally furnished both in the original language and in the english translation,
        the method returns the japanese version, otherwise the english translation is returned."""
        if all('lang' in item for item in field):
            ja = [item for item in field if item['lang'] == 'ja']
            ja = list(filter(lambda x: x['type'] != 'before' if 'type' in x else x, ja))
            if ja:
                return ja
            en = [item for item in field if item['lang'] == 'en']
            en = list(filter(lambda x: x['type'] != 'before' if 'type' in x else x, en))
            if en:
                return en
        return '?'

    def find_publishers_if_not_lang_citing(self, input_dir, output_json_path):
        list_of_zips = self.find_zip_subfiles(input_dir)
        publishers_dict_no_ja_no_en_tot = {}
        for zip_file in tqdm(list_of_zips):
            publishers_dict_no_ja_no_en = {}
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if 'publisher_list' in data:
                        publisher_lang = self.get_ja(data['publisher_list'])
                        if publisher_lang == '?':
                            publisher = data['publisher_list'][0]['publisher_name']
                            if publisher:
                                try:
                                    detected_language = detect(publisher)
                                except langdetect.LangDetectException:
                                    print(publisher)
                                    detected_language = "unknown"
                                if publisher not in publishers_dict_no_ja_no_en:
                                    publishers_dict_no_ja_no_en[publisher] = {"lang": [detected_language], "count":1, "doi":[data["doi"]]}
                                elif publisher in publishers_dict_no_ja_no_en:
                                    if detected_language not in publishers_dict_no_ja_no_en[publisher]["lang"]:
                                        publishers_dict_no_ja_no_en[publisher]["lang"].append(detected_language)
                                    publishers_dict_no_ja_no_en[publisher]["count"] += 1
                                    publishers_dict_no_ja_no_en[publisher]["doi"].append(data["doi"])

            if publishers_dict_no_ja_no_en:
                publishers_dict_no_ja_no_en_tot.update(publishers_dict_no_ja_no_en)

        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            json.dump(publishers_dict_no_ja_no_en_tot, json_file, ensure_ascii=False, indent=2)

        return publishers_dict_no_ja_no_en_tot

    def find_venue_title_if_not_lang_citing(self, input_dir, output_json_path):
        list_of_zips = self.find_zip_subfiles(input_dir)
        venue_dict_no_ja_no_en_tot = {}
        for zip_file in tqdm(list_of_zips):
            venue_dict_no_ja_no_en = {}
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if 'journal_title_name_list' in data:
                        venue_name = self.get_ja(data['journal_title_name_list'])
                        if venue_name == '?':
                            full_venue = [item for item in data['journal_title_name_list'] if 'type' in item if item['type'] == 'full']
                            if full_venue:
                                venue_name = full_venue[0]['journal_title_name']
                            else:
                                venue_name = data['journal_title_name_list'][0]['journal_title_name']
                            try:
                                detected_language = detect(venue_name)
                            except langdetect.LangDetectException:
                                print(venue_name)
                                detected_language = "unknown"
                            if venue_name not in venue_dict_no_ja_no_en:
                                venue_dict_no_ja_no_en[venue_name] = {"lang": [detected_language], "count":1, "doi":[data["doi"]]}
                            elif venue_name in venue_dict_no_ja_no_en:
                                if detected_language not in venue_dict_no_ja_no_en[venue_name]["lang"]:
                                    venue_dict_no_ja_no_en[venue_name]["lang"].append(detected_language)
                                venue_dict_no_ja_no_en[venue_name]["count"] += 1
                                venue_dict_no_ja_no_en[venue_name]["doi"].append(data["doi"])

            if venue_dict_no_ja_no_en:
                venue_dict_no_ja_no_en_tot.update(venue_dict_no_ja_no_en)

        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            json.dump(venue_dict_no_ja_no_en_tot, json_file, ensure_ascii=False, indent=2)

        return venue_dict_no_ja_no_en_tot

    def find_titles_if_not_lang_citing(self, input_dir, output_json_path):
        list_of_zips = self.find_zip_subfiles(input_dir)
        title_dict_no_ja_no_en_tot = {}
        for zip_file in tqdm(list_of_zips):
            title_dict_no_ja_no_en = {}
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if 'title_list' in data:
                        title_lang = self.get_ja(data['title_list'])
                        if title_lang == '?':
                            title = data['title_list'][0]['title']
                            if title:
                                try:
                                    detected_language = detect(title)
                                except langdetect.LangDetectException:
                                    print(title)
                                    detected_language = "unknown"
                                if title not in title_dict_no_ja_no_en:
                                    title_dict_no_ja_no_en[title] = {"lang": [detected_language], "count":1, "doi":[data["doi"]]}
                                elif title in title_dict_no_ja_no_en:
                                    if detected_language not in title_dict_no_ja_no_en[title]["lang"]:
                                        title_dict_no_ja_no_en[title]["lang"].append(detected_language)
                                    title_dict_no_ja_no_en[title]["count"] += 1
                                    title_dict_no_ja_no_en[title]["doi"].append(data["doi"])

            if title_dict_no_ja_no_en:
                title_dict_no_ja_no_en_tot.update(title_dict_no_ja_no_en)

        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            json.dump(title_dict_no_ja_no_en_tot, json_file, ensure_ascii=False, indent=2)

        return title_dict_no_ja_no_en_tot

    def find_creators_if_not_lang_citing(self, input_dir, output_json_path):
        list_of_zips = self.find_zip_subfiles(input_dir)
        creators_dict_no_ja_no_en_tot = {}
        for zip_file in tqdm(list_of_zips):
            creators_dict_no_ja_no_en = {}
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if data.get("creator_list"):
                        creators = data.get("creator_list")
                        for c in creators:
                            names = c['names'] if 'names' in c else ''
                            creator_lang = self.get_ja(names)
                            if creator_lang == '?':
                                ja_name = c['names'][0]
                                last_name = ja_name['last_name'] if 'last_name' in ja_name else ''
                                first_name = ja_name['first_name'] if 'first_name' in ja_name else ''
                                full_name = ''
                                if last_name:
                                    full_name += last_name
                                    if first_name:
                                        full_name += f', {first_name}'
                                if full_name:
                                    try:
                                        detected_language = detect(full_name)
                                    except langdetect.LangDetectException:
                                        print(full_name)
                                        detected_language = "unknown"
                                    if full_name not in creators_dict_no_ja_no_en:
                                        creators_dict_no_ja_no_en[full_name] = {"lang": [detected_language], "count":1, "doi":[data["doi"]]}
                                    elif full_name in creators_dict_no_ja_no_en:
                                        if detected_language not in creators_dict_no_ja_no_en[full_name]["lang"]:
                                            creators_dict_no_ja_no_en[full_name]["lang"].append(detected_language)
                                        creators_dict_no_ja_no_en[full_name]["count"] += 1
                                        creators_dict_no_ja_no_en[full_name]["doi"].append(data["doi"])

            if creators_dict_no_ja_no_en:
                creators_dict_no_ja_no_en_tot.update(creators_dict_no_ja_no_en)

        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            json.dump(creators_dict_no_ja_no_en_tot, json_file, ensure_ascii=False, indent=2)

    def count_lang_in_json(self, json_input):
        with open(json_input, 'r', encoding='utf-8') as json_file:
            my_dict = json.load(json_file)
            count_total_keys = 0
            languages = dict()
            for each_key in my_dict:
                count_total_keys += my_dict[each_key]['count']
                for each_lang in my_dict[each_key]['lang']:
                    if each_lang in languages:
                        languages[each_lang] += my_dict[each_key]['count']
                    else:
                        languages[each_lang] = my_dict[each_key]['count']
            return languages, count_total_keys

    def update_lang(self, json_input):
        with open(json_input, 'r', encoding='utf-8') as json_file:
            my_dict = json.load(json_file)
            for each_key in my_dict:
                if len(my_dict[each_key]['lang']) > 1:
                    lang = detect_langs(each_key)[0]
                    new_lang = lang.lang
                    my_dict[each_key]['lang'] = [new_lang]
        with open(json_input, 'w', encoding='utf-8') as json_file:
            json.dump(my_dict, json_file, ensure_ascii=False, indent=2)
            print(f"{json_file} updated succesfully")

    #CITED
    def find_titles_if_not_lang_cited(self, input_dir, output_json_path):
        list_of_zips = self.find_zip_subfiles(input_dir)
        title_dict_no_ja_no_en_tot = {}
        for zip_file in tqdm(list_of_zips):
            title_dict_no_ja_no_en = {}
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if data.get("citation_list"):
                        cit_list_entities = [x for x in data["citation_list"] if x.get('doi')]
                        if cit_list_entities:
                            for citation in cit_list_entities:
                                if 'title_list' in citation:
                                    title_lang = self.get_ja(citation['title_list'])
                                    if title_lang == '?':
                                        title = citation['title_list'][0]['title']
                                        if title:
                                            try:
                                                detected_language = detect(title)
                                            except langdetect.LangDetectException:
                                                print(title)
                                                detected_language = "unknown"
                                            if title not in title_dict_no_ja_no_en:
                                                title_dict_no_ja_no_en[title] = {"lang": [detected_language], "count":1, "doi":{citation["doi"]}}
                                            elif title in title_dict_no_ja_no_en:
                                                if detected_language not in title_dict_no_ja_no_en[title]["lang"]:
                                                    title_dict_no_ja_no_en[title]["lang"].append(detected_language)
                                                title_dict_no_ja_no_en[title]["count"] += 1
                                                title_dict_no_ja_no_en[title]["doi"].add(citation["doi"])

            if title_dict_no_ja_no_en:
                title_dict_no_ja_no_en_tot.update(title_dict_no_ja_no_en)

        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            # Convert sets to lists before dumping to JSON
            title_dict_no_ja_no_en_tot_serializable = {
                key: {
                    "lang": value["lang"],
                    "count": value["count"],
                    "doi": list(value["doi"])
                }
                for key, value in title_dict_no_ja_no_en_tot.items()
            }
            json.dump(title_dict_no_ja_no_en_tot_serializable, json_file, ensure_ascii=False, indent=2)

        return title_dict_no_ja_no_en_tot


    def find_creators_if_not_lang_cited(self, input_dir, output_json_path):
        list_of_zips = self.find_zip_subfiles(input_dir)
        creators_dict_no_ja_no_en_tot = {}
        for zip_file in tqdm(list_of_zips):
            creators_dict_no_ja_no_en = {}
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if data.get("citation_list"):
                        cit_list_entities = [x for x in data["citation_list"] if x.get('doi')]
                        if cit_list_entities:
                            for citation in cit_list_entities:
                                if citation.get("creator_list"):
                                    creators = citation.get("creator_list")
                                    for c in creators:
                                        names = c['names'] if 'names' in c else ''
                                        creator_lang = self.get_ja(names)
                                        if creator_lang == '?':
                                            ja_name = c['names'][0]
                                            last_name = ja_name['last_name'] if 'last_name' in ja_name else ''
                                            first_name = ja_name['first_name'] if 'first_name' in ja_name else ''
                                            full_name = ''
                                            if last_name:
                                                full_name += last_name
                                                if first_name:
                                                    full_name += f', {first_name}'
                                            if full_name:
                                                try:
                                                    detected_language = detect(full_name)
                                                except langdetect.LangDetectException:
                                                    print(full_name)
                                                    detected_language = "unknown"
                                                if full_name not in creators_dict_no_ja_no_en:
                                                    creators_dict_no_ja_no_en[full_name] = {"lang": [detected_language],
                                                                                            "count": 1,
                                                                                            "doi": {citation["doi"]}}
                                                elif full_name in creators_dict_no_ja_no_en:
                                                    if detected_language not in creators_dict_no_ja_no_en[full_name]["lang"]:
                                                        creators_dict_no_ja_no_en[full_name]["lang"].append(
                                                            detected_language)
                                                    creators_dict_no_ja_no_en[full_name]["count"] += 1
                                                    creators_dict_no_ja_no_en[full_name]["doi"].add(citation["doi"])
            if creators_dict_no_ja_no_en:
                creators_dict_no_ja_no_en_tot.update(creators_dict_no_ja_no_en)

        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            # Convert sets to lists before dumping to JSON
            creators_dict_no_ja_no_en_tot_serializable = {
                key: {
                    "lang": value["lang"],
                    "count": value["count"],
                    "doi": list(value["doi"])
                }
                for key, value in creators_dict_no_ja_no_en_tot.items()
            }
            json.dump(creators_dict_no_ja_no_en_tot_serializable, json_file, ensure_ascii=False, indent=2)

        return creators_dict_no_ja_no_en_tot



    def find_publishers_if_not_lang_cited(self, input_dir, output_json_path):
        list_of_zips = self.find_zip_subfiles(input_dir)
        publishers_dict_no_ja_no_en_tot = {}
        for zip_file in tqdm(list_of_zips):
            publishers_dict_no_ja_no_en = {}
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if data.get("citation_list"):
                        cit_list_entities = [x for x in data["citation_list"] if x.get('doi')]
                        if cit_list_entities:
                            for citation in cit_list_entities:
                                if 'publisher_list' in citation:
                                    publisher_lang = self.get_ja(citation['publisher_list'])
                                    if publisher_lang == '?':
                                        publisher = citation['publisher_list'][0]['publisher_name']
                                        if publisher:
                                            try:
                                                detected_language = detect(publisher)
                                            except langdetect.LangDetectException:
                                                print(publisher)
                                                detected_language = "unknown"
                                            if publisher not in publishers_dict_no_ja_no_en:
                                                publishers_dict_no_ja_no_en[publisher] = {"lang": [detected_language],
                                                                                          "count": 1,
                                                                                          "doi": {citation["doi"]}}
                                            elif publisher in publishers_dict_no_ja_no_en:
                                                if detected_language not in publishers_dict_no_ja_no_en[publisher]["lang"]:
                                                    publishers_dict_no_ja_no_en[publisher]["lang"].append(detected_language)
                                                publishers_dict_no_ja_no_en[publisher]["count"] += 1
                                                publishers_dict_no_ja_no_en[publisher]["doi"].add(citation["doi"])
            if publishers_dict_no_ja_no_en:
                publishers_dict_no_ja_no_en_tot.update(publishers_dict_no_ja_no_en)

        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            # Convert sets to lists before dumping to JSON
            publishers_dict_no_ja_no_en_tot_serializable = {
                key: {
                    "lang": value["lang"],
                    "count": value["count"],
                    "doi": list(value["doi"])
                }
                for key, value in publishers_dict_no_ja_no_en_tot.items()
            }
            json.dump(publishers_dict_no_ja_no_en_tot_serializable, json_file, ensure_ascii=False, indent=2)

        return publishers_dict_no_ja_no_en_tot

    def find_journal_title_if_not_lang_cited(self, input_dir, output_json_path):
        list_of_zips = self.find_zip_subfiles(input_dir)
        journal_title_dict_no_ja_no_en_tot = {}
        for zip_file in tqdm(list_of_zips):
            journal_title_dict_no_ja_no_en = {}
            zip_f = zipfile.ZipFile(zip_file)
            source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
            source_dict = []
            # here I create a list containing all the json in the zip folder as dictionaries
            for json_file in source_data:
                f = zip_f.open(json_file, 'r')
                my_dict = json.load(f)
                source_dict.append(my_dict)
            for entity in source_dict:
                data = entity.get("data")
                if data:
                    if data.get("citation_list"):
                        cit_list_entities = [x for x in data["citation_list"] if x.get('doi')]
                        if cit_list_entities:
                            for citation in cit_list_entities:
                                if 'journal_title_name_list' in citation:
                                    venue_name = self.get_ja(citation['journal_title_name_list'])
                                    if venue_name == '?':
                                        full_venue = [item for item in citation['journal_title_name_list'] if 'type' in item
                                                      if item['type'] == 'full']
                                        if full_venue:
                                            venue_name = full_venue[0]['journal_title_name']
                                        else:
                                            venue_name = data['journal_title_name_list'][0]['journal_title_name']
                                        try:
                                            detected_language = detect(venue_name)
                                        except langdetect.LangDetectException:
                                            print(venue_name)
                                            detected_language = "unknown"
                                        if venue_name not in journal_title_dict_no_ja_no_en:
                                            journal_title_dict_no_ja_no_en[venue_name] = {"lang": [detected_language],
                                                                                      "count": 1,
                                                                                      "doi": {citation["doi"]}}
                                        elif venue_name in journal_title_dict_no_ja_no_en:
                                            if detected_language not in journal_title_dict_no_ja_no_en[venue_name]["lang"]:
                                                journal_title_dict_no_ja_no_en[venue_name]["lang"].append(detected_language)
                                            journal_title_dict_no_ja_no_en[venue_name]["count"] += 1
                                            journal_title_dict_no_ja_no_en[venue_name]["doi"].add(citation["doi"])
            if journal_title_dict_no_ja_no_en:
                journal_title_dict_no_ja_no_en_tot.update(journal_title_dict_no_ja_no_en)

        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            # Convert sets to lists before dumping to JSON
            journal_title_dict_no_ja_no_en_tot_serializable = {
                key: {
                    "lang": value["lang"],
                    "count": value["count"],
                    "doi": list(value["doi"])
                }
                for key, value in journal_title_dict_no_ja_no_en_tot.items()
            }
            json.dump(journal_title_dict_no_ja_no_en_tot_serializable, json_file, ensure_ascii=False, indent=2)

        return journal_title_dict_no_ja_no_en_tot

    def find_zip_subfiles(self, jalc_json_dir):
        els_to_be_skipped = []
        input_dir_cont = os.listdir(jalc_json_dir)
        # for element in the list of elements in jalc_json_dir (input)
        for el in input_dir_cont:  # should be one (the input dir contains 1 zip)
            if el.startswith("._"):
                # skip elements starting with ._
                els_to_be_skipped.append(os.path.join(jalc_json_dir, el))
            else:
                if el.endswith(".zip"):
                    base_name = el.replace('.zip', '')
                    if [x for x in os.listdir(jalc_json_dir) if
                        x.startswith(base_name) and x.endswith("decompr_zip_dir")]:
                        els_to_be_skipped.append(os.path.join(jalc_json_dir, el))
        req_type = ".zip"
        all_input_zip = []
        els_to_be_skipped_cont = [x for x in els_to_be_skipped if x.endswith(".zip")]

        if els_to_be_skipped_cont:
            for el_to_skip in els_to_be_skipped_cont:
                if el_to_skip.startswith("._"):
                    continue
                base_name_el_to_skip = basename(el_to_skip).replace('.zip', '')
                for el in os.listdir(jalc_json_dir):
                    if el.startswith(base_name_el_to_skip) and el.endswith("decompr_zip_dir"):
                        all_input_zip = [os.path.join(jalc_json_dir, el, file) for file in
                                         os.listdir(os.path.join(jalc_json_dir, el)) if
                                         not file.endswith(".json") and not file.startswith("._")]

        if len(all_input_zip) == 0:
            for zip_lev0 in os.listdir(jalc_json_dir):
                all_input_zip, targz_fd = self.get_all_files_by_type(os.path.join(jalc_json_dir, zip_lev0), req_type)
        return all_input_zip


