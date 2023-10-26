import csv
import os
import pathlib
import zipfile
from os.path import isdir, basename, exists
from os import walk, sep, makedirs
import zstandard as zstd
import json
from tqdm import tqdm
import tarfile


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




    def find_zip_subfiles(self, jalc_json_dir):
        els_to_be_skipped =[]
        input_dir_cont = os.listdir(jalc_json_dir)
        # for element in the list of elements in jalc_json_dir (input)
        for el in input_dir_cont:  # should be one (the input dir contains 1 zip)
            if el.startswith("._"):
                # skip elements starting with ._
                els_to_be_skipped.append(os.path.join(jalc_json_dir, el))
            else:
                if el.endswith(".zip"):
                    base_name = el.replace('.zip', '')
                    if [x for x in os.listdir(jalc_json_dir) if x.startswith(base_name) and x.endswith("decompr_zip_dir")]:
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


# To execute the function you need to use the method "call_functions_for_all_zips"

