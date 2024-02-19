import zipfile
import json
from jalc_languages_metadata_count import CountMetadataLang
from tqdm import tqdm

def update_dict(n_lang, dict_to_update, is_citing):
    if is_citing:
        information_loss = 0
        if n_lang == 1:
            dict_to_update["citing"]["one_language"] += 1
        elif n_lang == 2:
            dict_to_update["citing"]["two_languages"] += 1
            information_loss = n_lang - 1
        elif n_lang > 2:
            dict_to_update["citing"]["more_than_two_languages"] += 1
            information_loss = n_lang - 1
            if n_lang in dict_to_update["citing"]["tot_lang"]:
                dict_to_update["citing"]["tot_lang"][n_lang] += 1
            else:
                dict_to_update["citing"]["tot_lang"][n_lang] = 1
        dict_to_update["citing"]["information_loss"] += information_loss
    else:
        information_loss = 0
        if n_lang == 1:
            dict_to_update["cited"]["one_language"] += 1
        elif n_lang == 2:
            dict_to_update["cited"]["two_languages"] += 1
            information_loss = n_lang - 1
        elif n_lang > 2:
            dict_to_update["cited"]["more_than_two_languages"] += 1
            information_loss = n_lang - 1
            if n_lang in dict_to_update["cited"]["tot_lang"]:
                dict_to_update["cited"]["tot_lang"][n_lang] += 1
            else:
                dict_to_update["cited"]["tot_lang"][n_lang] = 1
        dict_to_update["cited"]["information_loss"] += information_loss



def execute_count(input_dir):
    c = CountMetadataLang()
    all_zips = c.find_zip_subfiles(input_dir)
    titles = {"citing":{"one_language":0, "two_languages":0, "more_than_two_languages":0, "information_loss":0, "tot_lang":dict()},
              "cited":{"one_language":0, "two_languages":0, "more_than_two_languages":0, "information_loss":0, "tot_lang":dict()}}
    journal_titles = {"citing":{"one_language":0, "two_languages":0, "more_than_two_languages":0, "information_loss":0, "tot_lang":dict()},
                        "cited":{"one_language":0, "two_languages":0, "more_than_two_languages":0, "information_loss":0, "tot_lang":dict()}}
    publishers = {"citing":{"one_language":0, "two_languages":0, "more_than_two_languages":0, "information_loss":0, "tot_lang":dict()}}
    authors = {"citing":{"one_language":0, "two_languages":0, "more_than_two_languages":0, "information_loss":0, "tot_lang":dict()},
              "cited":{"one_language":0, "two_languages":0, "more_than_two_languages":0, "information_loss":0, "tot_lang":dict()}}

    for zip_file in tqdm(all_zips):
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
                    title_list = data['title_list']
                    n_lang_titles = len(title_list)
                    update_dict(n_lang_titles, titles, True)
                if 'journal_title_name_list' in data:
                    full_venue = [item for item in data['journal_title_name_list'] if 'type' in item if item['type'] == 'full']
                    if full_venue:
                        n_lang_journal_titles = len(full_venue)
                    else:
                        n_lang_journal_titles = len(data['journal_title_name_list'])
                    update_dict(n_lang_journal_titles, journal_titles, True)
                if "creator_list" in data:
                    creators = data["creator_list"]
                    for c in creators:
                        if "names" in c:
                            n_lang_authors = len(c["names"])
                            update_dict(n_lang_authors, authors, True)
                if 'publisher_list' in data:
                    n_lang_publishers = len(data["publisher_list"])
                    update_dict(n_lang_publishers, publishers, True)
                if 'citation_list' in data:
                    cit_list_entities = [x for x in data["citation_list"] if x.get('doi')]
                    for citation in cit_list_entities:
                        if 'title_list' in citation:
                            title_list_cited = citation['title_list']
                            n_lang_titles_cited = len(title_list_cited)
                            update_dict(n_lang_titles_cited, titles, False)
                        if 'journal_title_name_list' in citation:
                            journal_title_name_list_cited = citation['journal_title_name_list']
                            full_venue_cited = [item for item in journal_title_name_list_cited if 'type' in item if
                                                item['type'] == 'full']
                            if full_venue_cited:
                                n_lang_journal_titles_cited = len(full_venue_cited)
                            else:
                                n_lang_journal_titles_cited = len(journal_title_name_list_cited)
                            update_dict(n_lang_journal_titles_cited, journal_titles, False)
                        if 'creator_list' in citation:
                            creators_cited = citation["creator_list"]
                            for c in creators_cited:
                                if "names" in c:
                                    n_lang_authors_cited = len(c["names"])
                                    update_dict(n_lang_authors_cited, authors, False)
    return titles, journal_titles, publishers, authors


