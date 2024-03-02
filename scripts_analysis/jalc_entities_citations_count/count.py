from scripts_analysis.jalc_languages_metadata.jalc_languages_metadata_count import *

class_for_count = CountMetadataLang()
all_input_zip = class_for_count.find_zip_subfiles(r"D:\JOCI\JALC_PRE_24")
count_citing_entities = 0
citations_count = 0
for zip_file in tqdm(all_input_zip):
    zip_f = zipfile.ZipFile(zip_file)
    source_data = [x for x in zip_f.namelist() if not x.startswith("doiList")]
    source_dict = []
    # here I create a list containing all the json in the zip folder as dictionaries
    for json_file in source_data:
        f = zip_f.open(json_file, 'r')
        my_dict = json.load(f)
        source_dict.append(my_dict)
    for entity_dict in source_dict:
        data = entity_dict["data"]
        if data.get("doi"):
            count_citing_entities += 1
            if data.get("citation_list"):
                cit_list_entities = [x for x in data["citation_list"] if x.get("doi")]
                if cit_list_entities:
                    for cited in cit_list_entities:
                        citations_count += 1
print(count_citing_entities, citations_count)



