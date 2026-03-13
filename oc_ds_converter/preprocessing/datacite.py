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


