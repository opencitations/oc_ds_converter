import os
import zipfile
import shutil
from tqdm import tqdm
from oc_ds_converter.lib.jsonmanager import *
from oc_ds_converter.lib.file_manager import normalize_path
from argparse import ArgumentParser



def preprocessing(jalc_json_dir: str, output_dir:str):


    els_to_be_skipped = []
    input_dir_cont = os.listdir(jalc_json_dir)
    for el in input_dir_cont:# should be one (the input dir contains 1 zip)
        if el.startswith("._"):
            # skip elements starting with ._
            els_to_be_skipped.append(os.path.join(jalc_json_dir, el))
        else:
            if el.endswith(".zip"):
                base_name = el.replace('.zip', '')
                if [x for x in os.listdir(jalc_json_dir) if
                    x.startswith(base_name) and x.endswith("decompr_zip_dir")]:
                    els_to_be_skipped.append(os.path.join(jalc_json_dir, el))

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_unzipped_files = []
    els_to_be_skipped_cont = [x for x in els_to_be_skipped if x.endswith(".zip")]
    if els_to_be_skipped_cont:
        for el_to_skip in els_to_be_skipped_cont:
            if el_to_skip.startswith("._"):
                continue
            base_name_el_to_skip = os.path.basename(el_to_skip).replace('.zip', '')
            for directory in os.listdir(jalc_json_dir):
                if directory == base_name_el_to_skip + "_decompr_zip_dir":
                    # if el.startswith(base_name_el_to_skip) and el.endswith("decompr_zip_dir"):
                    for dir_lev2 in os.listdir(os.path.join(jalc_json_dir, directory)):
                        all_unzipped_files = [os.path.join(jalc_json_dir, directory, dir_lev2, file) for file in os.listdir(os.path.join(jalc_json_dir, directory, dir_lev2)) if not file.startswith("._")]

    if len(all_unzipped_files) == 0:
        for zip_lev0 in os.listdir(jalc_json_dir):
            if zip_lev0.endswith("zip") and not zip_lev0.startswith("._"):
                with zipfile.ZipFile(os.path.join(jalc_json_dir, zip_lev0), 'r') as zip_ref:
                    dest_dir = os.path.join(jalc_json_dir, zip_lev0).replace('.zip', '') + "_decompr_zip_dir"
                    if not exists(dest_dir):
                        makedirs(dest_dir)
                    zip_ref.extractall(dest_dir)
                    print(f"Unzipped to {dest_dir}")
                for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                    for cur_file in cur_files:
                        if not basename(cur_file).startswith("."):
                            all_unzipped_files.append(cur_dir + sep + cur_file)



    # Filter for zip files (files with ".zip" extension)
    zip_files = [file for file in all_unzipped_files if file.endswith(".zip")]
    not_zip_files = [file for file in all_unzipped_files if not file.endswith(".zip")]

    # copy all extra files as they are in the output dir
    for file in not_zip_files:
        if os.path.isfile(file):
            shutil.copy(file, output_dir)

    # Iterate over the zip files and print the names of files they contain
    for zip_file in tqdm(zip_files, desc="Processing ZIP Files"):
        if not zip_file.endswith(".zip"):
            shutil.copy(zip_file, output_dir)
        else:
            # manage the resizing
            # Full path to the zip file
            zip_file_path = zip_file
            first_level_path = os.path.dirname(zip_file_path)
            print("executing", zip_file_path)
            # Open the zip file for reading
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:

                directories = [entry for entry in zip_ref.namelist() if entry.endswith('/')]
                # List the names of files within the zip (the relevant one, the json containing the citation information)
                file_names = [x for x in zip_ref.namelist() if x.endswith(".json") and "doiList" not in x and x not in directories]
                extra_file_names = [x for x in zip_ref.namelist() if x not in file_names and x not in directories]
                if len(file_names) <= 100:
                    print("less than 100")
                    shutil.copy(zip_file_path, output_dir)

                else:
                    # if there are more than 100 jsons, they should be divided as many zips as the total number of jsons/ 100.
                    # Each new zip should also contain all the extra files which where stored in the original zip
                    print("more than 100: ", len(file_names))
                    zip_basename = zip_file_path.replace(".zip", "").replace(first_level_path, "").replace("/", "").replace(
                    "\\", "")


                    zip_part_counter = 0

                    while len(file_names):
                        # nominare con numerazione la cartella del nuovo zip di arrivo
                        zip_basename_w_counter = zip_basename + "_" + str(zip_part_counter)
                        # definire il path per la cartella dove verranno estratti i files
                        sliced_out_name = os.path.join(output_dir, zip_basename_w_counter)
                        # creare la cartella
                        os.makedirs(sliced_out_name, exist_ok=True)
                        # copiare nella cartella tutti i files extra che non sono json con informazioni sulle citazioni
                        for non_zip in extra_file_names:
                            if not os.path.isdir(non_zip) and 'doiList' in non_zip:
                                '''We use os.path.basename(file_info.filename) to extract just the filename
                                without the folder structure. We create a new file in the destination folder 
                                with the extracted filename and write the content of the file from the ZIP archive into it.'''
                                file_info = zip_ref.getinfo(non_zip)
                                extracted_file_path = os.path.join(sliced_out_name,
                                                                       os.path.basename(file_info.filename))
                                with open(extracted_file_path, 'wb') as extracted_file:
                                    extracted_file.write(zip_ref.read(non_zip))

                        # se i files rimasti da processare sono più di cento
                        if len(file_names) > 100:
                            print("json to be processed in this zip:", len(file_names))

                            files_slice = file_names[:100]
                            file_names = file_names[100:]
                            zip_part_counter +=1

                            for fs in files_slice:
                                file_info = zip_ref.getinfo(fs)
                                extracted_file_path = os.path.join(sliced_out_name,
                                                                   os.path.basename(file_info.filename))
                                with open(extracted_file_path, 'wb') as extracted_file:
                                    extracted_file.write(zip_ref.read(fs))


                        else:
                            print("last", len(file_names), "jsons to be processed")
                            files_slice = file_names
                            file_names = []
                            zip_part_counter += 1

                            for fs in files_slice:
                                file_info = zip_ref.getinfo(fs)
                                extracted_file_path = os.path.join(sliced_out_name,
                                                                   os.path.basename(file_info.filename))
                                with open(extracted_file_path, 'wb') as extracted_file:
                                    extracted_file.write(zip_ref.read(fs))

                        # Directory to zip and delete
                        directory_to_zip = sliced_out_name
                        # Parent directory
                        parent_directory = os.path.dirname(directory_to_zip)
                        # Name the zip file
                        zip_file_name = directory_to_zip + ".zip"
                        # Full path to the destination zip file
                        zip_file_path = os.path.join(parent_directory, zip_file_name)
                        # create a zip file
                        with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            for foldername, subfolders, filenames in os.walk(directory_to_zip):
                                for filename in filenames:
                                    file_path = os.path.join(foldername, filename)
                                    relative_path = os.path.relpath(file_path, directory_to_zip)
                                    zipf.write(file_path, relative_path)
                        # After creating the zip, delete the original directory
                        shutil.rmtree(directory_to_zip)
                        print("Just Completed zip:", zip_file_path)

    # At the end of the process, create a ZIP archive of the output_dir and rename it
    print("Zipping the output directory")
    shutil.make_archive(output_dir, 'zip', output_dir)
    print("Removing output directory")
    shutil.rmtree(output_dir)  # Delete the original directory'''

if __name__ == '__main__':
    arg_parser = ArgumentParser('jalc.py', description='''This script does the preprocessing of the initial JALC dump, in particular it splits the original zip files in smaller ones if they contain more than 100 JSON files 
                                and it modifies the dump's original structure by removing the intermediate directories and bringing it to the following structure: dump.zip -> prefixes.zip -> json files''')
    arg_parser.add_argument('-ja', '--jalc', dest='jalc_json_dir', required=True, help='Directory that contains the original zipped dump')
    arg_parser.add_argument('-out', '--output', dest='output_dir', required=True, help='Directory where the files of the original dump will be stored, and the directory will be zipped.')
    args = arg_parser.parse_args()
    jalc_json_dir = args.jalc_json_dir
    jalc_json_dir = normalize_path(jalc_json_dir)
    output_dir = args.output_dir
    output_dir = normalize_path(output_dir)
    preprocessing(jalc_json_dir, output_dir)

