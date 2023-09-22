import os
import zipfile
import shutil
from tqdm import tqdm

# Input zip file
input_zip_file = "/Volumes/T7_Touch/LAVORO/JOCI/jalc_20220831.zip"
output_dir = "/Volumes/T7_Touch/LAVORO/jalc_20220831_RESIZED"
output_zip_file = "/Volumes/T7_Touch/LAVORO/jalc_20220831_RESIZED.zip"

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Destination directory where contents are extracted
destination_dir = "/Volumes/T7_Touch/LAVORO/JOCI"
first_level_path = input_zip_file.replace(".zip", "")

if not os.path.exists(first_level_path):
    # Open the zip file for reading
    with zipfile.ZipFile(input_zip_file, 'r') as zip_ref:
        # Extract all the contents to the destination directory
        zip_ref.extractall(destination_dir)

    print(f"Unzipped to {destination_dir}")

# List all files in the directory
file_list = os.listdir(first_level_path)

# Filter for zip files (files with ".zip" extension)
zip_files = [os.path.join(first_level_path, file) for file in file_list if file.endswith(".zip")]
not_zip_files = [os.path.join(first_level_path, file) for file in file_list if not file.endswith(".zip")]

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
        zip_file_path = os.path.join(first_level_path, zip_file)
        print("executing", zip_file_path)
        # Open the zip file for reading
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # List the names of files within the zip (the relevant one, the json containing the citation information)
            file_names = [x for x in zip_ref.namelist() if x.endswith(".json") and "doiList" not in x]
            if len(file_names) <=100:
                print("less than 100")
                shutil.copy(zip_file_path, output_dir)

            else:
                # if there are more than 100 jsons, they should be divided as many zips as the total number of jsons/ 100.
                # Each new zip should also contains all the extra files which where stored in the original zip
                print("more than 100: ", len(file_names))
                zip_basename = zip_file_path.replace(".zip", "").replace(first_level_path, "").replace("/", "").replace(
                    "\\", "")

                zip_basename_with_base = os.path.join(first_level_path, zip_basename)
                extra_file_names = [x for x in zip_ref.namelist() if x not in file_names and not os.path.isdir(x)]
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
                        if not os.path.isdir(non_zip) and os.path.isfile(non_zip):
                            shutil.copy(os.path.join(zip_basename_with_base, non_zip), sliced_out_name)

                    # se i files rimasti da processare sono più di cento
                    if len(file_names) > 100:
                        print("json to be processed in this zip:", len(file_names))

                        files_slice = file_names[:100]
                        file_names = file_names[100:]
                        zip_part_counter +=1

                        for fs in files_slice:
                            zip_ref.extract(fs, sliced_out_name)

                    else:
                        print("last", len(file_names), "jsons to be processed")
                        files_slice = file_names
                        file_names = []
                        zip_part_counter += 1
                        for fs in files_slice:
                            zip_ref.extract(fs, sliced_out_name)

                    # Directory to zip and delete
                    directory_to_zip = sliced_out_name
                    # Parent directory
                    parent_directory = os.path.dirname(directory_to_zip)
                    # Name the zip file
                    zip_file_name = directory_to_zip + ".zip"
                    # Full path to the destination zip file
                    zip_file_path = os.path.join(parent_directory, zip_file_name)
                    # create a zip file
                    shutil.make_archive(zip_file_path[:-4], 'zip', parent_directory, os.path.basename(directory_to_zip))
                    # After creating the zip, delete the original directory
                    shutil.rmtree(directory_to_zip)
                    print("Just Completed zip:", zip_file_path)

# At the end of the process, create a ZIP archive of the output_dir and rename it
print("Zipping the output directory")
shutil.make_archive(output_dir, 'zip', output_dir)
print("Removing output directory")
shutil.rmtree(output_dir)  # Delete the original directory