import os
import os.path
import unittest
import shutil
from os.path import sep

from oc_ds_converter.preprocessing.jalc import preprocessing
import zipfile



BASE = os.path.join('test', 'preprocessing_jalc')
JALC_DIR = os.path.join(BASE, 'ZIP_JOCI_TEST')
JALC_DIR = os.path.abspath(JALC_DIR)

OUT_DIR = os.path.join(BASE, 'OUT_DIR')
OUT_DIR = os.path.abspath(OUT_DIR)




class TestJalcPreprocessing(unittest.TestCase):
    def test_base_decompress_and_rearrange(self):

        if os.path.exists(OUT_DIR + '.zip'):
            os.remove(OUT_DIR + '.zip')

        for el in os.listdir(JALC_DIR):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(JALC_DIR, el))


        def count_files(dir_with_dump_to_ckeck):
            count = 0

            all_unzipped_files = []
            for zip_lev0 in os.listdir(dir_with_dump_to_ckeck):
                if zip_lev0.endswith("zip") and not zip_lev0.startswith("._"):
                    with zipfile.ZipFile(os.path.join(dir_with_dump_to_ckeck, zip_lev0), 'r') as zip_ref:
                        dest_dir = os.path.join(dir_with_dump_to_ckeck, zip_lev0).replace('.zip', '') + "_decompr_zip_dir"
                        if not os.path.exists(dest_dir):
                            os.makedirs(dest_dir)
                        zip_ref.extractall(dest_dir)
                        print(f"Unzipped to {dest_dir}")
                    for cur_dir, cur_subdir, cur_files in os.walk(dest_dir):
                        for cur_file in cur_files:
                            if not os.path.basename(cur_file).startswith("."):
                                all_unzipped_files.append(cur_dir + sep + cur_file)

            zip_files = [file for file in all_unzipped_files if file.endswith(".zip")]
            not_zip_files = [file for file in all_unzipped_files if not file.endswith(".zip")]

            for file in not_zip_files:
                if os.path.isfile(file):
                    count += 1

            for zip_file in zip_files:
                zip_file_path = zip_file
                # Open the zip file for reading
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    directories = [entry for entry in zip_ref.namelist() if entry.endswith('/')]
                    # List the names of files within the zip (the relevant one, the json containing the citation information)
                    file_names = [x for x in zip_ref.namelist() if
                                  x.endswith(".json") and "doiList" not in x and x not in directories]
                    extra_file_names = [x for x in zip_ref.namelist() if x not in file_names and x not in directories]
                    if len(file_names) > 100 and len(file_names) % 100 != 0:
                        number_of_zip = len(file_names) // 100 + 1
                    elif len(file_names) > 100 and len(file_names) % 100 == 0:
                        number_of_zip = len(file_names) // 100
                    elif len(file_names) < 100:
                        number_of_zip = 1
                    count += number_of_zip
                    number_extra_files = len(extra_file_names)
                    count += number_extra_files
                    count += len(file_names)
            return count

        expected_count = count_files(JALC_DIR)
        preprocessing(JALC_DIR, OUT_DIR, 5)

        def count_elements_in_zip(zip_file):
            element_count = 0
            with zipfile.ZipFile(zip_file, 'r') as zipf:
                for item in zipf.infolist():
                    if item.is_dir():
                        continue
                    else:
                        element_count += 1
                    if item.filename.endswith('.zip'):
                        # Extract the nested zip file into a temporary folder
                        nested_zip_path = os.path.join('temp', item.filename)
                        zipf.extract(item, 'temp')
                        # Recursively count elements in the nested zip
                        element_count += count_elements_in_zip(nested_zip_path)
                        # Clean up the extracted nested zip file
                        os.remove(nested_zip_path)
            return element_count

        real_count = count_elements_in_zip(OUT_DIR+".zip")
        self.assertEqual(expected_count, real_count)

        os.remove(OUT_DIR + '.zip')

        for el in os.listdir(JALC_DIR):
            if el.endswith("decompr_zip_dir"):
                shutil.rmtree(os.path.join(JALC_DIR, el))






if __name__ == '__main__':
    unittest.main()







