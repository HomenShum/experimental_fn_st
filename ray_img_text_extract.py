from unstructured.partition.image import partition_image
import time
from glob import glob
import ray
from typing import List, Any, Dict
import numpy as np
import os
from unstructured.chunking.title import chunk_by_title
import pandas as pd

# Tracks performance of the function
print("Starting timer... Non-Ray")
start = time.time()

# Reads all the images in the example-docs folder: mainpage_app\experimental_functions\lumiilumii messages folder-20240108
images = glob("mainpage_app\experimental_functions\lumiilumii messages folder-20240108\lumiilumii messages folder\*.png")
images_list = [img for img in images]
print(len(images_list), "images found" + ". Time taken: ", time.time() - start)

elements = []
# ############################# # Method 1: for loop ############################################################################################
# for img in images_list:
#     elements.append(partition_image(img))
# print("Method 1 Time taken: ", time.time() - start) # 106.88 seconds

# ############################# # Method 2: list comprehension ##################################################################################
# start = time.time()
# elements = [partition_image(img) for img in images_list]
# print("Method 2 Time taken: ", time.time() - start) # 107.56 seconds

# ############################# # Method 3: map #################################################################################################
# start = time.time()
# elements = list(map(partition_image, images_list))
# print("Method 3 Time taken: ", time.time() - start) # 102.93 seconds

# ############################## # Method 4: ray #################################################################################################
# # Initialize Ray
# start = time.time()
# ray.init()
# print("Ray init time: ", time.time() - start) # 4.4 seconds

# start = time.time()
# @ray.remote
# def process_image(img: str) -> List:
#     return chunk_by_title(partition_image(img), combine_text_under_n_chars=500, max_characters=1500)

# ds = (ray.data.read_images('mainpage_app\experimental_functions\lumiilumii messages folder-20240108\lumiilumii messages folder', include_paths=True))

# results = []
# for img in ds.iter_rows():
#     results.append(process_image.remote(img['path']))

# # Gather results
# elements = ray.get(results)
# for el in elements:
#     for e in el:
#         print(e)
# print("Method 4 Time taken: ", time.time() - start) # 74.7 seconds

############################## Method 5: ray with data (fastest so far) #########################################################################################
# ray.data.DataContext.get_current().execution_options.verbose_progress = True
start = time.time()
# ds = ray.data.read_images(images_list).map_batches(process_image)
def parse_img_file(row: Dict[str, Any]) -> Dict[str, Any]:
    row["filename"] = os.path.basename(row["path"])
    elements = partition_image(row["path"])
    unstructured_chunks = chunk_by_title(elements, combine_text_under_n_chars=500, max_characters=1500)
    row["extracted_text"] = [str(chunk) for chunk in unstructured_chunks]

    result = {}
    result['filename'] = row['filename']
    result['extracted_text'] = row['extracted_text']
    return result

ds = (ray.data.read_images('mainpage_app\experimental_functions\lumiilumii messages folder-20240108\lumiilumii messages folder', include_paths=True).map(parse_img_file))
# print(ds.take_all())
final_df = pd.DataFrame(ds.take_all())
# Sort the DataFrame by the filename column
final_df = final_df.sort_values('filename')

# Write the sorted DataFrame to a CSV file
final_df.to_csv(f'mainpage_app\experimental_functions\lumiilumii_20240108_extracted_text.csv', index=False)

print("Method 5 Time taken: ", time.time() - start) # 59-63 seconds

