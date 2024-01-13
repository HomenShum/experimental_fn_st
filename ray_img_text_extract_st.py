from unstructured.partition.image import partition_image
import time
from glob import glob
import ray
from typing import List, Any, Dict
import numpy as np
import os
from unstructured.chunking.title import chunk_by_title
import streamlit as st

# Tracks performance of the function
print("Starting timer... Non-Ray")
start = time.time()

# Reads all the images in the example-docs folder: mainpage_app\experimental_functions\lumiilumii messages folder-20240108
images = glob("lumiilumii messages folder-20240108\lumiilumii messages folder\*.png")
images_list = [img for img in images]

st.image(images_list[0], width=300)

print(len(images_list), "images found" + ". Time taken: ", time.time() - start)

elements = []

############################## Method 5: ray with data #########################################################################################
# ray.data.DataContext.get_current().execution_options.verbose_progress = True
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

if st.button("Run"):
    start = time.time()
    ds = (ray.data.read_images('lumiilumii messages folder-20240108\lumiilumii messages folder', include_paths=True).map(parse_img_file))
    print(ds.take_all())
    print("Method 5 Time taken: ", time.time() - start) # 59-63 seconds

    st.json(ds.take(1))