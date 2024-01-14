import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
import time
import multiprocessing
import psutil

import streamlit as st

if 'save' not in st.session_state:
    st.session_state.save = []


def task(v):
    """session state does not work here"""
    time.sleep(0.5)
    return v * v


if __name__ == '__main__':
    jobs = [1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5]
    num_workers = multiprocessing.cpu_count()
    processed_jobs = []

    # 2 col
    col1, col2 = st.columns(2)

    with col1:

        start16 = st.button('start work 16 cpus')

        if start16:
            start_time = time.time()
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                for j in jobs:
                    pj = executor.submit(task, j)
                    processed_jobs.append(pj)

                for future in concurrent.futures.as_completed(processed_jobs):
                    try:
                        res = future.result()
                        # st.write(f'res: {res}')

                        # Incrementally save the completed task so far.
                        st.session_state.save.append(res)

                    except concurrent.futures.process.BrokenProcessPool as ex:
                        raise Exception(ex)

                st.success(f'Completed in {time.time() - start_time} seconds')
                st.write('#### Completed Jobs')
                st.write(f'{st.session_state.save[-1]}')

    with col2:

        start8 = st.button('start work 8 cpus')

        if start8:
            start_time = time.time()
            with ProcessPoolExecutor(max_workers=8) as executor:
                for j in jobs:
                    pj = executor.submit(task, j)
                    processed_jobs.append(pj)

                for future in concurrent.futures.as_completed(processed_jobs):
                    try:
                        res = future.result()
                        # st.write(f'res: {res}')

                        # Incrementally save the completed task so far.
                        st.session_state.save.append(res)

                    except concurrent.futures.process.BrokenProcessPool as ex:
                        raise Exception(ex)

                st.success(f'Completed in {time.time() - start_time} seconds')
                st.write('#### Completed Jobs')
                st.write(f'{st.session_state.save[-1]}')

    st.write("CPU Core Count:", multiprocessing.cpu_count())
    
    # Get the virtual memory status
    vm = psutil.virtual_memory()

    # Print the total, available and used memory
    st.write(f'Total memory: {vm.total / 1024**3} GB')
    st.write(f'Available memory: {vm.available / 1024**3} GB')
    st.write(f'Used memory: {vm.used / 1024**3} GB')
############### Testing with streamlit + ray ############################################################################################################

from unstructured.partition.image import partition_image
import time
# from glob import glob
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
# images = glob("lumiilumii messages folder-20240108\lumiilumii messages folder\*.png")
images = st.file_uploader("Upload Images", type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)

if images:
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