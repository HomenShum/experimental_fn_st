from unstructured.partition.html import partition_html
from unstructured.chunking.title import chunk_by_title

url = "https://understandingwar.org/backgrounder/russian-offensive-campaign-assessment-august-27-2023-0"
elements = partition_html(url=url)
chunks = chunk_by_title(elements)

for chunk in chunks:
    print(chunk)
    print("\n\n" + "-"*80)