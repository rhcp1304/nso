import os
import re
from pptx import Presentation

def extract_all_potential_links_from_last_slide(pptx_file_path: str) -> list[str]:
    if not os.path.exists(pptx_file_path):
        print(f"Error: File not found at '{pptx_file_path}'")
        return []

    if not pptx_file_path.lower().endswith('.pptx'):
        print(f"Error: '{pptx_file_path}' is not a .pptx file.")
        return []

    found_urls = set()
    url_pattern = re.compile(r'https?://[^\s\]\)\}>"]+')

    try:
        prs = Presentation(pptx_file_path)
        if not prs.slides:
            print(f"No slides found in '{pptx_file_path}'.")
            return []

        last_slide = prs.slides[-1]
        print(f"Analyzing the last slide (Slide {len(prs.slides)}) for all potential links...")

        def find_urls_in_text_content(text_frame_obj):
            for paragraph in text_frame_obj.paragraphs:
                full_text = "".join([run.text for run in paragraph.runs])
                for match in url_pattern.finditer(full_text):
                    found_urls.add(match.group(0).strip())

        for shape in last_slide.shapes:
            if hasattr(shape, 'action') and shape.action.hyperlink:
                url = shape.action.hyperlink.address
                if url:
                    found_urls.add(url)

            if shape.has_text_frame:
                find_urls_in_text_content(shape.text_frame)
                for paragraph in shape.text_frame.paragraphs:
                    for run in paragraph.runs:
                        if run.hyperlink.address:
                            url = run.hyperlink.address
                            if url:
                                found_urls.add(url)

            if shape.has_table:
                for row in shape.table.rows:
                    for cell in row.cells:
                        if cell.text_frame:
                            find_urls_in_text_content(cell.text_frame)
                            for paragraph in cell.text_frame.paragraphs:
                                for run in paragraph.runs:
                                    if run.hyperlink.address:
                                        url = run.hyperlink.address
                                        if url:
                                            found_urls.add(url)

            if hasattr(shape, 'image') and hasattr(shape.image, 'hyperlink') and shape.image.hyperlink.address:
                url = shape.image.hyperlink.address
                if url:
                    found_urls.add(url)

        return list(found_urls)

    except Exception as e:
        print(f"An error occurred while processing the PPTX file: {e}")
        return []
