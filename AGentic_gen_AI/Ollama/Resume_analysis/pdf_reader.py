from PyPDF2 import PdfReader 

def exact_text(pdf_file)

    reader PDFReade(pdf_file)
    text = "" 
    
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text +