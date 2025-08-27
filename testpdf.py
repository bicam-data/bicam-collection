import pymupdf

pdf_path = "GOVPUB-Y4_AG8_1-PURL-LPS46012.pdf"

doc = pymupdf.open(pdf_path)


print(doc[0].get_text())