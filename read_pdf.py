import PyPDF2

pdf_path = '/Users/satyanarendrareddybudati/Desktop/bigdata-cricket/Big Data Project.pdf'
output_path = '/Users/satyanarendrareddybudati/Desktop/bigdata-cricket/pdf_content.txt'

try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        text = ''
        for page in reader.pages:
            text += page.extract_text()
        
        with open(output_path, 'w') as out:
            out.write(text)
        print(f"PDF content written to {output_path}")
except Exception as e:
    print(f'Error: {e}')
