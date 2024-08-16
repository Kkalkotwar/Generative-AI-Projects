from pdf2docx import Converter
import streamlit as st
import boto3
import os
import time
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def pdf_to_word(pdf_file, word_file):
    # Create a Converter object
    cv = Converter(pdf_file)
    # Convert the PDF file to a Word document
    cv.convert(word_file)
    # Close the Converter object
    cv.close()

def check_pdf_type(pdf_file):
    # Simple check based on filename (for example)
    # You can replace this with more sophisticated checks
    return 'sample_2' in pdf_file.lower()

# Fetch credentials and region
aws_access_key_id = os.getenv["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.getenv["AWS_SECRET_ACCESS_KEY"]
region_name = os.getenv["AWS_DEFAULT_REGION"]

# Initialize the S3 and Textract clients
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

textract = boto3.client(
    'textract',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# Streamlit interface
st.title("RapidDraw - Manufacturing Drawing Analysis")
uploaded_file = st.file_uploader("Upload a PDF file", type=["pdf"])

if uploaded_file is not None:
    pdf_type = check_pdf_type(uploaded_file.name)

    if pdf_type:
        # If PDF is of "sample_2" type, use CODE1 procedure
        word_file = uploaded_file.name.replace('.pdf', '.docx')
        with open("temp.pdf", "wb") as f:
            f.write(uploaded_file.read())
        pdf_to_word("temp.pdf", word_file)
        st.write(f"Converted '{uploaded_file.name}' to '{word_file}'.")

        # Provide a download button for the converted Word file
        with open(word_file, "rb") as f:
            st.download_button(
                label="Download Word File",
                data=f,
                file_name=word_file,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )

    else:
        # Otherwise, follow CODE2 procedure
        with open("temp.pdf", "wb") as f:
            f.write(uploaded_file.read())

        # Upload the file to S3
        bucket_name = 'for-textract-use-case'
        file_key = 'uploaded_file.pdf'
        s3.upload_file("temp.pdf", bucket_name, file_key)
        st.write(f"Uploaded '{uploaded_file.name}' to S3 bucket '{bucket_name}'.")

        # Start the asynchronous Textract job
        response = textract.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': file_key}},
            FeatureTypes=['TABLES', 'FORMS']
        )

        job_id = response['JobId']
        st.write(f"Job started with ID: {job_id}")

        # Poll for job completion
        def check_job_status(job_id):
            while True:
                response = textract.get_document_analysis(JobId=job_id)
                status = response['JobStatus']
                if status in ['SUCCEEDED', 'FAILED']:
                    return status, response
                st.write("Job is still in progress, waiting...")
                time.sleep(5)

        job_status, textract_response = check_job_status(job_id)

        if job_status == 'SUCCEEDED':
            st.write("Job completed successfully!")

            # Process the Textract response
            def process_textract_response(response):
                raw_text = []
                tables = []
                forms = []

                for block in response['Blocks']:
                    if block['BlockType'] == 'LINE':
                        raw_text.append(block['Text'])

                    elif block['BlockType'] == 'TABLE':
                        table = {}
                        for relationship in block.get('Relationships', []):
                            if relationship['Type'] == 'CHILD':
                                for child_id in relationship['Ids']:
                                    cell = next((b for b in response['Blocks'] if b['Id'] == child_id), None)
                                    if cell and cell['BlockType'] == 'CELL':
                                        row_index = cell['RowIndex']
                                        column_index = cell['ColumnIndex']
                                        text = ''
                                        if 'Relationships' in cell:
                                            for cell_relationship in cell['Relationships']:
                                                if cell_relationship['Type'] == 'CHILD':
                                                    for cell_child_id in cell_relationship['Ids']:
                                                        word = next((b for b in response['Blocks'] if b['Id'] == cell_child_id), None)
                                                        if word and word['BlockType'] == 'WORD' and 'Text' in word:
                                                            text += word['Text'] + ' '
                                        if row_index not in table:
                                            table[row_index] = {}
                                        table[row_index][column_index] = text.strip()
                        df_table = pd.DataFrame.from_dict(table, orient='index').sort_index(axis=1)
                        tables.append(df_table)

                    elif block['BlockType'] == 'KEY_VALUE_SET':
                        if 'KEY' in block['EntityTypes']:
                            key = ''
                            value = ''

                            # Extract key
                            if 'Relationships' in block:
                                for relationship in block['Relationships']:
                                    if relationship['Type'] == 'CHILD':
                                        for child_id in relationship['Ids']:
                                            word = next((b for b in response['Blocks'] if b['Id'] == child_id), None)
                                            if word and 'Text' in word:
                                                key += word['Text'] + ' '

                            # Extract value
                            if 'Relationships' in block:
                                for relationship in block['Relationships']:
                                    if relationship['Type'] == 'VALUE':
                                        value_block = next((b for b in response['Blocks'] if b['Id'] == relationship['Ids'][0]), None)
                                        if value_block and 'Relationships' in value_block:
                                            for value_relationship in value_block['Relationships']:
                                                if value_relationship['Type'] == 'CHILD':
                                                    for value_child_id in value_relationship['Ids']:
                                                        word = next((b for b in response['Blocks'] if b['Id'] == value_child_id), None)
                                                        if word and 'Text' in word:
                                                            value += word['Text'] + ' '

                            forms.append({
                                'Key': key.strip(),
                                'Value': value.strip(),
                            })

                return raw_text, forms, tables

            raw_text, forms, tables = process_textract_response(textract_response)

            st.write("Extracted Text:")
            st.write(raw_text)

            st.write("Extracted Forms (Key-Value Pairs):")
            df_forms = pd.DataFrame(forms)
            st.dataframe(df_forms)

            st.write("Extracted Tables:")
            for i, df_table in enumerate(tables):
                st.write(f"Table {i + 1}:")
                st.dataframe(df_table)

            # Save the data to Excel files
            forms_file_path = "forms_output.xlsx"
            tables_file_path = "tables_output.xlsx"

            df_forms.to_excel(forms_file_path, index=False)
            with pd.ExcelWriter(tables_file_path) as writer:
                for i, df_table in enumerate(tables):
                    df_table.to_excel(writer, sheet_name=f'Table_{i + 1}', index=False)

            # Allow the user to download the Excel files
            with open(forms_file_path, "rb") as f:
                st.download_button(
                    label="Download Forms Excel File",
                    data=f,
                    file_name="forms_output.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            with open(tables_file_path, "rb") as f:
                st.download_button(
                    label="Download Tables Excel File",
                    data=f,
                    file_name="tables_output.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        else:
            st.error("Job failed.")