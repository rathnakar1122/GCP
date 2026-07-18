import streamlit as st
from Ollama.Resume_analysis.pdf_reader import extract_text
from Ollama.Resume_analysis.analyzer import analyze_resume

# Page Configuration
st.set_page_config(
    page_title="Rathnakar Resume Analyzer App",
    page_icon="📄",
    layout="wide"
)

# Title
st.title("Rathnakar Resume Analyzer App")

st.write(
    "Upload a resume in PDF format to analyze its content, formatting, "
    "skills, strengths, weaknesses, and receive AI insights."
)

# File Uploader
uploaded_file = st.file_uploader(
    "Upload Resume",
    type=["pdf"]
)

# Process Uploaded Resume
if uploaded_file is not None:
    with st.spinner("Analyzing Resume..."):
        resume_text = extract_text(uploaded_file)

        if resume_text.strip():
            analysis_result = analyze_resume(resume_text)

            st.subheader("Analysis Result")
            st.write(analysis_result)
        else:
            st.error("Could not extract text from the PDF.")