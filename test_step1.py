
import streamlit as st
import pandas as pd

st.title("Step 1: Load Raw Data")
st.markdown("Let's just load one raw file and see what it looks like")

uploaded_file = st.file_uploader("Upload ONE raw DSR file (.xlsx)", type=['xlsx'])

if uploaded_file:
    st.success(f"File uploaded: {uploaded_file.name}")
    
    # Read the file
    df = pd.read_excel(uploaded_file)
    
    st.subheader("Raw Data Preview")
    st.write(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    
    st.subheader("Column Names")
    st.write(list(df.columns))
    
    st.subheader("First 10 Rows")
    st.dataframe(df.head(10))
    
    st.subheader("Data Info")
    st.write("Non-null counts per column:")
    st.write(df.count())