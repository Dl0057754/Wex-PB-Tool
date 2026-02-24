import streamlit as st
import pandas as pd
import google.generativeai as genai
import json
import sqlite3
import io

# --- PAGE SETUP ---
st.set_page_config(page_title="HVAC to WEX FSM Converter", page_icon="⚙️", layout="wide")
st.title("⚙️ HVAC Pricebook to WEX FSM Pipeline")
st.markdown("Upload a chaotic distributor pricebook, and let AI structure it into Payzerware/WEX FSM templates.")

# --- API KEY HANDLING ---
# Securely gets the API key from the user or Streamlit Secrets
with st.sidebar:
    st.header("Settings")
    api_key = st.text_input("Gemini API Key", type="password", help="Enter your Google Gemini API Key.")
    if not api_key:
        st.warning("Please enter your API key to proceed.")
    else:
        st.success("API Key loaded!")
        genai.configure(api_key=api_key)

# --- CORE PIPELINE CLASS ---
class WEXFSMPipeline:
    def __init__(self):
        # Using Gemini 2.5 Pro for advanced reasoning and data extraction
        self.model = genai.GenerativeModel('gemini-2.5-pro')

    def find_header_row(self, df):
        keywords = ['Part', 'Model', 'Item', 'Description', 'Price', 'Bosch #', 'System Cost', 'Product']
        for i, row in df.iterrows():
            row_str = ' '.join(str(val).lower() for val in row.values)
            if any(kw.lower() in row_str for kw in keywords):
                return i
        return 0

    def ingest_uploaded_file(self, uploaded_file):
        """Reads directly from Streamlit's file uploader memory."""
        file_name = uploaded_file.name
        try:
            if file_name.endswith('.csv'):
                raw_df = pd.read_csv(uploaded_file, header=None)
                uploaded_file.seek(0) # Reset pointer
                header_idx = self.find_header_row(raw_df)
                df = pd.read_csv(uploaded_file, skiprows=header_idx)
            else:
                raw_df = pd.read_excel(uploaded_file, header=None)
                uploaded_file.seek(0)
                header_idx = self.find_header_row(raw_df)
                df = pd.read_excel(uploaded_file, skiprows=header_idx)
                
            df = df.dropna(how='all')
            return df.to_dict('records')
        except Exception as e:
            st.error(f"Failed to read file: {e}")
            return []

    def extract_and_enrich_with_ai(self, raw_text, file_context, template_choice):
        few_shot_examples = """
        EXAMPLE 1 (Compressor Input): "ZP25LXE-PFV-800 | COPELAND SCROLL COMPRESSOR | $1075.52"
        EXPECTED OUTPUT 1:
        {
            "Manufacturer": "Copeland",
            "Model_Number": "ZP25LXE-PFV-800",
            "Part_Number": "ZP25LXE-PFV-800",
            "Cost": 1075.52,
            "Folder_1": "Compressors",
            "Folder_2": "COMPRESSOR COOL DOWN",
            "Folder_3": "",
            "Standard_Name": "COMPRESSOR - COPELAND ZP25LXE-PFV-800",
            "Description": "Copeland Scroll Compressor, ZP25LXE-PFV-800",
            "Labor_Hours": 6.0,
            "Confidence_Score": 95
        }
        """

        prompt = f"""
        You are an expert HVAC data enrichment engine mapping distributor data into Payzerware/WEX FSM.
        Context / Source File Name: {file_context}
        Target Template: {template_choice}
        
        {few_shot_examples}
        
        Analyze the following raw row of data from a distributor pricebook.
        Extract the exact Manufacturer, Model Number, Part Number, and Cost/Price.
        
        ENRICHMENT RULES:
        1. Emulate the Folder categorization and Standard Naming seen in the Examples above.
        2. Give a Confidence_Score (0-100). Subtract points if guessing.
        3. Estimate Labor Hours needed to install this item.
        
        Return ONLY valid JSON matching the structure of the examples.
        Raw Row Data: {raw_text}
        """
        
        try:
            response = self.model.generate_content(prompt)
            clean_json = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_json)
        except Exception:
            return {"Confidence_Score": 0}

    def _format_bundle(self, df, supplier_name, labor_rate, labor_cost):
        return pd.DataFrame({
            'Folder 1\n*Required': df.get('Folder_1', ''),
            'Folder 2': df.get('Folder_2', ''),
            'Product Name\n*Required': df.get('Standard_Name', ''),
            'Is the product Taxable? (Y/N)\n*Required': 'Y',
            'Model Number': df.get('Model_Number', ''),
            'Description': df.get('Description', ''),
            'Standard Price \n*Required': (pd.to_numeric(df.get('Cost', 0)) * 1.5) + (pd.to_numeric(df.get('Labor_Hours', 2)) * labor_rate),
            'Labor Hours': df.get('Labor_Hours', 2),
            'Labor Rate': labor_rate, 
            'Labor Cost': pd.to_numeric(df.get('Labor_Hours', 2)) * labor_cost, 
            'Labor Name\n*Required': 'Single Part Labor',
            'Is the labor Taxable? (Y/N)\n*Required': 'N',
            'Part Name\n*Required': df.get('Standard_Name', ''),
            'Part Income Account': 'Sales',
            'Is the part Taxable? (Y/N)\n*Required': 'Y',
            'Part Cost': df.get('Cost', 0),
            'Standard Price': pd.to_numeric(df.get('Cost', 0)) * 1.5,
            'Part Model Number': df.get('Model_Number', ''),
            'Serialized? (Y/N)\n*Required': 'N',
            'Part Supplier': supplier_name
        })

# --- USER INTERFACE ---
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("1. Setup")
    supplier_name = st.text_input("Supplier Name", "Glacier Supply")
    template_choice = st.selectbox("Output Template", ["Part + Labor Bundle", "Single Part", "Supplier Loader"])
    
    st.subheader("2. Labor Settings (If Bundle)")
    labor_rate = st.number_input("Customer Labor Rate ($/hr)", value=141.43)
    labor_cost = st.number_input("Internal Tech Cost ($/hr)", value=54.40)

with col2:
    st.subheader("3. Upload")
    uploaded_file = st.file_uploader("Upload Contractor Pricebook (Excel/CSV)", type=['xlsx', 'csv'])

if st.button("🚀 Process Pricebook", type="primary"):
    if not api_key:
        st.error("Please enter your Gemini API Key in the sidebar.")
    elif uploaded_file is None:
        st.error("Please upload a file first.")
    else:
        pipeline = WEXFSMPipeline()
        raw_items = pipeline.ingest_uploaded_file(uploaded_file)
        
        if not raw_items:
            st.error("Could not read data from the file.")
        else:
            # Progress Bar Setup
            progress_bar = st.progress(0)
            status_text = st.empty()
            processed_data = []
            
            # Processing Loop
            total_items = len(raw_items)
            
            # For testing/demo, you might want to slice this to raw_items[:10] to avoid long waits
            for i, item in enumerate(raw_items): 
                raw_string = " | ".join([f"{str(k).strip()}: {str(v).strip()}" for k, v in item.items() if pd.notna(v) and str(v).strip() != ""])
                if raw_string:
                    status_text.text(f"Processing item {i+1} of {total_items}...")
                    ai_data = pipeline.extract_and_enrich_with_ai(raw_string, uploaded_file.name, template_choice)
                    ai_data["Raw_Input"] = raw_string
                    processed_data.append(ai_data)
                
                # Update progress
                progress_bar.progress((i + 1) / total_items)
            
            status_text.text("Formatting data...")
            df = pd.DataFrame(processed_data)
            
            # Split by Confidence
            high_confidence = df[df['Confidence_Score'] >= 85].copy()
            needs_review = df[df['Confidence_Score'] < 85].copy()
            
            # Format high confidence
            final_df = pipeline._format_bundle(high_confidence, supplier_name, labor_rate, labor_cost)
            
            st.success(f"✅ Processing Complete! {len(final_df)} items ready for WEX, {len(needs_review)} items need human review.")
            
            # --- PREPARE DOWNLOADS ---
            st.subheader("📥 Download Results")
            dl_col1, dl_col2 = st.columns(2)
            
            with dl_col1:
                # Convert final_df to CSV bytes
                csv_data = final_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download WEX Upload File (CSV)",
                    data=csv_data,
                    file_name=f"WEX_UPLOAD_{uploaded_file.name}.csv",
                    mime="text/csv",
                    icon="⬇️"
                )
                
            with dl_col2:
                # Convert needs_review to Excel bytes
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    needs_review.to_excel(writer, index=False, sheet_name='Needs Review')
                excel_data = output.getvalue()
                
                st.download_button(
                    label="Download Needs Review (Excel)",
                    data=excel_data,
                    file_name=f"NEEDS_REVIEW_{uploaded_file.name}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    icon="⚠️"
                )
