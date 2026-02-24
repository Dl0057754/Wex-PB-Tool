import streamlit as st
import pandas as pd
import json
import re
import io

# --- PAGE SETUP ---
st.set_page_config(page_title="HVAC to WEX FSM Converter", page_icon="⚙️", layout="wide")
st.title("⚙️ HVAC Pricebook to WEX FSM Pipeline")
st.markdown("Upload a chaotic distributor pricebook, and structure it into Payzerware/WEX FSM templates using rule-based extraction.")

# --- CORE PIPELINE CLASS ---
class WEXFSMPipeline:
    def __init__(self):
        # Rule-based extraction - no API key needed!
        self.hvac_categories = {
            'compressor': ['compressor', 'scroll', 'reciprocating', 'copeland', 'tecumseh'],
            'refrigerant': ['refrigerant', 'r410', 'r22', 'r407', 'coolant'],
            'motor': ['motor', 'blower', 'fan motor', 'condenser fan'],
            'capacitor': ['capacitor', 'run cap', 'start cap', 'mfd'],
            'valve': ['valve', 'expansion valve', 'check valve', 'solenoid'],
            'coil': ['coil', 'evaporator', 'condenser coil'],
            'thermostat': ['thermostat', 'programmable', 'digital'],
            'filter': ['filter', 'air filter', 'furnace filter'],
            'ductwork': ['duct', 'ductboard', 'insulation'],
        }
        
        self.labor_hours_map = {
            'compressor': 8.0,
            'refrigerant': 3.0,
            'motor': 2.0,
            'capacitor': 1.5,
            'valve': 2.0,
            'coil': 6.0,
            'thermostat': 1.0,
            'filter': 0.5,
            'ductwork': 4.0,
        }

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
                uploaded_file.seek(0)
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

    def categorize_part(self, text):
        """Categorize part based on keywords."""
        text_lower = text.lower()
        for category, keywords in self.hvac_categories.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return category
        return 'other'

    def extract_price(self, row_dict):
        """Extract price from row."""
        for value in row_dict.values():
            if isinstance(value, str):
                price_match = re.search(r'\$?([\d,]+\.\d{0,2})', str(value))
                if price_match:
                    try:
                        return float(price_match.group(1).replace(',', ''))
                    except:
                        pass
        return 0.0

    def rule_based_extraction(self, row_dict):
        """Extract data using rules instead of AI."""
        text_repr = " | ".join([f"{str(k)}: {str(v)}" for k, v in row_dict.items() if pd.notna(v) and str(v).strip() != ""])        
        
        category = self.categorize_part(text_repr)
        cost = self.extract_price(row_dict)
        
        # Extract model number
        model_number = ""
        for key, value in row_dict.items():
            if 'model' in str(key).lower() or 'part' in str(key).lower() or 'item' in str(key).lower():
                model_number = str(value)
                break
        
        data = {
            "Manufacturer": "Unknown",
            "Model_Number": model_number,
            "Part_Number": model_number,
            "Cost": cost,
            "Folder_1": "HVAC Components",
            "Folder_2": category.title(),
            "Folder_3": "",
            "Standard_Name": f"{category.upper()} - {model_number}" if model_number else category.upper(),
            "Description": text_repr[:100],
            "Labor_Hours": self.labor_hours_map.get(category, 2.0),
            "Confidence_Score": 85 if (model_number and cost > 0) else 70
        }
        
        return data

    def format_for_template(self, row_dict, template_choice, supplier_name, labor_rate, labor_cost):
        """Format row based on template choice."""
        if template_choice == "Part + Labor Bundle":
            return {
                'Folder 1\n*Required': row_dict.get('Folder_1', ''),
                'Folder 2': row_dict.get('Folder_2', ''),
                'Product Name\n*Required': row_dict.get('Standard_Name', ''),
                'Is the product Taxable? (Y/N)\n*Required': 'Y',
                'Model Number': row_dict.get('Model_Number', ''),
                'Description': row_dict.get('Description', ''),
                'Standard Price \n*Required': (float(row_dict.get('Cost', 0)) * 1.5) + (float(row_dict.get('Labor_Hours', 2)) * labor_rate),
                'Labor Hours': row_dict.get('Labor_Hours', 2),
                'Labor Rate': labor_rate, 
                'Labor Cost': float(row_dict.get('Labor_Hours', 2)) * labor_cost, 
                'Labor Name\n*Required': 'Single Part Labor',
                'Is the labor Taxable? (Y/N)\n*Required': 'N',
                'Part Name\n*Required': row_dict.get('Standard_Name', ''),
                'Part Income Account': 'Sales',
                'Is the part Taxable? (Y/N)\n*Required': 'Y',
                'Part Cost': row_dict.get('Cost', 0),
                'Standard Price': float(row_dict.get('Cost', 0)) * 1.5,
                'Part Model Number': row_dict.get('Model_Number', ''),
                'Serialized? (Y/N)\n*Required': 'N',
                'Part Supplier': supplier_name
            }
        elif template_choice == "Single Part":
            return {
                'Folder 1\n*Required': row_dict.get('Folder_1', ''),
                'Folder 2': row_dict.get('Folder_2', ''),
                'Product Name\n*Required': row_dict.get('Standard_Name', ''),
                'Is the product Taxable? (Y/N)\n*Required': 'Y',
                'Model Number': row_dict.get('Model_Number', ''),
                'Description': row_dict.get('Description', ''),
                'Standard Price \n*Required': float(row_dict.get('Cost', 0)) * 1.5,
                'Part Name\n*Required': row_dict.get('Standard_Name', ''),
                'Part Cost': row_dict.get('Cost', 0),
                'Part Model Number': row_dict.get('Model_Number', ''),
                'Serialized? (Y/N)\n*Required': 'N',
                'Part Supplier': supplier_name
            }
        elif template_choice == "Supplier Loader":
            return {
                'Supplier Name': supplier_name,
                'Part Number': row_dict.get('Model_Number', ''),
                'Description': row_dict.get('Description', ''),
                'Cost': row_dict.get('Cost', 0),
                'Category': row_dict.get('Folder_2', ''),
            }

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
    if uploaded_file is None:
        st.error("Please upload a file first.")
    else:
        try:
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
                
                for i, item in enumerate(raw_items): 
                    status_text.text(f"Processing item {i+1} of {total_items}...")
                    rule_data = pipeline.rule_based_extraction(item)
                    processed_data.append(rule_data)
                    progress_bar.progress((i + 1) / total_items)
                
                status_text.text("Formatting data...")
                df_processed = pd.DataFrame(processed_data)
                
                # Split by Confidence
                high_confidence = df_processed[df_processed['Confidence_Score'] >= 70].copy()
                needs_review = df_processed[df_processed['Confidence_Score'] < 70].copy()
                
                # Format based on template choice
                formatted_rows = []
                for _, row in high_confidence.iterrows():
                    formatted_row = pipeline.format_for_template(row, template_choice, supplier_name, labor_rate, labor_cost)
                    formatted_rows.append(formatted_row)
                
                final_df = pd.DataFrame(formatted_rows)
                
                st.success(f"✅ Processing Complete! {len(final_df)} items ready for WEX, {len(needs_review)} items need human review.")
                
                # --- PREPARE DOWNLOADS ---
                st.subheader("📥 Download Results")
                dl_col1, dl_col2 = st.columns(2)
                
                with dl_col1:
                    csv_data = final_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download WEX Upload File (CSV)",
                        data=csv_data,
                        file_name=f"WEX_UPLOAD_{uploaded_file.name}.csv",
                        mime="text/csv",
                        icon="⬇️"
                    )
                    
                with dl_col2:
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
        except Exception as e:
            st.error(f"Error processing file: {e}")