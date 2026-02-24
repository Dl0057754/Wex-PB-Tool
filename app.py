import streamlit as st
import pandas as pd
import re
import io
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import time

# --- PAGE SETUP ---
st.set_page_config(page_title="HVAC to WEX FSM Converter", page_icon="⚙️", layout="wide")
st.title("⚙️ HVAC Pricebook to WEX FSM Pipeline")
st.markdown("Upload a distributor pricebook. Extract part numbers and pull OEM data from brand websites.")

# --- OEM BRAND DATABASE (12 BRANDS) ---
OEM_BRANDS = {
    'Carrier': {'search_url': 'https://www.carrier.com', 'keywords': ['carrier', 'hvac', 'compressor']},
    'Copeland': {'search_url': 'https://www.emerson.com', 'keywords': ['copeland', 'scroll', 'compressor']},
    'Trane': {'search_url': 'https://www.trane.com', 'keywords': ['trane', 'hvac', 'unit']},
    'Lennox': {'search_url': 'https://www.lennox.com', 'keywords': ['lennox', 'hvac']},
    'York': {'search_url': 'https://www.yorkclimaticsystems.com', 'keywords': ['york', 'hvac']},
    'Goodman': {'search_url': 'https://www.goodmanmfg.com', 'keywords': ['goodman', 'hvac']},
    'Bosch': {'search_url': 'https://www.bosch-home.com', 'keywords': ['bosch', 'hvac']},
    'Daikin': {'search_url': 'https://www.daikin.com', 'keywords': ['daikin', 'compressor']},
    'Mitsubishi': {'search_url': 'https://www.mitsubishicomfort.com', 'keywords': ['mitsubishi', 'heat pump']},
    'LG': {'search_url': 'https://www.lg.com', 'keywords': ['lg', 'hvac', 'air']},
    'Rheem': {'search_url': 'https://www.rheem.com', 'keywords': ['rheem', 'hvac']},
    'Ruud': {'search_url': 'https://www.ruud.com', 'keywords': ['ruud', 'hvac']},
}

DISTRIBUTORS = {
    'Carrier': 'carrier.com',
    'Copeland (Emerson)': 'emerson.com',
    'Trane': 'trane.com',
    'Lennox': 'lennox.com',
    'York': 'yorkclimaticsystems.com',
    'Goodman': 'goodmanmfg.com',
    'Bosch': 'bosch-home.com',
    'Daikin': 'daikin.com',
    'Mitsubishi': 'mitsubishicomfort.com',
    'LG': 'lg.com',
    'Rheem': 'rheem.com',
    'Ruud': 'ruud.com',
    'Google (All Distributors)': 'google.com',
}

# --- CORE PIPELINE CLASS ---
class WEXFSMPipeline:
    def __init__(self):
        self.hvac_categories = {
            'compressor': ['compressor', 'scroll', 'reciprocating', 'copeland', 'tecumseh', 'piston'],
            'refrigerant': ['refrigerant', 'r410', 'r22', 'r407', 'coolant', 'charge'],
            'motor': ['motor', 'blower', 'fan motor', 'condenser fan', 'indoor fan'],
            'capacitor': ['capacitor', 'run cap', 'start cap', 'mfd', 'microfarad'],
            'valve': ['valve', 'expansion valve', 'check valve', 'solenoid', 'txv'],
            'coil': ['coil', 'evaporator', 'condenser coil', 'heat exchanger'],
            'thermostat': ['thermostat', 'programmable', 'digital', 'smart', 'control'],
            'filter': ['filter', 'air filter', 'furnace filter', 'media', 'merv'],
            'ductwork': ['duct', 'ductboard', 'insulation', 'ductless'],
            'relay': ['relay', 'defrost', 'contactor', 'switch'],
            'transformer': ['transformer', 'low voltage', '24v'],
            'compressor_oil': ['oil', 'lubricant', 'pag', 'mineral oil'],
        }
        
        self.labor_hours_map = {
            'compressor': 8.0, 'refrigerant': 3.0, 'motor': 2.0, 'capacitor': 1.5,
            'valve': 2.0, 'coil': 6.0, 'thermostat': 1.0, 'filter': 0.5, 'ductwork': 4.0,
            'relay': 1.0, 'transformer': 1.5, 'compressor_oil': 2.0,
        }

    def find_header_row(self, df):
        keywords = ['Part', 'Model', 'Item', 'Description', 'Price', 'Bosch #', 'System Cost', 'Product', '#']
        for i, row in df.iterrows():
            row_str = ' '.join(str(val).lower() for val in row.values)
            if any(kw.lower() in row_str for kw in keywords):
                return i
        return 0

    def ingest_uploaded_file(self, uploaded_file):
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

    def extract_part_number(self, row_dict):
        """Extract part number - PRIORITY: Model/Part columns."""
        for key, value in row_dict.items():
            key_lower = str(key).lower()
            if 'model' in key_lower or 'part' in key_lower or 'item' in key_lower or '#' in key_lower:
                part_num = str(value).strip()
                if part_num and len(part_num) > 2 and part_num != 'nan':
                    return part_num
        
        for value in row_dict.values():
            val_str = str(value).strip()
            if val_str and len(val_str) >= 4 and len(val_str) <= 20 and not val_str.startswith('$'):
                if re.match(r'^[A-Z0-9\-]+$', val_str):
                    return val_str
        return ""

    def extract_price(self, row_dict):
        """Extract price from row."""
        for value in row_dict.values():
            if isinstance(value, (int, float)) and value > 0:
                return float(value)
            if isinstance(value, str):
                price_match = re.search(r'\$?([\d,]+\.?\d{0,2})', str(value))
                if price_match:
                    try:
                        return float(price_match.group(1).replace(',', ''))
                    except:
                        pass
        return 0.0

    def web_search_part(self, part_number, brand, distributor_domain):
        """Search web for part info using Google + site-specific search."""
        if not part_number:
            return "", 0
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            if distributor_domain != 'google.com':
                search_query = f"{part_number} site:{distributor_domain}"
            else:
                search_query = f"{brand} {part_number} hvac specifications"
            
            url = f"https://www.google.com/search?q={quote(search_query)}"
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract from search results
                snippets = []
                for g in soup.find_all('div', class_='VwiC3b'):
                    snippet = g.get_text()
                    if snippet:
                        snippets.append(snippet)
                
                if snippets:
                    description = snippets[0][:200]
                    return description, 1
                
                # Fallback to page text
                text_content = soup.get_text()[:300]
                if part_number.lower() in text_content.lower():
                    return text_content, 0.8
            
            return "", 0.5
        except:
            return "", 0

    def categorize_part(self, text):
        """Categorize part based on keywords."""
        text_lower = text.lower()
        for category, keywords in self.hvac_categories.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return category
        return 'other'

    def extract_with_oem_lookup(self, row_dict, brand, distributor_domain):
        """Extract data with OEM web search."""
        part_number = self.extract_part_number(row_dict)
        cost = self.extract_price(row_dict)
        text_repr = " | ".join([f"{str(k)}: {str(v)}" for k, v in row_dict.items() if pd.notna(v)])
        
        oem_description = ""
        oem_confidence = 0
        if part_number:
            oem_description, oem_confidence = self.web_search_part(part_number, brand, distributor_domain)
            time.sleep(0.5)
        
        description = oem_description if oem_description else text_repr[:150]
        category = self.categorize_part(description + " " + part_number)
        
        base_confidence = 60
        if part_number:
            base_confidence += 15
        if cost > 0:
            base_confidence += 10
        if oem_description:
            base_confidence += 15
        
        confidence = min(100, base_confidence)
        
        data = {
            "Manufacturer": brand,
            "Model_Number": part_number,
            "Part_Number": part_number,
            "Cost": cost,
            "Folder_1": "HVAC Components",
            "Folder_2": category.title(),
            "Standard_Name": f"{brand} {part_number}" if part_number else brand,
            "Description": description,
            "Labor_Hours": self.labor_hours_map.get(category, 2.0),
            "Confidence_Score": confidence,
            "OEM_Search_Status": "Found" if oem_description else "Not Found"
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
                'Manufacturer': row_dict.get('Manufacturer', ''),
            }

# --- USER INTERFACE ---
st.sidebar.header("🔧 Configuration")
brand = st.sidebar.selectbox("Select Brand", list(OEM_BRANDS.keys()))
distributor = st.sidebar.selectbox("Select Distributor", list(DISTRIBUTORS.keys()))

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("1. Setup")
    supplier_name = st.text_input("Supplier Name", "Glacier Supply")
    template_choice = st.selectbox("Output Template", ["Part + Labor Bundle", "Single Part", "Supplier Loader"])
    
    st.subheader("2. Labor Settings")
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
                progress_bar = st.progress(0)
                status_text = st.empty()
                processed_data = []
                total_items = len(raw_items)
                
                for i, item in enumerate(raw_items):
                    status_text.text(f"Processing item {i+1} of {total_items}... (Searching OEM: {brand})")
                    extracted = pipeline.extract_with_oem_lookup(item, brand, DISTRIBUTORS[distributor])
                    processed_data.append(extracted)
                    progress_bar.progress((i + 1) / total_items)
                
                status_text.text("Formatting data...")
                df_processed = pd.DataFrame(processed_data)
                
                high_confidence = df_processed[df_processed['Confidence_Score'] >= 70].copy()
                needs_review = df_processed[df_processed['Confidence_Score'] < 70].copy()
                
                formatted_rows = []
                for _, row in high_confidence.iterrows():
                    formatted_row = pipeline.format_for_template(row, template_choice, supplier_name, labor_rate, labor_cost)
                    formatted_rows.append(formatted_row)
                
                final_df = pd.DataFrame(formatted_rows)
                
                st.success(f"✅ Processing Complete! {len(final_df)} items ready for WEX, {len(needs_review)} items need review.")
                
                # Show summary
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("High Confidence", len(high_confidence))
                with col_b:
                    st.metric("Needs Review", len(needs_review))
                with col_c:
                    found_oem = len(df_processed[df_processed['OEM_Search_Status'] == 'Found'])
                    st.metric("OEM Data Found", found_oem)
                
                st.subheader("📥 Download Results")
                dl_col1, dl_col2, dl_col3 = st.columns(3)
                
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
                
                with dl_col3:
                    full_data = df_processed.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Full Data (with scores)",
                        data=full_data,
                        file_name=f"FULL_DATA_{uploaded_file.name}.csv",
                        mime="text/csv",
                        icon="📊"
                    )
        except Exception as e:
            st.error(f"Error processing file: {e}")
            import traceback
            st.error(traceback.format_exc())
