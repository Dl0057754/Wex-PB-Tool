import streamlit as st

# Access Streamlit secrets
API_KEY = st.secrets['api_key']

# Template formatting methods

def part_labor_bundle(data):
    # Code to format for Part + Labor Bundle
    pass


def single_part(data):
    # Code to format for Single Part
    pass


def supplier_loader(data):
    # Code to format for Supplier Loader
    pass

# Main app logic
selected_template = st.selectbox('Choose a template:', ['Part + Labor Bundle', 'Single Part', 'Supplier Loader'])

if selected_template == 'Part + Labor Bundle':
    part_labor_bundle(data)
elif selected_template == 'Single Part':
    single_part(data)
elif selected_template == 'Supplier Loader':
    supplier_loader(data)