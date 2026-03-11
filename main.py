import streamlit as st
import pandas as pd
from io import BytesIO
import sys
from pathlib import Path

# Add src to path

from src.categorizer import Categorizer
from src.splitter import Splitter
from src.process_dataframe import process_dataframe

# Page config
st.set_page_config(
    page_title="Data Processing Tool",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for equal-width tabs
st.markdown("""
<style>
    /* Make tabs equal width */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0px;
        width: 100%;
    }
    
    .stTabs [data-baseweb="tab"] {
        flex: 1;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
        justify-content: center;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: rgba(255, 75, 75, 0.1);
        border-bottom: 2px solid #ff4b4b;
    }
    
    /* Hover effect */
    .stTabs [data-baseweb="tab"]:hover {
        background-color: rgba(128, 128, 128, 0.1);
    }
    
    .stTabs [aria-selected="true"]:hover {
        background-color: rgba(255, 75, 75, 0.15);
    }
</style>
""", unsafe_allow_html=True)

st.title("📊 Data Processing Tool")

# Initialize categorizer and splitter
@st.cache_resource
def load_categorizer():
    return Categorizer(rules_file='rules/categorize_rules.json')

@st.cache_resource
def load_splitter():
    return Splitter()

def sanitize_excel_values(df: pd.DataFrame):
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: f"'{x}" if isinstance(x, str) and x.strip().startswith('=') else x
        )
    return df

categorizer = load_categorizer()
splitter = load_splitter()

# Helper functions for Tab 2
def render_file_uploaders(config):
    """Render file uploaders based on configuration"""
    files = {}
    if config['type'] == 'single':
        files['file'] = st.file_uploader("Upload file Excel", type=['xlsx'], key=config['key'])
    elif config['type'] == 'raw_interaction':
        col1, col2 = st.columns(2)
        with col1:
            files['raw_file'] = st.file_uploader("File Raw", type=['xlsx'], key=f"{config['key']}_raw")
        with col2:
            files['interaction_file'] = st.file_uploader("File Interactions", type=['xlsx'], key=f"{config['key']}_interaction")
    elif config['type'] == 'raw_demo':
        col1, col2 = st.columns(2)
        with col1:
            files['raw_file'] = st.file_uploader("File Raw", type=['xlsx'], key=f"{config['key']}_raw")
        with col2:
            files['demographic_file'] = st.file_uploader("File Demographic", type=['xlsx'], key=f"{config['key']}_demo")
    elif config['type'] == 'hospital':
        files['files'] = st.file_uploader(
            "Upload file(s) Raw Excel", 
            type=['xlsx'], 
            accept_multiple_files=True,
            key=f"{config['key']}_files"
        )
        col1, col2 = st.columns(2)
        with col1:
            files['hp_interaction'] = st.file_uploader(
                "File Interaction Hanh Phuc Hospital", 
                type=['xlsx'], 
                key='hp_interaction'
            )
        with col2:
            files['hm_interaction'] = st.file_uploader(
                "File Interaction Hoan My Hospital", 
                type=['xlsx'], 
                key='hm_interaction'
            )
    return files

def render_process_section(project_name, config, files):
    """Render process button and download section"""
    output_filename = st.text_input(
        "Tên file đầu ra (không cần .xlsx)", 
        value=config['default_filename'], 
        key=f"{config['key']}_filename"
    )
    
    # Check if all required files are uploaded
    all_files_uploaded = all(v is not None for v in files.values() if not isinstance(v, list)) and \
                         all(len(v) > 0 if isinstance(v, list) else True for v in files.values())
    
    
    if all_files_uploaded:
        if st.button(config.get('button_label', '🚀 Tách Sheet'), type="primary", use_container_width=True, key=f"{config['key']}_process"):
            with st.spinner(config.get('spinner_text', 'Đang xử lý...')):
                result = splitter.split(project_name, output_filename=output_filename, **files)
                
                if result['success']:
                    st.session_state[f"{config['key']}_result"] = result
                    st.rerun()
                else:
                    st.error(result['message'])
        
        # Display download button(s)
        if f"{config['key']}_result" in st.session_state:
            result = st.session_state[f"{config['key']}_result"]
            st.success(result['message'])
            
            # Special handling for HDBank (2 files)
            if len(result['files']) > 1:
                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        label="📥 Download HDBank",
                        data=result['files'][0],
                        file_name=result['filenames'][0],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key=f"{config['key']}_download1"
                    )
                with col2:
                    st.download_button(
                        label="📥 Download Đối thủ",
                        data=result['files'][1],
                        file_name=result['filenames'][1],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key=f"{config['key']}_download2"
                    )
            else:
                st.download_button(
                    label="📥 Download",
                    data=result['files'][0],
                    file_name=result['filenames'][0],
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    key=f"{config['key']}_download"
                )
    elif config['type'] == 'hospital' and files.get('files'):
        st.info("👆 Vui lòng upload cả 2 file Interaction để tiếp tục")

# Project configurations
PROJECT_CONFIGS = {
    "Be App": {
        'key': 'be',
        'type': 'raw_interaction',
        'default_filename': 'Kompa x Be App',
        'button_label': '🚀 Xử lý',
        'subtitle': '📋 Be App - Join Raw & Interactions'
    },
    "Giao Hàng Nhanh": {
        'key': 'ghn',
        'type': 'raw_interaction',
        'default_filename': 'Kompa x Giao Hàng Nhanh',
        'subtitle': '📦 Giao Hàng Nhanh - Tách theo Labels1'
    },
    "Hafele": {
        'key': 'hafele',
        'type': 'single',
        'default_filename': 'Kompa x Hafele',
        'subtitle': '🔧 Hafele - Tách theo Categories'
    },
    "Hanh Phuc Hospital/Hoan My Hospital": {
        'key': 'hospital',
        'type': 'hospital',
        'default_filename': 'Kompa x Hanh Phuc Hospital/Hoan My Hospital',
        'button_label': '🚀 Gộp và Tách Sheet',
        'spinner_text': 'Đang gộp và xử lý...',
        'subtitle': '🏥 Hanh Phuc Hospital/Hoan My Hospital - Tách theo Sentiment'
    },
    "ShopeeFood": {
        'key': 'sf',
        'type': 'raw_demo',
        'default_filename': 'Kompa x ShopeeFood',
        'button_label': '🚀 Xử lý',
        'subtitle': '🍔 ShopeeFood - Merge Data & Demographic'
    },
    "Tân Hiệp Phát": {
        'key': 'thp',
        'type': 'single',
        'default_filename': 'Kompa x Tân Hiệp Phát',
        'subtitle': '🥤 Tân Hiệp Phát - Tách theo Label1'
    },
    "HDBank": {
        'key': 'hdb',
        'type': 'single',
        'default_filename': 'Kompa x HDBank',
        'subtitle': '🏦 HDBank - Tách HDBank & Đối thủ'
    },
    "PNJ": {
        'key': 'pnj',
        'type': 'raw_demo',
        'default_filename': 'Kompa x PNJ',
        'subtitle': '💎 PNJ - Tách theo Keywords'
    },
    "HomeCredit": {
        'key': 'home',
        'type': 'raw_interaction',
        'default_filename': 'Kompa x HomeCredit',
        'subtitle': '💳 HomeCredit - Tách theo Topic'
    },
    "HomeCredit (Cyber Fraud)": {
        'key': 'home_fraud',
        'type': 'raw_interaction',
        'default_filename': 'Kompa_Daily_Cyber_Fraud',
        'subtitle': '🔒 HomeCredit (Cyber Fraud) - Chỉnh file theo template'
    }
}

categorizer = load_categorizer()
splitter = load_splitter()

# Create tabs
tab1, tab2 = st.tabs(["🏷️ Chia Cate", "📑 Tách Sheet"])

with tab1:
    st.header("Phân loại Category")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # File upload
        uploaded_file = st.file_uploader(
            "Upload file Excel (.xlsx)", 
            type=['xlsx'],
            key='cate_upload'
        )
    
    with col2:
        # Project selection
        projects = categorizer.get_projects()
        selected_project = st.selectbox(
            "Chọn Project",
            options=projects,
            key='project_select'
        )
        
        # Show project info
        if selected_project:
            rules_count = len(categorizer.get_project_rules(selected_project))
            st.info(f"📋 {rules_count} rules")
    
    if uploaded_file is not None:
        try:
            # Read Excel file
            df_original = pd.read_excel(uploaded_file)
            
            # st.subheader("📄 Preview Data Gốc")
            # st.dataframe(df_original.head(10), width="stretch")
            
            # Process button
            if st.button("🚀 Phân loại", type="primary", width="stretch"):
                with st.spinner('Đang phân loại...'):
                    # Categorize
                    df = process_dataframe(df_original)
                    result_df = categorizer.categorize_dataframe(
                        df,
                        topic_col='Topic',
                        content_col='Text',
                        output_col='Category',
                        project_filter=selected_project
                    )
                    result_df = sanitize_excel_values(result_df)
                    # Show results
                    st.subheader("✅ Kết quả")
                    
                    # Statistics
                    total = len(result_df)
                    categorized = result_df['category'].notna().sum()
                    uncategorized = total - categorized
                    
                    metric_col1, metric_col2, metric_col3 = st.columns(3)
                    with metric_col1:
                        st.metric("Tổng số dòng", total)
                    with metric_col2:
                        st.metric("Đã phân loại", categorized, 
                                 delta=f"{categorized/total*100:.1f}%")
                    with metric_col3:
                        st.metric("Chưa phân loại", uncategorized,
                                 delta=f"{uncategorized/total*100:.1f}%",
                                 delta_color="inverse")
                    
                    # Category distribution
                    if categorized > 0:
                        st.subheader("📊 Phân bố Category")
                        category_counts = result_df['category'].value_counts()
                        st.bar_chart(category_counts)
                    
                    # # Preview result
                    # st.subheader("👀 Preview Kết quả")
                    # st.dataframe(result_df.head(20), width="stretch")
                    
                    # Download button
                    st.subheader("💾 Tải xuống")
                    
                    # Convert to Excel
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        result_df.to_excel(writer, index=False, sheet_name='Result')
                    output.seek(0)
                    
                    st.download_button(
                        label="📥 Download Excel",
                        data=output,
                        file_name=f"categorized_{selected_project}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width="stretch"
                    )
        
        except Exception as e:
            st.error(f"❌ Lỗi: {str(e)}")
            st.exception(e)
            st.exception(e)
    
    else:
        st.info("👆 Vui lòng upload file Excel để bắt đầu")

# ============================================================================
# TAB 2: TÁCH SHEET
# ============================================================================
with tab2:
    st.header("Tách Sheet")
    
    # Nút tạo mới - hiển thị nếu có bất kỳ result nào
    result_keys = [key for key in st.session_state.keys() if key.endswith('_result')]
    if result_keys:
        if st.button("🔄 Tạo mới", key='reset_all_results', help="Xóa tất cả kết quả và file đã upload"):
            # Xóa tất cả result keys
            for key in result_keys:
                del st.session_state[key]
            
            # Xóa tất cả file uploader keys
            file_keys = [
                'be_raw', 'be_interaction',
                'ghn_raw', 'ghn_interaction', 
                'hafele_file',
                'hospital_files', 'hp_interaction', 'hm_interaction',
                'sf_raw', 'sf_demo',
                'thp_file',
                'hdb_file',
                'pnj_raw', 'pnj_demo',
                'home_raw', 'home_interaction',
                'home_fraud_raw', 'home_fraud_interaction'
            ]
            for key in file_keys:
                if key in st.session_state:
                    del st.session_state[key]
            
            # Xóa selectbox key để reset về project đầu tiên
            if 'split_project_select' in st.session_state:
                st.session_state['split_project_select'] = "Be App"
            
            st.rerun()
    
    # Project selection
    split_projects = splitter.get_supported_projects()
    selected_split_project = st.selectbox(
        "Chọn Project",
        options=split_projects,
        key='split_project_select'
    )
    
    # Clear result when project changes
    if 'previous_project' not in st.session_state:
        st.session_state['previous_project'] = selected_split_project
    
    if st.session_state['previous_project'] != selected_split_project:
        # Clear result of previous project
        if st.session_state['previous_project'] in PROJECT_CONFIGS:
            prev_config = PROJECT_CONFIGS[st.session_state['previous_project']]
            result_key = f"{prev_config['key']}_result"
            if result_key in st.session_state:
                del st.session_state[result_key]
        
        st.session_state['previous_project'] = selected_split_project
    
    st.divider()
    
    # Render project UI based on configuration
    if selected_split_project in PROJECT_CONFIGS:
        config = PROJECT_CONFIGS[selected_split_project]
        st.subheader(config['subtitle'])
        files = render_file_uploaders(config)
        render_process_section(selected_split_project, config, files)
