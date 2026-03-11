# -*- coding: utf-8 -*-
"""
Process DataFrame: Xử lý dataframe để tạo cột Text và Topic
"""

import pandas as pd

import unicodedata
import re

def read_files(files, sheet_name=None):
    dfs = []
    for file in files:
        if sheet_name:
            df = pd.read_excel(file, sheet_name=sheet_name)
        else:
            df = pd.read_excel(file)
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    return df

def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = unicodedata.normalize("NFKD", text)
    # text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).lower().strip()

def sanitize_excel_values(df: pd.DataFrame):
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: f"'{x}" if isinstance(x, str) and x.strip().startswith('=') else x
        )
    return df

def process_dataframe(df):
    """
    Xử lý dataframe để tạo cột Text và Topic
    
    Logic:
    - Nếu cột Type (lowercase) chứa "topic": 
        Text = merge Title + Content + Description (bỏ trùng)
    - Ngược lại: Text = merge Title + Content (bỏ trùng)
    - Topic lấy từ cột Topic
    """
    df = df.copy()
    
    # Chuẩn hóa tên cột
    df.columns = df.columns.str.strip()
    
    # Kiểm tra cột Type
    has_type_col = any('type' in col.lower() for col in df.columns)
    type_col = next((col for col in df.columns if 'type' in col.lower()), None)
    
    # Tìm các cột cần thiết
    title_col = next((col for col in df.columns if 'title' in col.lower()), None)
    content_col = next((col for col in df.columns if 'content' in col.lower()), None)
    description_col = next((col for col in df.columns if 'description' in col.lower()), None)
    topic_col = next((col for col in df.columns if 'topic' == col.lower()), None)
    
    def merge_unique_text(*texts):
        """Merge các text và bỏ trùng"""
        unique_parts = []
        seen = set()
        
        for text in texts:
            if pd.notna(text) and str(text).strip():
                text_str = str(text).strip()
                text_lower = text_str.lower()
                if text_lower not in seen:
                    unique_parts.append(text_str)
                    seen.add(text_lower)
        
        return ' '.join(unique_parts)
    
    # Tạo cột Text
    def create_text_column(row):
        if has_type_col and type_col and pd.notna(row.get(type_col)):
            type_value = str(row[type_col]).lower()
            if 'topic' in type_value:
                # Merge Title + Content + Description
                return merge_unique_text(
                    row.get(title_col),
                    row.get(content_col),
                    row.get(description_col)
                )
        
        # Merge Title + Content
        return merge_unique_text(
            row.get(title_col),
            row.get(content_col)
        )
    
    df['Text'] = df.apply(create_text_column, axis=1)
    
    # Tạo cột Topic
    if topic_col:
        df['Topic'] = df[topic_col]
    else:
        df['Topic'] = ''
    
    return df
