# -*- coding: utf-8 -*-
"""
Categorizer: Phân loại dòng data dựa trên topic và keywords
"""

import json
import pandas as pd
from typing import Optional, Dict, List

from src.process_dataframe import normalize_text, sanitize_excel_values

class Categorizer:
    def __init__(self, rules_file='categorize_rules.json'):
        """Load rules từ file JSON"""
        with open(rules_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            self.projects = data['projects']
            # Flatten tất cả rules từ các projects
            self.all_rules = []
            for project_name, rules in self.projects.items():
                for rule in rules:
                    self.all_rules.append({
                        'project': project_name,
                        **rule
                    })
    
    def _normalize_text(self, text: str) -> str:
        """Chuẩn hóa text để so sánh (lowercase, strip)"""
        if not text:
            return ""
        return normalize_text(text)
    
    def _check_keyword_match(self, content: str, keywords: List[str]) -> bool:
        """
        Kiểm tra xem content có chứa bất kỳ keyword nào không
        """
        if not content:
            return False
        
        content_lower = self._normalize_text(content)
        
        for keyword in keywords:
            if self._normalize_text(keyword) in content_lower or self._normalize_text(keyword.replace(" ", "")) in content_lower:
                return True
        
        return False
    
    def _check_topic_match(self, topic: str, valid_topics: List[str]) -> bool:
        """Kiểm tra xem topic có nằm trong danh sách valid topics không"""
        if not topic:
            return False
        
        topic_lower = self._normalize_text(topic)
        
        for valid_topic in valid_topics:
            # print("Checking {valid_topic} in {topic_lower}")
            if self._normalize_text(valid_topic) in topic_lower:
                return True
        
        return False
    
    def categorize_row(self, row: pd.Series, topic_col='topic', content_col='content', 
                      project_filter=None) -> Optional[str]:
        """
        Phân loại một dòng dataframe
        
        Args:
            row: pandas Series (một dòng của dataframe)
            topic_col: tên cột chứa topic
            content_col: tên cột chứa content
            project_filter: tên project để filter (None = check tất cả projects)
        
        Returns:
            category name hoặc None nếu không match
        """
        topic = row.get(topic_col, '')
        content = row.get(content_col, '')
        
        # Duyệt qua tất cả rules
        for rule in self.all_rules:
            # Filter theo project nếu có
            if project_filter and rule['project'] != project_filter:
                continue
            
            has_topics = rule.get('topics') and len(rule['topics']) > 0
            has_keywords = rule.get('keywords') and len(rule['keywords']) > 0
            
            # Case 1: Chỉ có keywords, không có topics (SCG, SHB)
            if not has_topics and has_keywords:
                if self._check_keyword_match(content, rule['keywords']):
                    return rule['cate']
            
            # Case 2: Chỉ có topics, không có keywords (Vinamilk)
            elif has_topics and not has_keywords:
                if self._check_topic_match(topic, rule['topics']):
                    return rule['cate']
            
            # Case 3: Có cả topics và keywords (Hafele)
            elif has_topics and has_keywords:
                if self._check_topic_match(topic, rule['topics']):
                    if self._check_keyword_match(content, rule['keywords']):
                        return rule['cate']
        
        return None
    
    def categorize_dataframe(self, df: pd.DataFrame, topic_col='topic', content_col='content', 
                            output_col='Category', project_filter=None) -> pd.DataFrame:
        """
        Phân loại toàn bộ dataframe
        
        Args:
            df: pandas DataFrame
            topic_col: tên cột chứa topic
            content_col: tên cột chứa content
            output_col: tên cột output để lưu category
            project_filter: tên project để filter (None = check tất cả projects)
        
        Returns:
            DataFrame với cột category mới
        """
        df = df.copy()
        df[output_col] = df.apply(
            lambda row: self.categorize_row(row, topic_col, content_col, project_filter), 
            axis=1
        )
        df = df.drop(columns=["Text"])
        return df
    
    def get_projects(self) -> List[str]:
        """Lấy danh sách tên các projects"""
        return list(self.projects.keys())
    
    def get_project_rules(self, project_name: str) -> List[Dict]:
        """Lấy rules của một project cụ thể"""
        return self.projects.get(project_name, [])
