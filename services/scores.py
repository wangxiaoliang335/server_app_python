"""Scores-related DB helpers extracted from app.py to reduce module size."""
import datetime
import json
import os
import time
import traceback
from typing import Any, Dict, List, Optional

import mysql.connector

from common import app_logger
from db import get_db_connection


def parse_excel_file_url(excel_file_url):
    """
    解析excel_file_url字段，将JSON格式转换为数组格式
    支持多种格式：
    1. 旧格式（单个URL字符串）: "https://..."
    2. 旧格式（JSON对象）: {"文件名": "URL"}
    3. 新格式（带说明与字段映射）: {"文件名": {"url": "URL", "description": "说明", "fields": ["语文", ...]}}
    返回格式: [{"filename": "文件名", "url": "URL", "description": "说明", "fields": [...]}, ...]
    """
    if not excel_file_url:
        return []
    
    try:
        # 尝试解析为JSON
        if isinstance(excel_file_url, str):
            url_dict = json.loads(excel_file_url)
        else:
            url_dict = excel_file_url
        
        # 如果是字典格式
        if isinstance(url_dict, dict):
            result = []
            for filename, value in url_dict.items():
                # 判断是新格式（对象）还是旧格式（字符串）
                if isinstance(value, dict):
                    # 新格式: {"文件名": {"url": "URL", "description": "说明", "fields": []}}
                    result.append({
                        'filename': filename,
                        'url': value.get('url', ''),
                        'description': value.get('description', ''),
                        'fields': value.get('fields', []) or []
                    })
                else:
                    # 旧格式: {"文件名": "URL"}
                    result.append({
                        'filename': filename,
                        'url': value,
                        'description': '',
                        'fields': []
                    })
            return result
        # 如果是列表格式（可能未来扩展）
        elif isinstance(url_dict, list):
            # 确保每个元素都包含必需字段
            normalized = []
            for item in url_dict:
                if isinstance(item, dict):
                    normalized.append({
                        'filename': item.get('filename', ''),
                        'url': item.get('url', ''),
                        'description': item.get('description', ''),
                        'fields': item.get('fields', []) or []
                    })
            return normalized
        # 如果是字符串（旧格式，单个URL）
        elif isinstance(url_dict, str):
            return [{'filename': 'excel_file', 'url': url_dict, 'description': '', 'fields': []}]
        else:
            return []
    except (json.JSONDecodeError, TypeError, AttributeError):
        # 如果解析失败，可能是旧的单个URL格式
        if isinstance(excel_file_url, str):
            return [{'filename': 'excel_file', 'url': excel_file_url, 'description': '', 'fields': []}]
        return []


def save_student_scores(
    class_id: str,
    exam_name: Optional[str],
    term: Optional[str] = None,
    remark: Optional[str] = None,
    scores: List[Dict] = None,
    excel_file_url: Optional[str] = None,
    excel_file_name: Optional[str] = None,
    excel_file_description: Optional[str] = None,
    excel_files: Optional[List[Dict]] = None,
    operation_mode: str = 'append',
    fields: List[Dict] = None
) -> Dict[str, object]:
    """
    保存学生成绩表
    参数说明：
    - class_id: 班级ID（必需）
    - exam_name: 考试名称（必需，如"期中考试"、"期末考试"）
    - term: 学期（可选，如 '2025-2026-1'）
    - remark: 备注（可选）
    - excel_file_url: Excel文件在OSS的URL（可选）
    - excel_file_name: Excel文件名（可选，用于管理多个文件）
    - excel_file_description: Excel文件说明（可选）
    - operation_mode: 操作模式，'append'（追加，默认）或 'replace'（替换）
    - fields: 字段定义列表（可选），每个元素包含:
      {
        'field_name': str,      # 字段名称（必需）
        'field_type': str,       # 字段类型（可选，默认'number'）
        'field_order': int,      # 字段顺序（可选）
        'is_total': int          # 是否为总分字段（可选，0或1）
      }
    - scores: 成绩明细列表，每个元素包含:
      {
        'student_id': str,      # 学号（可选）
        'student_name': str,    # 姓名（必需）
        'chinese': int,         # 语文成绩（可选）
        'math': int,            # 数学成绩（可选）
        'english': int,         # 英语成绩（可选）
        'total_score': float    # 总分（可选，可自动计算）
      }
    
    返回：
    - { success, score_header_id, inserted_count, updated_count, deleted_count, message }
    """
    if not class_id:
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '缺少必要参数 class_id' }

    # 兼容：exam_name 不再作为定位条件，但表结构 NOT NULL，仍需写入一个展示用字符串
    provided_exam_name = exam_name
    exam_name = (provided_exam_name or '').strip()
    if not exam_name:
        exam_name = '成绩'
    
    # 验证operation_mode
    if operation_mode not in ['append', 'replace']:
        operation_mode = 'append'  # 默认使用追加模式
    
    # 在替换模式下，scores可以为空（用于删除所有数据）
    if operation_mode == 'replace' and (not scores or not isinstance(scores, list)):
        scores = []
    elif operation_mode == 'append' and (not scores or not isinstance(scores, list)):
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '成绩明细列表不能为空' }

    app_logger.info(
        f"[save_student_scores] start class_id={class_id}, exam_name={exam_name}, term={term}, "
        f"operation_mode={operation_mode}, scores_count={len(scores) if scores else 0}"
    )
    
    connection = get_db_connection()
    if connection is None:
        error_msg = "Save student scores failed: Database connection error."
        print(f"[save_student_scores] 错误: {error_msg}")
        app_logger.error(error_msg)
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '数据库连接失败' }

    try:
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 预先收集excel文件中的字段，用于替换模式下避免误删其他excel对应的字段
        keep_fields_from_excel_urls = set()  # 最终用于保留“其他”Excel的字段
        other_excels_fields = set()          # 其他Excel的字段集合
        current_excel_old_fields = set()     # 本次上传对应Excel在旧数据中的字段
        current_excel_new_fields = set()     # 本次上传对应Excel在新数据中的字段
        uploaded_filenames = set()           # 本次上传涉及的文件名

        # 1. 插入或获取成绩表头
        # 约定：class_id + term 能定位一张成绩表；exam_name 仅作为展示字段保留，不作为定位条件
        if term is None:
            cursor.execute(
                "SELECT id, excel_file_url "
                "FROM ta_student_score_header "
                "WHERE class_id = %s AND term IS NULL "
                "ORDER BY created_at DESC, updated_at DESC "
                "LIMIT 1",
                (class_id,)
            )
        else:
            cursor.execute(
                "SELECT id, excel_file_url "
                "FROM ta_student_score_header "
                "WHERE class_id = %s AND term = %s "
                "ORDER BY created_at DESC, updated_at DESC "
                "LIMIT 1",
                (class_id, term)
            )
        header_row = cursor.fetchone()

        if header_row is None:
            # 插入新表头
            app_logger.info(
                f"[save_student_scores] insert header class_id={class_id}, term={term}, exam_name={exam_name}"
            )
            
            # 处理excel文件信息，使用JSON格式存储（支持多个文件，包含description与fields）
            final_excel_file_url = None
            if excel_files and isinstance(excel_files, list) and len(excel_files) > 0:
                url_dict = {}
                for ef in excel_files:
                    fn = ef.get('filename') or ef.get('name') or ef.get('file_name')
                    if not fn:
                        continue
                    url_dict[fn] = {
                        'url': ef.get('url', ''),
                        'description': ef.get('description', ''),
                        'fields': ef.get('fields', []) or []
                    }
                if url_dict:
                    final_excel_file_url = json.dumps(url_dict, ensure_ascii=False)
            elif excel_file_url:
                if excel_file_name:
                    # 使用新格式: {"文件名": {"url": "URL", "description": "说明", "fields": []}}
                    file_info = {
                        'url': excel_file_url,
                        'description': excel_file_description if excel_file_description else '',
                        'fields': []
                    }
                    url_dict = {excel_file_name: file_info}
                else:
                    # 如果没有文件名，使用默认key
                    timestamp = int(time.time())
                    file_info = {
                        'url': excel_file_url,
                        'description': excel_file_description if excel_file_description else '',
                        'fields': []
                    }
                    url_dict = {f"excel_file_{timestamp}": file_info}
                final_excel_file_url = json.dumps(url_dict, ensure_ascii=False)
            
            if final_excel_file_url:
                try:
                    parsed_dict = json.loads(final_excel_file_url)
                    if isinstance(parsed_dict, dict):
                        for _, v in parsed_dict.items():
                            if isinstance(v, dict):
                                keep_fields_from_excel_urls.update(v.get('fields', []) or [])
                except Exception as e:
                    app_logger.warning(f"[save_student_scores] parse excel_file_url failed: {e}")
            
            insert_header_sql = (
                "INSERT INTO ta_student_score_header (class_id, exam_name, term, remark, excel_file_url, created_at) "
                "VALUES (%s, %s, %s, %s, %s, NOW())"
            )
            cursor.execute(insert_header_sql, (class_id, exam_name, term, remark, final_excel_file_url))
            score_header_id = cursor.lastrowid
            app_logger.info(f"[save_student_scores] header inserted score_header_id={score_header_id}")
        else:
            score_header_id = header_row['id']
            app_logger.info(f"[save_student_scores] header exists score_header_id={score_header_id}")
            # 更新表头信息（若存在）
            update_fields = []
            update_values = []
            # exam_name 仅用于展示：如果客户端传了新值，则更新
            if provided_exam_name is not None:
                normalized_exam_name = str(provided_exam_name).strip()
                if normalized_exam_name:
                    update_fields.append("exam_name = %s")
                    update_values.append(normalized_exam_name)
                    app_logger.info(f"[save_student_scores] update header exam_name={normalized_exam_name}")
            if remark is not None:
                update_fields.append("remark = %s")
                update_values.append(remark)
            # 更新 excel_file_url（支持excel_files数组，或单个excel_file_url）：
            # - 同名文件则覆盖（url/description/fields）
            # - 不同文件追加
            # NOTE: 这里不再打印 excel_file_url / SQL 等调试信息，避免日志爆炸
            
            # 记录本次上传涉及的文件名
            if excel_files and isinstance(excel_files, list) and len(excel_files) > 0:
                for ef in excel_files:
                    fn = ef.get('filename') or ef.get('name') or ef.get('file_name')
                    if fn:
                        uploaded_filenames.add(fn)
            elif excel_file_name:
                uploaded_filenames.add(excel_file_name)

            if (excel_files and isinstance(excel_files, list) and len(excel_files) > 0) or excel_file_url:
                # 获取现有的excel_file_url值
                existing_excel_file_url = header_row.get('excel_file_url') if header_row else None
                print(f"[save_student_scores] 📋 现有的excel_file_url值: {existing_excel_file_url}")
                app_logger.info(f"[save_student_scores] 📋 现有的excel_file_url值: {existing_excel_file_url}")
                
                # 解析现有的URL列表（兼容旧格式），转换为新格式，补齐fields
                url_dict = {}
                if existing_excel_file_url:
                    try:
                        existing_dict = json.loads(existing_excel_file_url)
                        if not isinstance(existing_dict, dict):
                            existing_dict = {}
                        # 归一化为新格式
                        for filename, value in existing_dict.items():
                            if isinstance(value, dict):
                                url_dict[filename] = {
                                    'url': value.get('url', ''),
                                    'description': value.get('description', ''),
                                    'fields': value.get('fields', []) or []
                                }
                                if filename in uploaded_filenames:
                                    current_excel_old_fields.update(url_dict[filename]['fields'])
                                else:
                                    other_excels_fields.update(url_dict[filename]['fields'])
                            else:
                                url_dict[filename] = {
                                    'url': value,
                                    'description': '',
                                    'fields': []
                                }
                        print(f"[save_student_scores] ✅ 成功解析现有的URL字典: {url_dict}")
                        app_logger.info(f"[save_student_scores] ✅ 成功解析现有的URL字典: {url_dict}")
                    except (json.JSONDecodeError, TypeError):
                        # 旧的单URL格式
                        print(f"[save_student_scores] ⚠️ 现有值不是JSON格式，转换为字典格式")
                        app_logger.warning(f"[save_student_scores] ⚠️ 现有值不是JSON格式，转换为字典格式")
                        key_name = excel_file_name or 'excel_file'
                        url_dict[key_name] = {
                            'url': existing_excel_file_url,
                            'description': '',
                            'fields': []
                        }
                        if key_name in uploaded_filenames:
                            current_excel_old_fields.update(url_dict[key_name]['fields'])
                        else:
                            other_excels_fields.update(url_dict[key_name]['fields'])
                
                # 更新或添加新的文件信息
                if excel_files and isinstance(excel_files, list) and len(excel_files) > 0:
                    for ef in excel_files:
                        fn = ef.get('filename') or ef.get('name') or ef.get('file_name')
                        if not fn:
                            continue
                        url_dict[fn] = {
                            'url': ef.get('url', ''),
                            'description': ef.get('description', ''),
                            'fields': ef.get('fields', []) or []
                        }
                        if fn in uploaded_filenames:
                            current_excel_new_fields.update(url_dict[fn]['fields'])
                        else:
                            other_excels_fields.update(url_dict[fn]['fields'])
                        pass
                elif excel_file_url:
                    if excel_file_name:
                        url_dict[excel_file_name] = {
                            'url': excel_file_url,
                            'description': excel_file_description if excel_file_description else '',
                            'fields': []
                        }
                        current_excel_new_fields.update(url_dict[excel_file_name]['fields'])
                        pass
                    else:
                        timestamp = int(time.time())
                        default_key = f"excel_file_{timestamp}"
                        url_dict[default_key] = {
                            'url': excel_file_url,
                            'description': excel_file_description if excel_file_description else '',
                            'fields': []
                        }
                        current_excel_new_fields.update(url_dict[default_key]['fields'])
                        pass
                
                # 将字典转换为JSON字符串保存
                updated_excel_file_url = json.dumps(url_dict, ensure_ascii=False)
                
                update_fields.append("excel_file_url = %s")
                update_values.append(updated_excel_file_url)
                
                # 仅保留其他excel的字段，当前上传excel的字段不加入保留集
                keep_fields_from_excel_urls = set(other_excels_fields)
            else:
                pass
            if update_fields:
                update_values.append(score_header_id)
                update_sql = f"UPDATE ta_student_score_header SET {', '.join(update_fields)}, updated_at = NOW() WHERE id = %s"
                cursor.execute(update_sql, tuple(update_values))
                app_logger.info(f"[save_student_scores] header updated score_header_id={score_header_id}, rowcount={cursor.rowcount}")
            else:
                pass
            # 不删除旧的成绩明细和字段定义，而是追加新的数据
            app_logger.info(f"[save_student_scores] append mode on existing header score_header_id={score_header_id}")

        # 2/3. 不再打印 scores 明细与字段列表（过大）；仅记录摘要
        app_logger.info(
            f"[save_student_scores] process fields/details score_header_id={score_header_id}, "
            f"operation_mode={operation_mode}, scores_count={len(scores)}"
        )
        
        # 如果提供了fields参数，使用fields；否则从scores中提取。并合并excel_file_url中的字段，避免误删其他excel字段。
        if fields and isinstance(fields, list) and len(fields) > 0:
            # 使用提供的字段定义
            field_definitions = fields
            field_name_set = {f.get('field_name') for f in field_definitions if f.get('field_name')}
        else:
            # 从scores数据中提取所有字段名（除了student_id和student_name）
            field_set = set()
            for score_item in scores:
                for key in score_item.keys():
                    if key not in ['student_id', 'student_name']:
                        field_set.add(key)
            
            # 转换为字段定义格式
            field_definitions = []
            for idx, field_name in enumerate(sorted(list(field_set))):
                field_definitions.append({
                    'field_name': field_name,
                    'field_type': 'number',
                    'field_order': idx + 1,
                    'is_total': 1 if '总分' in field_name or 'total' in field_name.lower() else 0
                })
            field_name_set = field_set

        # 合并excel_file_url中的“其他excel字段”，防止替换模式误删它们；当前上传的excel字段不加入保留集
        if keep_fields_from_excel_urls:
            field_name_set = set(field_name_set) if 'field_name_set' in locals() else set()
            field_name_set.update(keep_fields_from_excel_urls)
            # 将缺失在field_definitions中的“其他excel字段”补充到定义列表（保持基础属性，顺序追加）
            existing_def_names = {f.get('field_name') for f in field_definitions if f.get('field_name')}
            append_idx = len(field_definitions)
            for fname in sorted(list(keep_fields_from_excel_urls)):
                if fname not in existing_def_names:
                    append_idx += 1
                    field_definitions.append({
                        'field_name': fname,
                        'field_type': 'number',
                        'field_order': append_idx,
                        'is_total': 1 if '总分' in fname or 'total' in fname.lower() else 0
                    })
            pass

        # 当前上传中涉及的字段集合（用于替换模式时保留其他excel的字段）
        upload_field_set = set()
        for score_item in scores:
            for key in score_item.keys():
                if key not in ['student_id', 'student_name']:
                    upload_field_set.add(key)
        if not upload_field_set and fields:
            upload_field_set = {f.get('field_name') for f in fields if f.get('field_name')}
        
        # 确定当前上传的Excel文件名（用于字段定义和成绩保存）
        current_excel_filename = None
        if excel_files and isinstance(excel_files, list) and len(excel_files) > 0:
            # 如果有多个文件，使用第一个文件的文件名
            current_excel_filename = excel_files[0].get('filename') or excel_files[0].get('name') or excel_files[0].get('file_name')
        elif excel_file_name:
            current_excel_filename = excel_file_name
        
        # 如果无法确定文件名，使用默认值（避免 NOT NULL 约束错误）
        if not current_excel_filename:
            current_excel_filename = f"excel_file_{int(time.time())}"
            app_logger.warning(f"[save_student_scores] ⚠️ 无法确定Excel文件名，使用默认值: {current_excel_filename}")
        
        # 4. 在替换模式下，删除不在新数据中的字段（需要按 excel_filename 和 field_name 组合删除）
        deleted_field_count = 0
        if operation_mode == 'replace' and field_name_set and current_excel_filename:
            # 查询当前Excel文件的所有现有字段
            cursor.execute(
                "SELECT field_name FROM ta_student_score_field WHERE score_header_id = %s AND excel_filename = %s",
                (score_header_id, current_excel_filename)
            )
            existing_fields = cursor.fetchall()
            existing_field_names = {f['field_name'] for f in existing_fields}
            
            # 找出需要删除的字段（存在于当前Excel但不在新数据中）
            fields_to_delete = existing_field_names - field_name_set
            if fields_to_delete:
                delete_field_sql = "DELETE FROM ta_student_score_field WHERE score_header_id = %s AND field_name = %s AND excel_filename = %s"
                for field_name in fields_to_delete:
                    cursor.execute(delete_field_sql, (score_header_id, field_name, current_excel_filename))
                    deleted_field_count += 1
                    app_logger.info(f"[save_student_scores] 删除字段: {field_name} (来自 {current_excel_filename})")
                app_logger.info(f"[save_student_scores] 替换模式下删除字段完成 - 删除{deleted_field_count}个字段")
        
        # 5. 保存或更新字段定义（添加 excel_filename 字段支持）
        if field_definitions:
            insert_field_sql = (
                "INSERT INTO ta_student_score_field "
                "(score_header_id, field_name, excel_filename, field_type, field_order, is_total) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE "
                "field_type = VALUES(field_type), "
                "field_order = VALUES(field_order), "
                "is_total = VALUES(is_total)"
            )
            new_field_count = 0
            updated_field_count = 0
            for field_def in field_definitions:
                field_name = field_def.get('field_name')
                if not field_name:
                    continue
                
                # 检查字段是否已存在（需要同时匹配 field_name 和 excel_filename）
                cursor.execute(
                    "SELECT id FROM ta_student_score_field WHERE score_header_id = %s AND field_name = %s AND excel_filename = %s",
                    (score_header_id, field_name, current_excel_filename)
                )
                existing_field = cursor.fetchone()
                
                field_type = field_def.get('field_type', 'number')
                field_order = field_def.get('field_order')
                is_total = field_def.get('is_total', 0)
                
                # 如果没有提供field_order，使用默认值
                if field_order is None:
                    if existing_field:
                        # 保持原有顺序
                        cursor.execute(
                            "SELECT field_order FROM ta_student_score_field WHERE score_header_id = %s AND field_name = %s AND excel_filename = %s",
                            (score_header_id, field_name, current_excel_filename)
                        )
                        order_result = cursor.fetchone()
                        field_order = order_result['field_order'] if order_result else 1
                    else:
                        # 新字段，追加到最后（按当前Excel文件的字段顺序）
                        cursor.execute(
                            "SELECT MAX(field_order) as max_order FROM ta_student_score_field WHERE score_header_id = %s AND excel_filename = %s",
                            (score_header_id, current_excel_filename)
                        )
                        max_order_result = cursor.fetchone()
                        max_order = max_order_result['max_order'] if max_order_result and max_order_result['max_order'] is not None else 0
                        field_order = max_order + 1
                
                cursor.execute(insert_field_sql, (
                    score_header_id,
                    field_name,
                    current_excel_filename,
                    field_type,
                    field_order,
                    is_total
                ))
                
                if existing_field:
                    updated_field_count += 1
                    print(f"[save_student_scores] 更新字段: {field_name} (顺序: {field_order})")
                    app_logger.info(f"[save_student_scores] 更新字段: {field_name} (顺序: {field_order})")
                else:
                    new_field_count += 1
                    print(f"[save_student_scores] 新增字段: {field_name} (顺序: {field_order})")
                    app_logger.info(f"[save_student_scores] 新增字段: {field_name} (顺序: {field_order})")
            
            print(f"[save_student_scores] 字段定义保存完成 - 新增{new_field_count}个字段，更新{updated_field_count}个字段，删除{deleted_field_count}个字段")
            app_logger.info(f"[save_student_scores] 字段定义保存完成 - 新增{new_field_count}个字段，更新{updated_field_count}个字段，删除{deleted_field_count}个字段")

        # 6. 在替换模式下，删除不在新数据中的学生
        deleted_student_count = 0
        if operation_mode == 'replace':
            # 收集新数据中的所有学生标识（student_name + student_id）
            new_student_keys = set()
            for score_item in scores:
                student_name = score_item.get('student_name', '').strip()
                student_id = score_item.get('student_id')
                if student_name:
                    # 使用 (student_name, student_id) 作为唯一标识
                    new_student_keys.add((student_name, student_id))
            
            # 查询所有现有学生
            cursor.execute(
                "SELECT id, student_name, student_id FROM ta_student_score_detail WHERE score_header_id = %s",
                (score_header_id,)
            )
            existing_students = cursor.fetchall()
            
            # 找出需要删除的学生（存在于数据库但不在新数据中）
            students_to_delete = []
            for student in existing_students:
                student_name = student.get('student_name', '').strip()
                student_id = student.get('student_id')
                student_key = (student_name, student_id)
                if student_key not in new_student_keys:
                    students_to_delete.append(student['id'])
            
            if students_to_delete:
                delete_student_sql = "DELETE FROM ta_student_score_detail WHERE id = %s"
                for student_id_to_delete in students_to_delete:
                    cursor.execute(delete_student_sql, (student_id_to_delete,))
                    deleted_student_count += 1
                print(f"[save_student_scores] 替换模式下删除学生完成 - 删除{deleted_student_count}个学生")
                app_logger.info(f"[save_student_scores] 替换模式下删除学生完成 - 删除{deleted_student_count}个学生")
        
        # 7. 批量插入或更新成绩明细（使用JSON格式存储动态字段）
        print(f"[save_student_scores] 开始插入/更新成绩明细 - score_header_id={score_header_id}, operation_mode={operation_mode}, 待处理数量={len(scores)}")
        app_logger.info(f"[save_student_scores] 开始插入/更新成绩明细 - score_header_id={score_header_id}, operation_mode={operation_mode}, 待处理数量={len(scores)}")
        
        # 使用 INSERT ... ON DUPLICATE KEY UPDATE 来支持插入或更新
        # 注意：需要根据student_id和student_name来判断是否已存在
        insert_detail_sql = (
            "INSERT INTO ta_student_score_detail "
            "(score_header_id, student_id, student_name, scores_json, comments_json, field_source_json, total_score) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "scores_json = VALUES(scores_json), "
            "comments_json = VALUES(comments_json), "
            "field_source_json = VALUES(field_source_json), "
            "total_score = VALUES(total_score), "
            "updated_at = NOW()"
        )
        
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        
        for idx, score_item in enumerate(scores):
            student_id = score_item.get('student_id')
            student_name = score_item.get('student_name', '').strip()
            if not student_name:
                skipped_count += 1
                print(f"[save_student_scores] 跳过第{idx+1}条记录：缺少学生姓名 - score_item={score_item}")
                app_logger.warning(f"[save_student_scores] 跳过第{idx+1}条记录：缺少学生姓名 - score_item={score_item}")
                continue  # 跳过没有姓名的记录
            
            # 检查该学生是否已有成绩记录
            check_sql = (
                "SELECT id, scores_json, field_source_json, comments_json FROM ta_student_score_detail "
                "WHERE score_header_id = %s AND student_name = %s "
                "AND (%s IS NULL OR student_id = %s) "
                "LIMIT 1"
            )
            cursor.execute(check_sql, (score_header_id, student_name, student_id, student_id))
            existing_record = cursor.fetchone()
            
            # 构建JSON对象（使用复合键名：字段名_Excel文件名）
            # 这样可以支持同一字段名来自不同Excel文件的情况
            scores_json = {}
            comments_json = {}  # 保存注释（支持复合键名）
            field_source_json = {}  # 记录字段名到Excel文件名的映射
            total_score = None
            for key, value in score_item.items():
                if key not in ['student_id', 'student_name']:
                    if value is not None:
                        # 检查是否为注释字段（以 _comment 结尾）
                        if key.endswith('_comment'):
                            # 这是注释字段，提取字段名并保存到 comments_json
                            field_name = key[:-8]  # 去掉 '_comment' 后缀
                            comment_value = str(value).strip() if value else ''
                            if comment_value:
                                # 使用复合键名保存注释（字段名_Excel文件名）
                                comment_key = f"{field_name}_{current_excel_filename}" if current_excel_filename else field_name
                                comments_json[comment_key] = comment_value
                                # 同时为了兼容性，也保存简单字段名（如果还没有的话）
                                if field_name not in comments_json:
                                    comments_json[field_name] = comment_value
                        else:
                            # 这是成绩字段，保存到 scores_json
                            # 使用复合键名（字段名_Excel文件名）来保存，避免同名字段覆盖
                            composite_key = f"{key}_{current_excel_filename}" if current_excel_filename else key
                            
                            # 尝试转换为数字
                            try:
                                if isinstance(value, (int, float)):
                                    scores_json[composite_key] = float(value)
                                elif isinstance(value, str) and value.strip():
                                    # 尝试解析为数字
                                    scores_json[composite_key] = float(value.strip())
                                else:
                                    scores_json[composite_key] = value
                            except (ValueError, TypeError):
                                scores_json[composite_key] = value
                            
                            # 记录字段来源映射（如果同一字段名来自多个Excel，使用数组）
                            if key in field_source_json:
                                # 如果已有记录，转换为数组
                                existing_source = field_source_json[key]
                                if isinstance(existing_source, str):
                                    field_source_json[key] = [existing_source, current_excel_filename] if current_excel_filename else existing_source
                                elif isinstance(existing_source, list):
                                    if current_excel_filename and current_excel_filename not in existing_source:
                                        existing_source.append(current_excel_filename)
                            else:
                                # 首次记录
                                field_source_json[key] = current_excel_filename if current_excel_filename else key
                        
                        # 检查是否为总分字段
                        if ('总分' in key or 'total' in key.lower()) and value is not None:
                            try:
                                # 如果有多个"总分"字段，取最后一个（或者可以根据业务需求调整）
                                total_score = float(value)
                            except (ValueError, TypeError):
                                pass
            
            # 在追加模式下，如果记录已存在，合并JSON数据（保留旧字段，添加新字段）
            # 在替换模式下，仅替换本次上传涉及的字段，保留其他excel的字段
            existing_field_source_json = {}
            existing_comments_json = {}
            if existing_record and existing_record.get('field_source_json'):
                try:
                    existing_field_source_json = json.loads(existing_record['field_source_json']) if isinstance(existing_record['field_source_json'], str) else existing_record['field_source_json']
                except (json.JSONDecodeError, TypeError):
                    existing_field_source_json = {}
            
            if existing_record and existing_record.get('comments_json'):
                try:
                    existing_comments_json = json.loads(existing_record['comments_json']) if isinstance(existing_record['comments_json'], str) else existing_record['comments_json']
                except (json.JSONDecodeError, TypeError):
                    existing_comments_json = {}
            
            if operation_mode == 'append' and existing_record and existing_record.get('scores_json'):
                try:
                    existing_json = json.loads(existing_record['scores_json']) if isinstance(existing_record['scores_json'], str) else existing_record['scores_json']
                    # 合并时，复合键名不会冲突（因为包含Excel文件名）
                    merged_json = {**existing_json, **scores_json}
                    scores_json = merged_json
                    # 合并字段来源映射
                    field_source_json = {**existing_field_source_json, **field_source_json}
                    # 合并注释（复合键名不会冲突）
                    comments_json = {**existing_comments_json, **comments_json}
                    print(f"[save_student_scores] 合并已有成绩数据 - student_name={student_name}, 旧字段数={len(existing_json)}, 新字段数={len(scores_json)}")
                    app_logger.info(f"[save_student_scores] 合并已有成绩数据 - student_name={student_name}")
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"[save_student_scores] 解析已有JSON失败，使用新数据 - student_name={student_name}, error={e}")
                    app_logger.warning(f"[save_student_scores] 解析已有JSON失败，使用新数据 - student_name={student_name}, error={e}")
            elif operation_mode == 'replace' and existing_record and existing_record.get('scores_json'):
                try:
                    existing_json = json.loads(existing_record['scores_json']) if isinstance(existing_record['scores_json'], str) else existing_record['scores_json']
                    # 替换模式下，仅删除当前Excel文件的字段，保留其他Excel的字段
                    # 保留不以当前Excel文件名结尾的字段（即其他Excel的字段）
                    preserved = {k: v for k, v in existing_json.items() if not k.endswith(f"_{current_excel_filename}")}
                    scores_json = {**preserved, **scores_json}
                    # 同样处理字段来源映射：删除当前Excel的字段映射，保留其他的
                    preserved_sources = {k: v for k, v in existing_field_source_json.items() 
                                        if (isinstance(v, str) and v != current_excel_filename) or
                                           (isinstance(v, list) and current_excel_filename not in v)}
                    field_source_json = {**preserved_sources, **field_source_json}
                    # 同样处理注释：删除当前Excel的注释，保留其他的
                    preserved_comments = {k: v for k, v in existing_comments_json.items() 
                                         if not k.endswith(f"_{current_excel_filename}")}
                    comments_json = {**preserved_comments, **comments_json}
                    app_logger.info(f"[save_student_scores] 替换模式保留其他excel字段 - student_name={student_name}")
                except (json.JSONDecodeError, TypeError) as e:
                    app_logger.warning(f"[save_student_scores] 替换模式解析已有JSON失败，使用新数据 - student_name={student_name}, error={e}")
            
            # 如果没有找到总分字段，自动计算总分（所有数字字段的和）
            if total_score is None:
                total_score = 0.0
                for key, value in scores_json.items():
                    if isinstance(value, (int, float)):
                        total_score += float(value)
                if total_score == 0.0:
                    total_score = None  # 如果所有值都是0或没有值，设为None
            
            # 将scores_json、comments_json和field_source_json转换为JSON字符串
            scores_json_str = json.dumps(scores_json, ensure_ascii=False)
            comments_json_str = json.dumps(comments_json, ensure_ascii=False) if comments_json else None
            field_source_json_str = json.dumps(field_source_json, ensure_ascii=False) if field_source_json else None
            
            is_update = existing_record is not None
            action = "更新" if is_update else "插入"
            # 逐条成绩明细不再打印 JSON（过大）；只在失败时记录
            
            try:
                # 如果记录已存在，使用UPDATE语句
                if existing_record:
                    update_detail_sql = (
                        "UPDATE ta_student_score_detail "
                        "SET scores_json = %s, comments_json = %s, field_source_json = %s, total_score = %s, updated_at = NOW() "
                        "WHERE id = %s"
                    )
                    cursor.execute(update_detail_sql, (
                        scores_json_str,
                        comments_json_str,
                        field_source_json_str,
                        total_score,
                        existing_record['id']
                    ))
                    updated_count += 1
                else:
                    # 新记录，使用INSERT
                    cursor.execute(insert_detail_sql, (
                        score_header_id,
                        student_id,
                        student_name,
                        scores_json_str,
                        comments_json_str,
                        field_source_json_str,
                        total_score
                    ))
                    inserted_count += 1
            except Exception as insert_error:
                app_logger.error(f"[save_student_scores] 第{idx+1}条成绩{action}失败 - student_name={student_name}, error={insert_error}", exc_info=True)
                raise  # 重新抛出异常，让外层捕获

        app_logger.info(f"[save_student_scores] 成绩明细处理完成 - 插入={inserted_count}, 更新={updated_count}, 跳过={skipped_count}, 总计={len(scores)}")
        
        connection.commit()
        total_processed = inserted_count + updated_count
        app_logger.info(f"[save_student_scores] 事务提交成功 - score_header_id={score_header_id}, 插入={inserted_count}, 更新={updated_count}, 删除字段={deleted_field_count}, 删除学生={deleted_student_count}, 总计={total_processed}")
        return { 
            'success': True, 
            'score_header_id': score_header_id, 
            'inserted_count': inserted_count, 
            'updated_count': updated_count,
            'deleted_field_count': deleted_field_count,
            'deleted_student_count': deleted_student_count,
            'message': '保存成功' 
        }
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            app_logger.error(f"[save_student_scores] 数据库错误，回滚事务 - error={e}")
            connection.rollback()
        else:
            app_logger.error(f"[save_student_scores] 数据库错误，连接已断开 - error={e}")
        app_logger.error(f"Database error during save_student_scores: {e}", exc_info=True)
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'数据库错误: {e}' }
    except Exception as e:
        if connection and connection.is_connected():
            app_logger.error(f"[save_student_scores] 未知错误，回滚事务 - error={e}")
            connection.rollback()
        else:
            app_logger.error(f"[save_student_scores] 未知错误，连接已断开 - error={e}")
        app_logger.error(f"Unexpected error during save_student_scores: {e}", exc_info=True)
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'未知错误: {e}' }
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after saving student scores.")


def save_group_scores(
    class_id: str,
    exam_name: Optional[str] = None,
    term: Optional[str] = None,
    remark: Optional[str] = None,
    scores: List[Dict] = None,
    excel_file_url: Optional[str] = None,
    excel_file_name: Optional[str] = None,
    excel_file_description: Optional[str] = None,
    operation_mode: str = 'append',
    fields: List[Dict] = None,
    excel_files: List[Dict] = None
) -> Dict[str, object]:
    """
    保存小组成绩表（支持动态字段，使用JSON存储）
    参数说明：
    - class_id: 班级ID（必需）
    - exam_name: 考试/表名称（可选，仅展示字段；不再作为定位条件）
    - term: 学期（可选，如 '2025-2026-1'）
    - remark: 备注（可选）
    - excel_file_url: Excel文件在OSS的URL（可选）
    - excel_file_name: Excel文件名（可选，用于管理多个文件）
    - excel_file_description: Excel文件说明（可选）
    - operation_mode: 操作模式，'append'（追加，默认）或 'replace'（替换）
    - fields: 字段定义列表（可选），每个元素包含:
      {
        'field_name': str,      # 字段名称（必需）
        'field_type': str,       # 字段类型（可选，默认'number'）
        'field_order': int,      # 字段顺序（可选）
        'is_total': int          # 是否为总分字段（可选，0或1）
      }
    - excel_files: Excel文件列表（可选），每个元素包含:
      {
        'filename': str,         # 文件名（必需）
        'url': str,              # 文件URL（必需）
        'description': str,       # 文件说明（可选）
        'fields': [str]          # 该文件对应的字段列表（可选）
      }
    - scores: 成绩明细列表，每个元素包含:
      {
        'group_name': str,       # 小组名称/编号（必需，如"1"或"1组"）
        'student_id': str,       # 学号（可选）
        'student_name': str,     # 姓名（必需）
        '语文': int,              # 各科成绩（动态字段）
        '数学': int,
        '英语': int,
        '总分': float,            # 个人总分（可选，可自动计算）
        'group_total_score': float  # 小组总分（可选，可自动计算）
      }
    
    返回：
    - { success, score_header_id, inserted_count, updated_count, deleted_count, message }
    """
    if not class_id:
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '缺少必要参数 class_id' }

    exam_name_norm = str(exam_name).strip() if exam_name is not None and str(exam_name).strip() else None
    
    # 验证operation_mode
    if operation_mode not in ['append', 'replace']:
        operation_mode = 'append'  # 默认使用追加模式
    
    # 在替换模式下，scores可以为空（用于删除所有数据）
    if operation_mode == 'replace' and (not scores or not isinstance(scores, list)):
        scores = []
    elif operation_mode == 'append' and (not scores or not isinstance(scores, list)):
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '成绩明细列表不能为空' }

    print(f"[save_group_scores] 开始保存小组成绩 - class_id={class_id}, exam_name={exam_name_norm}, term={term}, operation_mode={operation_mode}, scores数量={len(scores) if scores else 0}")
    app_logger.info(f"[save_group_scores] 开始保存小组成绩 - class_id={class_id}, exam_name={exam_name_norm}, term={term}, operation_mode={operation_mode}, scores数量={len(scores) if scores else 0}")
    
    connection = get_db_connection()
    if connection is None:
        error_msg = "Save group scores failed: Database connection error."
        print(f"[save_group_scores] 错误: {error_msg}")
        app_logger.error(error_msg)
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': '数据库连接失败' }

    print(f"[save_group_scores] 数据库连接成功，开始事务")
    app_logger.info(f"[save_group_scores] 数据库连接成功，开始事务")
    try:
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # 1. 插入或获取小组成绩表头
        # 约定：class_id + term 唯一定位一张小组成绩表；exam_name 仅展示字段，不参与定位
        print(f"[save_group_scores] 查询小组成绩表头 - class_id={class_id}, term={term}（忽略exam_name定位：{exam_name_norm}）")
        app_logger.info(f"[save_group_scores] 查询小组成绩表头 - class_id={class_id}, term={term}（忽略exam_name定位：{exam_name_norm}）")
        cursor.execute(
            "SELECT id, excel_file_url, exam_name "
            "FROM ta_group_score_header "
            "WHERE class_id = %s AND ((%s IS NULL AND term IS NULL) OR term = %s) "
            "ORDER BY created_at DESC LIMIT 1",
            (class_id, term, term)
        )
        header_row = cursor.fetchone()
        print(f"[save_group_scores] 查询小组成绩表头结果: {header_row}")
        app_logger.info(f"[save_group_scores] 查询小组成绩表头结果: {header_row}")

        # 收集需要保留的字段（来自其他Excel文件）
        keep_fields_from_excel_urls = set()
        current_excel_filenames = set()
        if excel_files and isinstance(excel_files, list):
            for ef in excel_files:
                fn = ef.get('filename') or ef.get('name') or ef.get('file_name')
                if fn:
                    current_excel_filenames.add(fn)
        elif excel_file_name:
            current_excel_filenames.add(excel_file_name)

        if header_row is None:
            # 插入新表头
            print(f"[save_group_scores] ========== 插入新小组成绩表头 ==========")
            app_logger.info(f"[save_group_scores] ========== 插入新小组成绩表头 ==========")

            # exam_name 允许不传，但列为 NOT NULL，这里给默认展示名
            exam_name_to_store = exam_name_norm or "小组成绩"
            
            # 处理Excel文件URL（类似学生成绩接口）
            final_excel_file_url = None
            if excel_files and isinstance(excel_files, list) and len(excel_files) > 0:
                url_dict = {}
                for ef in excel_files:
                    fn = ef.get('filename') or ef.get('name') or ef.get('file_name')
                    if not fn:
                        continue
                    # 如果URL为空但excel_file_url有值，使用excel_file_url
                    file_url = ef.get('url', '')
                    if not file_url and excel_file_url:
                        file_url = excel_file_url
                        print(f"[save_group_scores] ✅ 创建表头时使用excel_file_url填充excel_files中的URL: {fn} -> {file_url}")
                        app_logger.info(f"[save_group_scores] ✅ 创建表头时使用excel_file_url填充excel_files中的URL: {fn} -> {file_url}")
                    url_dict[fn] = {
                        'url': file_url,
                        'description': ef.get('description', ''),
                        'fields': ef.get('fields', []) or []
                    }
                final_excel_file_url = json.dumps(url_dict, ensure_ascii=False)
            elif excel_file_url:
                if excel_file_name:
                    file_info = {
                        'url': excel_file_url,
                        'description': excel_file_description if excel_file_description else '',
                        'fields': []
                    }
                    url_dict = {excel_file_name: file_info}
                else:
                    timestamp = int(time.time())
                    file_info = {
                        'url': excel_file_url,
                        'description': excel_file_description if excel_file_description else '',
                        'fields': []
                    }
                    url_dict = {f"excel_file_{timestamp}": file_info}
                final_excel_file_url = json.dumps(url_dict, ensure_ascii=False)
            
            insert_header_sql = (
                "INSERT INTO ta_group_score_header (class_id, exam_name, term, remark, excel_file_url, created_at) "
                "VALUES (%s, %s, %s, %s, %s, NOW())"
            )
            cursor.execute(insert_header_sql, (class_id, exam_name_to_store, term, remark, final_excel_file_url))
            score_header_id = cursor.lastrowid
            print(f"[save_group_scores] ✅ 插入小组成绩表头成功 - score_header_id={score_header_id}")
            app_logger.info(f"[save_group_scores] ✅ 插入小组成绩表头成功 - score_header_id={score_header_id}")
        else:
            score_header_id = header_row['id']
            print(f"[save_group_scores] ========== 小组成绩表头已存在，准备更新 ==========")
            app_logger.info(f"[save_group_scores] ========== 小组成绩表头已存在，准备更新 ==========")
            
            # 更新表头信息（类似学生成绩接口的Excel文件处理逻辑）
            update_fields = []
            update_values = []
            # exam_name 不再参与定位：如果客户端传了，则作为展示字段更新
            if exam_name_norm is not None:
                update_fields.append("exam_name = %s")
                update_values.append(exam_name_norm)
            if remark is not None:
                update_fields.append("remark = %s")
                update_values.append(remark)
            
            # 处理Excel文件URL更新（参考学生成绩接口的实现）
            if (excel_files and isinstance(excel_files, list) and len(excel_files) > 0) or excel_file_url:
                existing_excel_file_url = header_row.get('excel_file_url')
                url_dict = {}
                if existing_excel_file_url:
                    try:
                        existing_dict = json.loads(existing_excel_file_url)
                        if isinstance(existing_dict, dict):
                            for filename, value in existing_dict.items():
                                if isinstance(value, dict):
                                    url_dict[filename] = {
                                        'url': value.get('url', ''),
                                        'description': value.get('description', ''),
                                        'fields': value.get('fields', []) or []
                                    }
                                    # 如果是其他Excel文件的字段，需要保留
                                    if filename not in current_excel_filenames:
                                        keep_fields_from_excel_urls.update(url_dict[filename]['fields'])
                                else:
                                    url_dict[filename] = {
                                        'url': value,
                                        'description': '',
                                        'fields': []
                                    }
                                    if filename not in current_excel_filenames:
                                        keep_fields_from_excel_urls.update(url_dict[filename]['fields'])
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                # 更新或添加新的文件信息
                if excel_files and isinstance(excel_files, list) and len(excel_files) > 0:
                    for ef in excel_files:
                        fn = ef.get('filename') or ef.get('name') or ef.get('file_name')
                        if not fn:
                            continue
                        # 如果URL为空但excel_file_url有值，使用excel_file_url
                        file_url = ef.get('url', '')
                        if not file_url and excel_file_url:
                            file_url = excel_file_url
                            print(f"[save_group_scores] ✅ 使用excel_file_url填充excel_files中的URL: {fn} -> {file_url}")
                            app_logger.info(f"[save_group_scores] ✅ 使用excel_file_url填充excel_files中的URL: {fn} -> {file_url}")
                        url_dict[fn] = {
                            'url': file_url,
                            'description': ef.get('description', ''),
                            'fields': ef.get('fields', []) or []
                        }
                elif excel_file_url:
                    if excel_file_name:
                        url_dict[excel_file_name] = {
                            'url': excel_file_url,
                            'description': excel_file_description if excel_file_description else '',
                            'fields': []
                        }
                    else:
                        timestamp = int(time.time())
                        default_key = f"excel_file_{timestamp}"
                        url_dict[default_key] = {
                            'url': excel_file_url,
                            'description': excel_file_description if excel_file_description else '',
                            'fields': []
                        }
                
                updated_excel_file_url = json.dumps(url_dict, ensure_ascii=False)
                update_fields.append("excel_file_url = %s")
                update_values.append(updated_excel_file_url)
            
            if update_fields:
                update_values.append(score_header_id)
                update_sql = f"UPDATE ta_group_score_header SET {', '.join(update_fields)}, updated_at = NOW() WHERE id = %s"
                cursor.execute(update_sql, tuple(update_values))
                print(f"[save_group_scores] ✅ UPDATE执行成功，影响行数: {cursor.rowcount}")
                app_logger.info(f"[save_group_scores] ✅ UPDATE执行成功，影响行数: {cursor.rowcount}")

        # 2. 处理字段定义（类似学生成绩接口，但小组成绩不需要字段定义表，直接使用scores_json）
        print(f"[save_group_scores] ========== 收到scores数据 ==========")
        print(f"[save_group_scores] scores数量: {len(scores)}")
        app_logger.info(f"[save_group_scores] 收到scores数据: {json.dumps(scores, ensure_ascii=False, indent=2) if scores else '[]'}")
        
        # 3. 在替换模式下，删除不在新数据中的学生
        deleted_student_count = 0
        if operation_mode == 'replace':
            new_student_keys = set()
            for score_item in scores:
                student_name = score_item.get('student_name', '').strip()
                student_id = score_item.get('student_id')
                if student_name:
                    new_student_keys.add((student_name, student_id))
            
            cursor.execute(
                "SELECT id, student_name, student_id FROM ta_group_score_detail WHERE score_header_id = %s",
                (score_header_id,)
            )
            existing_students = cursor.fetchall()
            
            students_to_delete = []
            for student in existing_students:
                student_name = student.get('student_name', '').strip()
                student_id = student.get('student_id')
                student_key = (student_name, student_id)
                if student_key not in new_student_keys:
                    students_to_delete.append(student['id'])
            
            if students_to_delete:
                delete_student_sql = "DELETE FROM ta_group_score_detail WHERE id = %s"
                for student_id_to_delete in students_to_delete:
                    cursor.execute(delete_student_sql, (student_id_to_delete,))
                    deleted_student_count += 1
                print(f"[save_group_scores] 替换模式下删除学生完成 - 删除{deleted_student_count}个学生")
                app_logger.info(f"[save_group_scores] 替换模式下删除学生完成 - 删除{deleted_student_count}个学生")
        
        # 4. 计算小组总分（按小组分组计算）
        group_totals = {}  # {group_name: total_score}
        if scores:
            for score_item in scores:
                group_name = score_item.get('group_name', '').strip()
                if not group_name:
                    continue
                
                # 计算个人总分
                total_score = score_item.get('总分') or score_item.get('total_score')
                if total_score is None:
                    # 自动计算总分（所有数字字段的和）
                    total_score = 0.0
                    for key, value in score_item.items():
                        if key not in ['group_name', 'student_id', 'student_name', 'group_total_score', '总分', 'total_score']:
                            if isinstance(value, (int, float)):
                                total_score += float(value)
                    if total_score == 0.0:
                        total_score = None
                
                # 累加小组总分
                if total_score is not None:
                    if group_name not in group_totals:
                        group_totals[group_name] = 0.0
                    group_totals[group_name] += float(total_score)
        
        # 5. 批量插入或更新成绩明细
        print(f"[save_group_scores] 开始插入/更新成绩明细 - score_header_id={score_header_id}, operation_mode={operation_mode}, 待处理数量={len(scores)}")
        app_logger.info(f"[save_group_scores] 开始插入/更新成绩明细 - score_header_id={score_header_id}, operation_mode={operation_mode}, 待处理数量={len(scores)}")
        
        insert_detail_sql = (
            "INSERT INTO ta_group_score_detail "
            "(score_header_id, group_name, student_id, student_name, scores_json, field_source_json, total_score, group_total_score) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        
        # 当前上传中涉及的字段集合（用于替换模式时保留其他excel的字段）
        upload_field_set = set()
        for score_item in scores:
            for key in score_item.keys():
                if key not in ['group_name', 'student_id', 'student_name', 'group_total_score', '总分', 'total_score', 'total']:
                    upload_field_set.add(key)
        
        # 确定当前Excel文件名（如果有多个，使用第一个）
        current_excel_filename = None
        if current_excel_filenames:
            current_excel_filename = list(current_excel_filenames)[0]  # 使用第一个文件名
        elif excel_file_name:
            current_excel_filename = excel_file_name
        
        for idx, score_item in enumerate(scores):
            group_name = score_item.get('group_name', '').strip()
            student_id = score_item.get('student_id')
            student_name = score_item.get('student_name', '').strip()
            if not student_name:
                skipped_count += 1
                print(f"[save_group_scores] 跳过第{idx+1}条记录：缺少学生姓名 - score_item={score_item}")
                app_logger.warning(f"[save_group_scores] 跳过第{idx+1}条记录：缺少学生姓名 - score_item={score_item}")
                continue
            
            # 检查该学生是否已有成绩记录
            check_sql = (
                "SELECT id, scores_json, field_source_json FROM ta_group_score_detail "
                "WHERE score_header_id = %s AND student_name = %s "
                "AND (%s IS NULL OR student_id = %s) "
                "LIMIT 1"
            )
            cursor.execute(check_sql, (score_header_id, student_name, student_id, student_id))
            existing_record = cursor.fetchone()
            
            # 构建JSON对象（使用复合键名：字段名_Excel文件名）
            # 这样可以支持同一字段名来自不同Excel文件的情况
            scores_json = {}
            field_source_json = {}  # 记录字段名到Excel文件名的映射（可选）
            total_score = None
            for key, value in score_item.items():
                if key not in ['group_name', 'student_id', 'student_name', 'group_total_score', '总分', 'total_score', 'total']:
                    if value is not None:
                        # 使用复合键名（字段名_Excel文件名）来保存，避免同名字段覆盖
                        composite_key = f"{key}_{current_excel_filename}" if current_excel_filename else key
                        
                        try:
                            if isinstance(value, (int, float)):
                                scores_json[composite_key] = float(value)
                            elif isinstance(value, str) and value.strip():
                                scores_json[composite_key] = float(value.strip())
                            else:
                                scores_json[composite_key] = value
                        except (ValueError, TypeError):
                            scores_json[composite_key] = value
                        
                        # 记录字段来源映射（如果同一字段名来自多个Excel，使用数组）
                        if key in field_source_json:
                            existing_source = field_source_json[key]
                            if isinstance(existing_source, str):
                                field_source_json[key] = [existing_source, current_excel_filename] if current_excel_filename else existing_source
                            elif isinstance(existing_source, list):
                                if current_excel_filename and current_excel_filename not in existing_source:
                                    existing_source.append(current_excel_filename)
                        else:
                            field_source_json[key] = current_excel_filename if current_excel_filename else key
                
                # 检查是否为总分字段
                if (key == '总分' or key == 'total_score') and value is not None:
                    try:
                        total_score = float(value)
                    except (ValueError, TypeError):
                        pass
            
            # 如果没有找到总分字段，自动计算总分
            if total_score is None:
                total_score = 0.0
                for key, value in scores_json.items():
                    if isinstance(value, (int, float)):
                        total_score += float(value)
                if total_score == 0.0:
                    total_score = None
            
            # 获取小组总分
            group_total_score = group_totals.get(group_name)
            
            # 在追加模式下，如果记录已存在，合并JSON数据（保留旧字段，添加新字段）
            # 在替换模式下，仅替换本次上传涉及的字段，保留其他excel的字段
            if operation_mode == 'append' and existing_record and existing_record.get('scores_json'):
                try:
                    existing_json = json.loads(existing_record['scores_json']) if isinstance(existing_record['scores_json'], str) else existing_record['scores_json']
                    # 合并时，复合键名不会冲突（因为包含Excel文件名）
                    merged_json = {**existing_json, **scores_json}
                    scores_json = merged_json
                    print(f"[save_group_scores] 合并已有成绩数据 - student_name={student_name}, 旧字段数={len(existing_json)}, 新字段数={len(scores_json)}")
                    app_logger.info(f"[save_group_scores] 合并已有成绩数据 - student_name={student_name}")
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"[save_group_scores] 解析已有JSON失败，使用新数据 - student_name={student_name}, error={e}")
                    app_logger.warning(f"[save_group_scores] 解析已有JSON失败，使用新数据 - student_name={student_name}, error={e}")
            elif operation_mode == 'replace' and existing_record and existing_record.get('scores_json'):
                try:
                    existing_json = json.loads(existing_record['scores_json']) if isinstance(existing_record['scores_json'], str) else existing_record['scores_json']
                    # 替换模式下，仅删除当前Excel文件的字段，保留其他Excel的字段
                    # 保留不以当前Excel文件名结尾的字段（即其他Excel的字段）
                    if current_excel_filename:
                        preserved = {k: v for k, v in existing_json.items() if not k.endswith(f"_{current_excel_filename}")}
                    else:
                        # 如果没有Excel文件名，保留不在本次上传字段集中的字段
                        preserved = {k: v for k, v in existing_json.items() if k not in upload_field_set}
                    scores_json = {**preserved, **scores_json}
                    print(f"[save_group_scores] 替换模式保留其他excel字段 - student_name={student_name}, 保留字段数={len(preserved)}, 新字段数={len(scores_json)}")
                    app_logger.info(f"[save_group_scores] 替换模式保留其他excel字段 - student_name={student_name}")
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"[save_group_scores] 解析已有JSON失败，使用新数据 - student_name={student_name}, error={e}")
                    app_logger.warning(f"[save_group_scores] 解析已有JSON失败，使用新数据 - student_name={student_name}, error={e}")
            
            # 在追加模式下，合并字段来源映射
            existing_field_source_json = {}
            if existing_record and existing_record.get('field_source_json'):
                try:
                    existing_field_source_json = json.loads(existing_record['field_source_json']) if isinstance(existing_record['field_source_json'], str) else existing_record['field_source_json']
                except (json.JSONDecodeError, TypeError):
                    existing_field_source_json = {}
            
            if operation_mode == 'append' and existing_record:
                # 合并字段来源映射
                field_source_json = {**existing_field_source_json, **field_source_json}
            elif operation_mode == 'replace' and existing_record:
                # 替换模式下，删除当前Excel的字段映射，保留其他的
                preserved_sources = {k: v for k, v in existing_field_source_json.items() 
                                    if (isinstance(v, str) and v != current_excel_filename) or
                                       (isinstance(v, list) and current_excel_filename not in v)}
                field_source_json = {**preserved_sources, **field_source_json}
            
            # 将scores_json和field_source_json转换为JSON字符串
            scores_json_str = json.dumps(scores_json, ensure_ascii=False)
            field_source_json_str = json.dumps(field_source_json, ensure_ascii=False) if field_source_json else None
            
            is_update = existing_record is not None
            action = "更新" if is_update else "插入"
            print(f"[save_group_scores] {action}第{idx+1}条成绩 - student_name={student_name}, group_name={group_name}, scores_json={scores_json_str}, field_source_json={field_source_json_str}, total_score={total_score}, group_total_score={group_total_score}")
            app_logger.info(f"[save_group_scores] {action}第{idx+1}条成绩 - student_name={student_name}, group_name={group_name}, total_score={total_score}, group_total_score={group_total_score}")
            
            try:
                if existing_record:
                    update_detail_sql = (
                        "UPDATE ta_group_score_detail "
                        "SET group_name = %s, scores_json = %s, field_source_json = %s, total_score = %s, group_total_score = %s, updated_at = NOW() "
                        "WHERE id = %s"
                    )
                    cursor.execute(update_detail_sql, (
                        group_name,
                        scores_json_str,
                        field_source_json_str,
                        total_score,
                        group_total_score,
                        existing_record['id']
                    ))
                    updated_count += 1
                else:
                    cursor.execute(insert_detail_sql, (
                        score_header_id,
                        group_name,
                        student_id,
                        student_name,
                        scores_json_str,
                        field_source_json_str,
                        total_score,
                        group_total_score
                    ))
                    inserted_count += 1
            except Exception as insert_error:
                print(f"[save_group_scores] 第{idx+1}条成绩{action}失败 - student_name={student_name}, error={insert_error}")
                app_logger.error(f"[save_group_scores] 第{idx+1}条成绩{action}失败 - student_name={student_name}, error={insert_error}", exc_info=True)
                raise

        print(f"[save_group_scores] 成绩明细处理完成 - 插入={inserted_count}, 更新={updated_count}, 跳过={skipped_count}, 总计={len(scores)}")
        app_logger.info(f"[save_group_scores] 成绩明细处理完成 - 插入={inserted_count}, 更新={updated_count}, 跳过={skipped_count}, 总计={len(scores)}")
        
        print(f"[save_group_scores] 开始提交事务")
        app_logger.info(f"[save_group_scores] 开始提交事务")
        connection.commit()
        total_processed = inserted_count + updated_count
        print(f"[save_group_scores] 事务提交成功 - score_header_id={score_header_id}, 插入={inserted_count}, 更新={updated_count}, 删除学生={deleted_student_count}, 总计={total_processed}")
        app_logger.info(f"[save_group_scores] 事务提交成功 - score_header_id={score_header_id}, 插入={inserted_count}, 更新={updated_count}, 删除学生={deleted_student_count}, 总计={total_processed}")
        return { 
            'success': True, 
            'score_header_id': score_header_id, 
            'inserted_count': inserted_count, 
            'updated_count': updated_count,
            'deleted_student_count': deleted_student_count,
            'message': '保存成功' 
        }
    except mysql.connector.Error as e:
        if connection and connection.is_connected():
            print(f"[save_group_scores] 数据库错误，回滚事务 - error={e}")
            app_logger.error(f"[save_group_scores] 数据库错误，回滚事务 - error={e}")
            connection.rollback()
        else:
            print(f"[save_group_scores] 数据库错误，连接已断开 - error={e}")
            app_logger.error(f"[save_group_scores] 数据库错误，连接已断开 - error={e}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[save_group_scores] 错误堆栈:\n{traceback_str}")
        app_logger.error(f"[save_group_scores] 错误堆栈:\n{traceback_str}")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'数据库错误: {e}' }
    except Exception as e:
        if connection and connection.is_connected():
            print(f"[save_group_scores] 未知错误，回滚事务 - error={e}")
            app_logger.error(f"[save_group_scores] 未知错误，回滚事务 - error={e}")
            connection.rollback()
        else:
            print(f"[save_group_scores] 未知错误，连接已断开 - error={e}")
            app_logger.error(f"[save_group_scores] 未知错误，连接已断开 - error={e}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[save_group_scores] 错误堆栈:\n{traceback_str}")
        app_logger.error(f"[save_group_scores] 错误堆栈:\n{traceback_str}")
        return { 'success': False, 'score_header_id': None, 'inserted_count': 0, 'message': f'未知错误: {e}' }
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after saving group scores.")


