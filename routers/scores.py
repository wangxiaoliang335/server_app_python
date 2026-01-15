import asyncio
import datetime
import json
import os
import re
import time
import traceback
from typing import Any, Dict, List, Optional

import mysql.connector
from fastapi import APIRouter, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from common import app_logger, safe_json_response
from db import get_db_connection
from services.scores import parse_excel_file_url, save_student_scores, save_group_scores
from services.oss_upload import upload_excel_to_oss


router = APIRouter()

def _normalize_excel_table_name(raw: Optional[str]) -> Optional[str]:
    """
    table_name（客户端传入）指的是“Excel 表/文件名”的基名：
    - 允许传：期中成绩单 / 期中成绩单.xlsx
    - 统一去掉末尾 .xlsx
    """
    if raw is None:
        return None
    name = str(raw).strip()
    if not name:
        return None
    if name.lower().endswith(".xlsx"):
        name = name[:-5]
    return name.strip() or None


def _excel_filename_matches_table_name(excel_filename: str, table_name_base: str) -> bool:
    """
    判断某个 excel_filename 是否属于客户端传入的 table_name：
    - table_name=期中成绩单
    - 精确匹配：仅匹配 期中成绩单.xlsx
    """
    if not excel_filename:
        return False
    base = excel_filename
    if base.lower().endswith(".xlsx"):
        base = base[:-5]
    base = str(base).strip()
    return base == table_name_base


@router.post("/student-scores/save")
async def api_save_student_scores(request: Request):
    """
    保存学生成绩表
    支持两种请求格式：
    1. application/json: 直接发送JSON数据
    2. multipart/form-data: 包含data字段（JSON字符串）和excel_file字段（Excel文件）
    
    请求体 JSON (或multipart中的data字段):
    {
      "class_id": "class_1001",
      "exam_name": "期中考试",  // 可选（仅用于展示，不再作为定位条件；不传则使用默认值“成绩”）
      "term": "2025-2026-1",  // 可选
      "remark": "备注信息",    // 可选
      "excel_file_name": "成绩表.xlsx",  // 可选，Excel文件名
      "excel_file_url": "https://...",  // 可选，Excel文件URL（如果不传文件）
      "excel_file_description": "这是期中考试的成绩统计表",  // 可选，Excel文件说明
      "operation_mode": "replace",  // 可选，操作模式："append"（追加，默认）或 "replace"（替换）
      "fields": [  // 可选，字段定义列表（用于替换模式，支持删除列和调整顺序）
        {
          "field_name": "语文",
          "field_type": "number",
          "field_order": 1,
          "is_total": 0
        },
        {
          "field_name": "数学",
          "field_type": "number",
          "field_order": 2,
          "is_total": 0
        }
      ],
      "scores": [
        {
          "student_id": "2024001",    // 可选
          "student_name": "张三",
          "chinese": 100,
          "math": 89,
          "english": 95,
          "total_score": 284           // 可选，会自动计算
        },
        {
          "student_name": "李四",
          "chinese": 90,
          "math": 78
          // total_score 会自动计算为 168
        }
      ]
    }
    """
    # ========== 打印客户端发送的原始消息 ==========
    print("=" * 80)
    print("[student-scores/save] ========== 收到客户端请求 ==========")
    print(f"[student-scores/save] 请求方法: {request.method}")
    print(f"[student-scores/save] 请求URL: {request.url}")
    print(f"[student-scores/save] 请求路径: {request.url.path}")
    print(f"[student-scores/save] 查询参数: {request.url.query}")
    
    # 打印请求头
    print(f"[student-scores/save] 请求头:")
    for header_name, header_value in request.headers.items():
        # 对于敏感信息（如 Authorization），只显示部分内容
        if header_name.lower() in ['authorization', 'cookie']:
            if header_value:
                masked_value = header_value[:20] + "..." if len(header_value) > 20 else header_value[:20]
                print(f"  {header_name}: {masked_value}")
            else:
                print(f"  {header_name}: (空)")
        else:
            print(f"  {header_name}: {header_value}")
    
    app_logger.info(f"[student-scores/save] 收到请求: {request.method} {request.url.path}")
    
    data = None
    excel_file = None
    excel_file_name = None
    excel_file_url = None
    excel_files = None
    
    # 检查Content-Type
    content_type = request.headers.get("content-type", "").lower()
    print(f"[student-scores/save] Content-Type: {content_type}")
    app_logger.info(f"[student-scores/save] Content-Type: {content_type}")
    
    if "multipart/form-data" in content_type:
        # 处理multipart/form-data格式
        print(f"[student-scores/save] ========== 处理 multipart/form-data 格式 ==========")
        app_logger.info(f"[student-scores/save] 处理 multipart/form-data 格式")
        try:
            form_data = await request.form()
            
            # 打印表单字段
            print(f"[student-scores/save] 表单字段列表:")
            for field_name in form_data:
                field_value = form_data.get(field_name)
                if field_name == "excel_file":
                    # 对于文件字段，只打印文件信息，不打印二进制内容
                    if hasattr(field_value, 'filename'):
                        filename = getattr(field_value, 'filename', None)
                        file_size = None
                        if hasattr(field_value, 'size'):
                            file_size = getattr(field_value, 'size', None)
                        print(f"  {field_name}: [文件] filename={filename}, size={file_size}, type={type(field_value).__name__}")
                        app_logger.info(f"[student-scores/save] 表单字段 {field_name}: 文件 {filename}, size={file_size}")
                    else:
                        print(f"  {field_name}: [文件] type={type(field_value).__name__}")
                        app_logger.info(f"[student-scores/save] 表单字段 {field_name}: 文件对象 {type(field_value).__name__}")
                else:
                    # 对于非文件字段，打印完整内容（但限制长度）
                    value_str = str(field_value)
                    if len(value_str) > 1000:
                        value_preview = value_str[:1000] + "...(截断)"
                        print(f"  {field_name}: {value_preview} [长度: {len(value_str)}]")
                        app_logger.info(f"[student-scores/save] 表单字段 {field_name}: 长度 {len(value_str)} 字符")
                    else:
                        print(f"  {field_name}: {value_str}")
                        app_logger.info(f"[student-scores/save] 表单字段 {field_name}: {value_str}")
            
            # 获取JSON数据（从data字段）
            data_str = form_data.get("data")
            if not data_str:
                return safe_json_response({'message': 'multipart请求中缺少data字段', 'code': 400}, status_code=400)
            
            # 打印原始 data 字段（JSON字符串）
            print(f"[student-scores/save] 原始 data 字段（JSON字符串）:")
            if data_str:
                if isinstance(data_str, str):
                    print(f"{data_str}")
                    print(f"[student-scores/save] data 字段长度: {len(data_str)} 字符")
                    app_logger.info(f"[student-scores/save] data 字段长度: {len(data_str)} 字符")
                    app_logger.info(f"[student-scores/save] data 字段内容: {data_str}")
                else:
                    print(f"{str(data_str)}")
                    app_logger.info(f"[student-scores/save] data 字段类型: {type(data_str)}")
                    app_logger.info(f"[student-scores/save] data 字段内容: {str(data_str)}")
            else:
                print("  (空)")
            
            # 解析JSON字符串（form_data.get返回的可能是字符串）
            if isinstance(data_str, str):
                data = json.loads(data_str)
            else:
                # 如果不是字符串，尝试转换为字符串再解析
                data = json.loads(str(data_str))
            
            # 打印解析后的JSON数据
            print(f"[student-scores/save] 解析后的 JSON 数据:")
            try:
                data_json_str = json.dumps(data, ensure_ascii=False, indent=2)
                print(f"{data_json_str}")
                print(f"[student-scores/save] JSON 数据长度: {len(data_json_str)} 字符")
                app_logger.info(f"[student-scores/save] 解析后的 JSON 数据: {data_json_str}")
            except Exception as e:
                print(f"[student-scores/save] 无法格式化JSON数据: {e}")
                print(f"[student-scores/save] 原始数据: {str(data)}")
                app_logger.error(f"[student-scores/save] 无法格式化JSON数据: {e}, 原始数据: {str(data)}")
            
            # 获取Excel文件（可选）
            excel_file = form_data.get("excel_file")
            excel_file_url = None
            print(f"[student-scores/save] ========== 开始处理Excel文件 ==========")
            app_logger.info(f"[student-scores/save] ========== 开始处理Excel文件 ==========")
            print(f"[student-scores/save] excel_file是否存在: {excel_file is not None}")
            app_logger.info(f"[student-scores/save] excel_file是否存在: {excel_file is not None}")
            if excel_file:
                print(f"[student-scores/save] excel_file类型: {type(excel_file)}")
                print(f"[student-scores/save] excel_file类型名称: {type(excel_file).__name__}")
                print(f"[student-scores/save] excel_file模块: {type(excel_file).__module__}")
                app_logger.info(f"[student-scores/save] excel_file类型: {type(excel_file)}, 类型名称: {type(excel_file).__name__}, 模块: {type(excel_file).__module__}")
                
                # 检查是否是UploadFile类型（支持fastapi.UploadFile和starlette.datastructures.UploadFile）
                is_upload_file = isinstance(excel_file, UploadFile) or type(excel_file).__name__ == 'UploadFile'
                print(f"[student-scores/save] isinstance(excel_file, UploadFile): {isinstance(excel_file, UploadFile)}")
                print(f"[student-scores/save] type(excel_file).__name__ == 'UploadFile': {type(excel_file).__name__ == 'UploadFile'}")
                print(f"[student-scores/save] is_upload_file: {is_upload_file}")
                app_logger.info(f"[student-scores/save] is_upload_file检查结果: {is_upload_file}")
                
                if is_upload_file:
                    filename_value = getattr(excel_file, 'filename', None)
                    print(f"[student-scores/save] excel_file.filename值: {filename_value}")
                    print(f"[student-scores/save] excel_file.filename类型: {type(filename_value)}")
                    app_logger.info(f"[student-scores/save] excel_file.filename值: {filename_value}, 类型: {type(filename_value)}")
                    
                    # 优先使用客户端JSON中的excel_file_name字段
                    # 如果JSON中没有，再使用excel_file.filename
                    # 如果都没有，使用默认名称
                    excel_file_name = None
                    if data:
                        excel_file_name = data.get('excel_file_name')
                        if excel_file_name:
                            print(f"[student-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                            app_logger.info(f"[student-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                    
                    # 如果JSON中没有，尝试使用excel_file.filename
                    if not excel_file_name and filename_value:
                        excel_file_name = filename_value
                        print(f"[student-scores/save] ✅ 使用excel_file.filename: {excel_file_name}")
                        app_logger.info(f"[student-scores/save] ✅ 使用excel_file.filename: {excel_file_name}")
                    
                    # 如果都没有，使用默认名称
                    if not excel_file_name:
                        timestamp = int(time.time())
                        excel_file_name = f"excel_{timestamp}.xlsx"
                        print(f"[student-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                        app_logger.warning(f"[student-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                    
                    print(f"[student-scores/save] 📋 最终使用的文件名: {excel_file_name}")
                    app_logger.info(f"[student-scores/save] 📋 最终使用的文件名: {excel_file_name}")
                    
                    # 读取Excel文件内容
                    try:
                        print(f"[student-scores/save] 📖 开始读取Excel文件内容...")
                        app_logger.info(f"[student-scores/save] 📖 开始读取Excel文件内容...")
                        excel_content = await excel_file.read()
                        print(f"[student-scores/save] ✅ Excel文件读取成功，文件大小: {len(excel_content)} bytes")
                        app_logger.info(f"[student-scores/save] ✅ Excel文件读取成功，文件大小: {len(excel_content)} bytes")
                        
                        # 生成OSS对象名称（使用时间戳和文件名避免冲突）
                        timestamp = int(time.time())
                        file_ext = os.path.splitext(excel_file_name)[1] or '.xlsx'
                        oss_object_name = f"excel/student-scores/{timestamp}_{excel_file_name}"
                        print(f"[student-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                        app_logger.info(f"[student-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                        
                        # 上传到阿里云OSS
                        print(f"[student-scores/save] ☁️ 开始上传Excel文件到阿里云OSS...")
                        print(f"[student-scores/save] ☁️ OSS对象名称: {oss_object_name}")
                        app_logger.info(f"[student-scores/save] ☁️ 开始上传Excel文件到阿里云OSS: {oss_object_name}")
                        excel_file_url = upload_excel_to_oss(excel_content, oss_object_name)
                        
                        print(f"[student-scores/save] ========== Excel文件上传结果 ==========")
                        app_logger.info(f"[student-scores/save] ========== Excel文件上传结果 ==========")
                        print(f"[student-scores/save] upload_excel_to_oss返回值类型: {type(excel_file_url)}")
                        app_logger.info(f"[student-scores/save] upload_excel_to_oss返回值类型: {type(excel_file_url)}")
                        print(f"[student-scores/save] upload_excel_to_oss返回值: {excel_file_url}")
                        app_logger.info(f"[student-scores/save] upload_excel_to_oss返回值: {excel_file_url}")
                        
                        if excel_file_url:
                            print(f"[student-scores/save] ✅ Excel文件上传成功！")
                            print(f"[student-scores/save] ✅ 阿里云OSS URL: {excel_file_url}")
                            app_logger.info(f"[student-scores/save] ✅ Excel文件上传成功，OSS URL: {excel_file_url}")
                        else:
                            print(f"[student-scores/save] ❌ Excel文件上传失败，返回值为None或空")
                            app_logger.warning(f"[student-scores/save] ❌ Excel文件上传失败，返回值为None或空")
                    except Exception as e:
                        error_msg = f'读取或上传Excel文件时出错: {str(e)}'
                        print(f"[student-scores/save] ❌ 错误: {error_msg}")
                        app_logger.error(f"[student-scores/save] ❌ {error_msg}", exc_info=True)
                        import traceback
                        traceback_str = traceback.format_exc()
                        print(f"[student-scores/save] ❌ 错误堆栈:\n{traceback_str}")
                        app_logger.error(f"[student-scores/save] ❌ 错误堆栈:\n{traceback_str}")
                        # 继续处理，不阻止成绩数据保存
                else:
                    # 即使不是标准的UploadFile类型，也尝试处理（可能是其他类型的文件对象）
                    print(f"[student-scores/save] ⚠️ Excel文件类型检查未通过，但尝试继续处理")
                    print(f"[student-scores/save] ⚠️ 文件对象类型: {type(excel_file)}, 类型名称: {type(excel_file).__name__}")
                    app_logger.warning(f"[student-scores/save] ⚠️ Excel文件类型检查未通过，但尝试继续处理，类型: {type(excel_file)}")
                    
                    # 尝试从JSON数据中获取文件名
                    excel_file_name = None
                    if data:
                        excel_file_name = data.get('excel_file_name')
                        if excel_file_name:
                            print(f"[student-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                            app_logger.info(f"[student-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                    
                    # 如果JSON中没有，使用默认名称
                    if not excel_file_name:
                        timestamp = int(time.time())
                        excel_file_name = f"excel_{timestamp}.xlsx"
                        print(f"[student-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                        app_logger.warning(f"[student-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                    
                    # 尝试读取文件内容（如果对象有read方法）
                    try:
                        if hasattr(excel_file, 'read'):
                            print(f"[student-scores/save] 📖 尝试读取文件内容（使用read方法）...")
                            app_logger.info(f"[student-scores/save] 📖 尝试读取文件内容（使用read方法）...")
                            if asyncio.iscoroutinefunction(excel_file.read):
                                excel_content = await excel_file.read()
                            else:
                                excel_content = excel_file.read()
                            
                            print(f"[student-scores/save] ✅ 文件读取成功，文件大小: {len(excel_content)} bytes")
                            app_logger.info(f"[student-scores/save] ✅ 文件读取成功，文件大小: {len(excel_content)} bytes")
                            
                            # 生成OSS对象名称
                            timestamp = int(time.time())
                            oss_object_name = f"excel/student-scores/{timestamp}_{excel_file_name}"
                            print(f"[student-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                            app_logger.info(f"[student-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                            
                            # 上传到阿里云OSS
                            print(f"[student-scores/save] ☁️ 开始上传Excel文件到阿里云OSS...")
                            app_logger.info(f"[student-scores/save] ☁️ 开始上传Excel文件到阿里云OSS: {oss_object_name}")
                            excel_file_url = upload_excel_to_oss(excel_content, oss_object_name)
                            
                            if excel_file_url:
                                print(f"[student-scores/save] ✅ Excel文件上传成功，OSS URL: {excel_file_url}")
                                app_logger.info(f"[student-scores/save] ✅ Excel文件上传成功，OSS URL: {excel_file_url}")
                            else:
                                print(f"[student-scores/save] ❌ Excel文件上传失败")
                                app_logger.warning(f"[student-scores/save] ❌ Excel文件上传失败")
                        else:
                            print(f"[student-scores/save] ❌ 文件对象没有read方法，无法读取")
                            app_logger.error(f"[student-scores/save] ❌ 文件对象没有read方法，无法读取")
                    except Exception as e:
                        error_msg = f'读取或上传Excel文件时出错: {str(e)}'
                        print(f"[student-scores/save] ❌ 错误: {error_msg}")
                        app_logger.error(f"[student-scores/save] ❌ {error_msg}", exc_info=True)
                        import traceback
                        traceback_str = traceback.format_exc()
                        print(f"[student-scores/save] ❌ 错误堆栈:\n{traceback_str}")
                        app_logger.error(f"[student-scores/save] ❌ 错误堆栈:\n{traceback_str}")
            else:
                print(f"[student-scores/save] ℹ️ 未提供Excel文件")
                app_logger.info(f"[student-scores/save] ℹ️ 未提供Excel文件")
            print(f"[student-scores/save] ========== Excel文件处理完成 ==========")
            print(f"[student-scores/save] 最终excel_file_url值: {excel_file_url}")
            app_logger.info(f"[student-scores/save] ========== Excel文件处理完成，最终excel_file_url值: {excel_file_url} ==========")
            
        except json.JSONDecodeError as e:
            error_msg = f'无法解析multipart中的JSON数据: {str(e)}'
            print(f"[student-scores/save] 错误: {error_msg}")
            app_logger.warning(f"[student-scores/save] {error_msg}")
            return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
        except Exception as e:
            error_msg = f'处理multipart请求时出错: {str(e)}'
            print(f"[student-scores/save] 错误: {error_msg}")
            app_logger.warning(f"[student-scores/save] {error_msg}")
            return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    else:
        # 处理application/json格式
        print(f"[student-scores/save] ========== 处理 application/json 格式 ==========")
        app_logger.info(f"[student-scores/save] 处理 application/json 格式")
        try:
            # 先读取原始请求体
            body_bytes = await request.body()
            body_str = body_bytes.decode('utf-8') if body_bytes else ""
            
            # 打印原始JSON字符串
            print(f"[student-scores/save] 原始 JSON 请求体:")
            if body_str:
                print(f"{body_str}")
                print(f"[student-scores/save] JSON 请求体长度: {len(body_str)} 字符")
                app_logger.info(f"[student-scores/save] JSON 请求体长度: {len(body_str)} 字符")
                app_logger.info(f"[student-scores/save] 原始 JSON 请求体: {body_str}")
            else:
                print("  (空)")
                app_logger.warning(f"[student-scores/save] JSON 请求体为空")
            
            # 解析JSON
            if body_str:
                try:
                    data = json.loads(body_str)
                except json.JSONDecodeError as json_err:
                    error_msg = f'JSON 解析失败: {str(json_err)}'
                    print(f"[student-scores/save] 错误: {error_msg}")
                    app_logger.warning(f"[student-scores/save] {error_msg}")
                    return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
            else:
                error_msg = 'JSON 请求体为空'
                print(f"[student-scores/save] 错误: {error_msg}")
                app_logger.warning(f"[student-scores/save] {error_msg}")
                return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
            
            # 打印解析后的JSON数据
            print(f"[student-scores/save] 解析后的 JSON 数据:")
            try:
                data_json_str = json.dumps(data, ensure_ascii=False, indent=2)
                print(f"{data_json_str}")
                print(f"[student-scores/save] JSON 数据长度: {len(data_json_str)} 字符")
                app_logger.info(f"[student-scores/save] 解析后的 JSON 数据: {data_json_str}")
            except Exception as e:
                print(f"[student-scores/save] 无法格式化JSON数据: {e}")
                print(f"[student-scores/save] 原始数据: {str(data)}")
                app_logger.error(f"[student-scores/save] 无法格式化JSON数据: {e}, 原始数据: {str(data)}")
                
        except Exception as e:
            error_msg = f'无效的 JSON 请求体: {str(e)}'
            print(f"[student-scores/save] 错误: {error_msg}")
            app_logger.warning(f"[student-scores/save] {error_msg}")
            return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    
    if not data:
        return safe_json_response({'message': '无法解析请求数据', 'code': 400}, status_code=400)
    
    # 打印接收到的数据（完整JSON）
    print(f"[student-scores/save] ========== 最终接收到的完整 JSON 数据 ==========")
    try:
        full_data_json = json.dumps(data, ensure_ascii=False, indent=2)
        print(full_data_json)
        print(f"[student-scores/save] JSON 数据总长度: {len(full_data_json)} 字符")
        app_logger.info(f"[student-scores/save] 最终接收到的完整 JSON 数据: {full_data_json}")
    except Exception as e:
        print(f"[student-scores/save] 无法格式化完整JSON数据: {e}")
        print(f"[student-scores/save] 原始数据: {str(data)}")
        app_logger.error(f"[student-scores/save] 无法格式化完整JSON数据: {e}, 原始数据: {str(data)}")
    print(f"[student-scores/save] ========== 完整 JSON 数据打印结束 ==========")
    if excel_file_name:
        print(f"[student-scores/save] Excel文件名: {excel_file_name}")
    
    # 从JSON数据中提取excel_file_name（如果multipart中没有提供）
    if not excel_file_name:
        excel_file_name = data.get('excel_file_name')
    
    # 从JSON数据中提取excel_file_url（如果multipart中没有提供）
    print(f"[student-scores/save] 📋 检查是否需要从JSON数据中提取excel_file_url...")
    app_logger.info(f"[student-scores/save] 📋 检查是否需要从JSON数据中提取excel_file_url...")
    print(f"[student-scores/save] 当前excel_file_url值: {excel_file_url}")
    app_logger.info(f"[student-scores/save] 当前excel_file_url值: {excel_file_url}")
    if not excel_file_url:
        json_excel_file_url = data.get('excel_file_url')
        print(f"[student-scores/save] 从JSON数据中获取excel_file_url: {json_excel_file_url}")
        app_logger.info(f"[student-scores/save] 从JSON数据中获取excel_file_url: {json_excel_file_url}")
        excel_file_url = json_excel_file_url
    else:
        print(f"[student-scores/save] ✅ excel_file_url已有值，无需从JSON数据中提取")
        app_logger.info(f"[student-scores/save] ✅ excel_file_url已有值，无需从JSON数据中提取")
    
    # 从JSON数据中提取excel_file_description
    excel_file_description = data.get('excel_file_description')
    
    class_id = data.get('class_id')
    exam_name = data.get('exam_name')
    term = data.get('term')
    remark = data.get('remark')
    scores = data.get('scores', [])
    operation_mode = data.get('operation_mode', 'append')  # 默认为追加模式
    fields = data.get('fields')  # 字段定义列表（可选）
    excel_files = data.get('excel_files')  # 多个excel文件信息（可选）

    # 调试：打印客户端传入的excel文件信息
    try:
        print(f"[student-scores/save] 接收到的excel_files: {json.dumps(excel_files, ensure_ascii=False) if excel_files else None}")
        print(f"[student-scores/save] 接收到的excel_file_url: {excel_file_url}")
        app_logger.info(f"[student-scores/save] 接收到的excel_files: {json.dumps(excel_files, ensure_ascii=False) if excel_files else None}")
        app_logger.info(f"[student-scores/save] 接收到的excel_file_url: {excel_file_url}")
    except Exception as log_err:
        print(f"[student-scores/save] ⚠️ 打印excel文件信息时出错: {log_err}")
        app_logger.warning(f"[student-scores/save] 打印excel文件信息时出错: {log_err}")

    print(f"[student-scores/save] ========== 解析后的参数 ==========")
    print(f"[student-scores/save] class_id: {class_id}")
    print(f"[student-scores/save] exam_name: {exam_name}")
    print(f"[student-scores/save] term: {term}")
    print(f"[student-scores/save] operation_mode: {operation_mode}")
    print(f"[student-scores/save] excel_file_name: {excel_file_name}")
    print(f"[student-scores/save] excel_file_url: {excel_file_url}")
    print(f"[student-scores/save] excel_file_description: {excel_file_description}")
    print(f"[student-scores/save] excel_file_url类型: {type(excel_file_url)}")
    print(f"[student-scores/save] excel_file_url是否为空: {not excel_file_url}")
    print(f"[student-scores/save] excel_files数量: {len(excel_files) if excel_files else 0}")
    print(f"[student-scores/save] fields数量: {len(fields) if fields else 0}")
    print(f"[student-scores/save] scores数量: {len(scores) if scores else 0}")
    app_logger.info(f"[student-scores/save] 解析后的参数: class_id={class_id}, exam_name={exam_name}, term={term}, operation_mode={operation_mode}, excel_file_name={excel_file_name}, excel_file_url={excel_file_url}, excel_file_description={excel_file_description}, excel_files数量={len(excel_files) if excel_files else 0}, fields数量={len(fields) if fields else 0}, scores数量={len(scores) if scores else 0}")

    if not class_id:
        error_msg = '缺少必要参数 class_id'
        print(f"[student-scores/save] 错误: {error_msg}")
        app_logger.warning(f"[student-scores/save] {error_msg}")
        return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)

    print(f"[student-scores/save] ========== 准备调用 save_student_scores 函数 ==========")
    app_logger.info(f"[student-scores/save] ========== 准备调用 save_student_scores 函数 ==========")
    print(f"[student-scores/save] 📤 传递给save_student_scores的参数:")
    print(f"[student-scores/save]   - class_id: {class_id}")
    print(f"[student-scores/save]   - exam_name: {exam_name}")
    print(f"[student-scores/save]   - term: {term}")
    print(f"[student-scores/save]   - remark: {remark}")
    print(f"[student-scores/save]   - operation_mode: {operation_mode}")
    print(f"[student-scores/save]   - excel_file_url: {excel_file_url}")
    print(f"[student-scores/save]   - excel_file_name: {excel_file_name}")
    print(f"[student-scores/save]   - excel_file_description: {excel_file_description}")
    print(f"[student-scores/save]   - excel_files数量: {len(excel_files) if excel_files else 0}")
    print(f"[student-scores/save]   - fields数量: {len(fields) if fields else 0}")
    print(f"[student-scores/save]   - scores数量: {len(scores) if scores else 0}")
    app_logger.info(f"[student-scores/save] 📤 传递给save_student_scores的参数: class_id={class_id}, exam_name={exam_name}, term={term}, remark={remark}, operation_mode={operation_mode}, excel_file_url={excel_file_url}, excel_file_name={excel_file_name}, excel_file_description={excel_file_description}, excel_files数量={len(excel_files) if excel_files else 0}, fields数量={len(fields) if fields else 0}, scores数量={len(scores) if scores else 0}")
    # 如果上传了excel文件且excel_files里对应文件url为空，则回填上传得到的excel_file_url
    if excel_files and excel_file_url and excel_file_name:
        for ef in excel_files:
            fn = ef.get('filename') or ef.get('name') or ef.get('file_name')
            if fn == excel_file_name and (not ef.get('url')):
                ef['url'] = excel_file_url

    result = save_student_scores(
        class_id=class_id,
        exam_name=exam_name,
        term=term,
        remark=remark,
        scores=scores,
        excel_file_url=excel_file_url,
        excel_file_name=excel_file_name,
        excel_file_description=excel_file_description,
        excel_files=excel_files,
        operation_mode=operation_mode,
        fields=fields
    )

    print(f"[student-scores/save] save_student_scores 返回结果: {result}")
    app_logger.info(f"[student-scores/save] save_student_scores 返回结果: {result}")

    if result.get('success'):
        return safe_json_response({'message': '保存成功', 'code': 200, 'data': result})
    else:
        return safe_json_response({'message': result.get('message', '保存失败'), 'code': 500}, status_code=500)


@router.get("/student-scores")
async def api_get_student_scores(
    request: Request,
    class_id: Optional[str] = Query(None, description="班级ID（与 group_id 二选一；也可两者都传）"),
    group_id: Optional[str] = Query(None, description="班级群ID（与 class_id 二选一；也可两者都传）"),
    exam_name: Optional[str] = Query(None, description="考试名称（兼容字段：不再作为查询条件）"),
    term: Optional[str] = Query(None, description="学期，可选")
):
    """
    查询学生成绩表
    返回 JSON:
    {
      "message": "查询成功",
      "code": 200,
      "data": {
        "headers": [
          {
            "id": 1,
            "class_id": "class_1001",
            "exam_name": "期中考试",
            "term": "2025-2026-1",
            "remark": "...",
            "excel_file_url": [
              {
                "filename": "期中成绩单.xlsx",
                "url": "https://..."
              },
              {
                "filename": "学生体质统计表.xlsx",
                "url": "https://..."
              }
            ],
            "created_at": "...",
            "updated_at": "...",
            "fields": [...],
            "scores": [
              {
                "id": 1,
                "student_id": "2024001",
                "student_name": "张三",
                "scores_json_full": {
                  "语文_期中成绩单.xlsx": 100,
                  "数学_期中成绩单.xlsx": 89
                }
              },
              ...
            ]
          },
          ...
        ]
      }
    }
    """
    # 兼容：class_id / group_id 二选一；也可两者都传
    class_id = str(class_id).strip() if class_id is not None else None
    group_id = str(group_id).strip() if group_id is not None else None

    if not class_id and not group_id:
        error_response = {"message": "缺少必要参数：class_id 或 group_id", "code": 400}
        try:
            error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            print(f"[student-scores] ========== 返回的 JSON 结果（缺少参数）==========")
            print(error_json)
            print(f"[student-scores] ========== JSON 结果打印结束 ==========")
            app_logger.warning(f"[student-scores] 返回的 JSON 结果（缺少参数）: {json.dumps(error_response, ensure_ascii=False)}")
        except Exception as json_error:
            print(f"[student-scores] 打印 JSON 时出错: {json_error}")
        return safe_json_response(error_response, status_code=400)

    connection = get_db_connection()
    if connection is None:
        error_response = {'message': '数据库连接失败', 'code': 500}
        try:
            error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            print(f"[student-scores] ========== 返回的 JSON 结果（数据库连接失败）==========")
            print(error_json)
            print(f"[student-scores] ========== JSON 结果打印结束 ==========")
            app_logger.error(f"[student-scores] 返回的 JSON 结果（数据库连接失败）: {json.dumps(error_response, ensure_ascii=False)}")
        except Exception as json_error:
            print(f"[student-scores] 打印 JSON 时出错: {json_error}")
        return safe_json_response(error_response, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)

        # 如果传了 group_id，优先尝试从 groups 表解析出对应的 classid
        # - 解析成功：用解析出的 classid 查询成绩
        # - 解析失败（查不到/为空）：兜底把 group_id 当成 class_id（兼容老数据/老约定）
        resolved_class_id: Optional[str] = class_id
        if group_id:
            group_classid: Optional[str] = None
            try:
                cursor.execute("SELECT classid FROM `groups` WHERE group_id = %s LIMIT 1", (group_id,))
                row = cursor.fetchone() or {}
                raw_classid = row.get("classid")
                if raw_classid is not None:
                    group_classid = str(raw_classid).strip()
            except Exception:
                group_classid = None

            if group_classid:
                if resolved_class_id and resolved_class_id != group_classid:
                    error_response = {"message": "参数不一致：class_id 与 group_id 对应的 classid 不一致", "code": 400}
                    try:
                        error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
                        print(f"[student-scores] ========== 返回的 JSON 结果（参数不一致）==========")
                        print(error_json)
                        print(f"[student-scores] ========== JSON 结果打印结束 ==========")
                        app_logger.warning(f"[student-scores] 返回的 JSON 结果（参数不一致）: {json.dumps(error_response, ensure_ascii=False)}")
                    except Exception as json_error:
                        print(f"[student-scores] 打印 JSON 时出错: {json_error}")
                    return safe_json_response(error_response, status_code=400)
                resolved_class_id = group_classid
            else:
                # 无法解析 classid：如果同时传了 class_id，则无法校验一致性，除非两者相同
                if resolved_class_id and resolved_class_id != group_id:
                    error_response = {"message": "无法从 group_id 解析班级ID(classid)，请只传 class_id，或先在 groups 表补齐 classid", "code": 400}
                    try:
                        error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
                        print(f"[student-scores] ========== 返回的 JSON 结果（无法解析classid）==========")
                        print(error_json)
                        print(f"[student-scores] ========== JSON 结果打印结束 ==========")
                        app_logger.warning(f"[student-scores] 返回的 JSON 结果（无法解析classid）: {json.dumps(error_response, ensure_ascii=False)}")
                    except Exception as json_error:
                        print(f"[student-scores] 打印 JSON 时出错: {json_error}")
                    return safe_json_response(error_response, status_code=400)
                resolved_class_id = resolved_class_id or group_id

        if not resolved_class_id:
            error_response = {"message": "无法确定班级ID（class_id）", "code": 400}
            try:
                error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
                print(f"[student-scores] ========== 返回的 JSON 结果（无法确定班级ID）==========")
                print(error_json)
                print(f"[student-scores] ========== JSON 结果打印结束 ==========")
                app_logger.warning(f"[student-scores] 返回的 JSON 结果（无法确定班级ID）: {json.dumps(error_response, ensure_ascii=False)}")
            except Exception as json_error:
                print(f"[student-scores] 打印 JSON 时出错: {json_error}")
            return safe_json_response(error_response, status_code=400)

        # 统一用 resolved_class_id 走原有逻辑
        class_id = resolved_class_id
        
        # 查询成绩表头
        # 约定：class_id + term 能定位一张成绩表；exam_name 仅作为展示字段保留，不作为定位条件
        if term is not None:
            cursor.execute(
                "SELECT id, class_id, exam_name, term, remark, excel_file_url, created_at, updated_at "
                "FROM ta_student_score_header "
                "WHERE class_id = %s AND term = %s "
                "ORDER BY created_at DESC, updated_at DESC "
                "LIMIT 1",
                (class_id, term)
            )
        else:
            cursor.execute(
                "SELECT id, class_id, exam_name, term, remark, excel_file_url, created_at, updated_at "
                "FROM ta_student_score_header "
                "WHERE class_id = %s "
                "ORDER BY created_at DESC, updated_at DESC",
                (class_id,)
            )
        
        headers = cursor.fetchall() or []
        
        # 查询每个表头的成绩明细和字段定义
        result_headers = []
        for header in headers:
            score_header_id = header['id']
            
            # 查询字段定义（包含 excel_filename）
            cursor.execute(
                "SELECT field_name, excel_filename, field_type, field_order, is_total "
                "FROM ta_student_score_field "
                "WHERE score_header_id = %s "
                "ORDER BY excel_filename ASC, field_order ASC",
                (score_header_id,)
            )
            fields = cursor.fetchall() or []
            # 补充 score_header_id，便于前端直接使用
            for f in fields:
                if isinstance(f, dict) and 'score_header_id' not in f:
                    f['score_header_id'] = score_header_id
            # 收集所有字段名（可能有重复，来自不同Excel）
            field_names = list({f['field_name'] for f in fields})  # 去重
            
            # 构建字段名到Excel文件名的映射（用于解析复合键名）
            field_excel_map = {}
            for f in fields:
                field_name = f['field_name']
                excel_filename = f.get('excel_filename', '')
                if field_name not in field_excel_map:
                    field_excel_map[field_name] = []
                if excel_filename and excel_filename not in field_excel_map[field_name]:
                    field_excel_map[field_name].append(excel_filename)
            
            # 查询成绩明细（包含 field_source_json）
            cursor.execute(
                "SELECT id, student_id, student_name, scores_json, field_source_json, comments_json, total_score "
                "FROM ta_student_score_detail "
                "WHERE score_header_id = %s "
                "ORDER BY total_score DESC, student_name ASC",
                (score_header_id,)
            )
            score_rows = cursor.fetchall() or []
            
            # 解析JSON字段并构建成绩列表
            scores = []
            for row in score_rows:
                # 仅返回结构化成绩信息（scores_json_full + 注释等），避免重复下发
                score_dict = {
                    'id': row['id'],
                    'score_header_id': score_header_id,
                    'student_id': row.get('student_id'),
                    'student_name': row.get('student_name')
                }
                
                # 解析成绩JSON字段（处理复合键名：字段名_Excel文件名）
                if row.get('scores_json'):
                    try:
                        if isinstance(row['scores_json'], str):
                            scores_data = json.loads(row['scores_json'])
                        else:
                            scores_data = row['scores_json']
                        # 仅返回完整的scores_json（包含所有复合键名），由客户端按复合键名取值
                        score_dict['scores_json_full'] = scores_data
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"[api_get_student_scores] 解析JSON失败: {e}, scores_json={row.get('scores_json')}")
                        app_logger.warning(f"[api_get_student_scores] 解析JSON失败: {e}")
                
                # 解析注释JSON字段（支持复合键名：字段名_Excel文件名）
                comments_dict = {}
                if row.get('comments_json'):
                    try:
                        if isinstance(row['comments_json'], str):
                            comments_dict = json.loads(row['comments_json'])
                        else:
                            comments_dict = row['comments_json']
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"[api_get_student_scores] 解析注释JSON失败: {e}, comments_json={row.get('comments_json')}")
                        app_logger.warning(f"[api_get_student_scores] 解析注释JSON失败: {e}")
                
                # 仅返回去重后的 comments：
                # - 如果存在复合键（字段名_Excel文件名），则只返回复合键，避免与简单键重复
                # - 否则（旧数据）保留原样
                filtered_comments = comments_dict
                try:
                    if isinstance(comments_dict, dict) and comments_dict:
                        all_excel_filenames = {fn for fns in field_excel_map.values() for fn in fns if fn}
                        has_composite = False

                        if all_excel_filenames:
                            for k in comments_dict.keys():
                                if any(k.endswith(f"_{fn}") for fn in all_excel_filenames):
                                    has_composite = True
                                    break
                        else:
                            has_composite = any('_' in k for k in comments_dict.keys())

                        if has_composite:
                            if all_excel_filenames:
                                filtered_comments = {
                                    k: v
                                    for k, v in comments_dict.items()
                                    if any(k.endswith(f"_{fn}") for fn in all_excel_filenames)
                                }
                            else:
                                filtered_comments = {k: v for k, v in comments_dict.items() if '_' in k}
                except Exception:
                    filtered_comments = comments_dict

                score_dict['comments'] = filtered_comments
                
                scores.append(score_dict)
            
            # 解析excel_file_url为数组格式
            excel_file_url_raw = header.get('excel_file_url')
            excel_file_urls = parse_excel_file_url(excel_file_url_raw)
            
            # 转换 datetime 为字符串（用于 JSON 序列化）
            created_at = header.get('created_at')
            if created_at and isinstance(created_at, datetime.datetime):
                created_at = created_at.strftime("%Y-%m-%d %H:%M:%S")
            updated_at = header.get('updated_at')
            if updated_at and isinstance(updated_at, datetime.datetime):
                updated_at = updated_at.strftime("%Y-%m-%d %H:%M:%S")
            
            header_dict = {
                'id': header['id'],
                'score_header_id': header['id'],
                'class_id': header['class_id'],
                'exam_name': header['exam_name'],
                'term': header.get('term'),
                'remark': header.get('remark'),
                'excel_file_url': excel_file_urls,  # 返回数组格式
                'created_at': created_at,
                'updated_at': updated_at,
                'fields': fields,  # 字段定义列表
                'scores': scores
            }
            result_headers.append(header_dict)

        # 转换 Decimal 类型为 float（用于 JSON 序列化）
        from decimal import Decimal
        def convert_for_json(obj):
            """递归转换 Decimal 和 datetime 类型为 JSON 可序列化的类型"""
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, datetime.datetime):
                return obj.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_for_json(item) for item in obj]
            return obj
        
        # 转换所有数据以确保 JSON 序列化正常
        result_headers = convert_for_json(result_headers)
        
        response_data = {
            'message': '查询成功',
            'code': 200,
            'data': {'headers': result_headers}
        }
        
        # 打印返回的 JSON 结果
        try:
            response_json = json.dumps(response_data, ensure_ascii=False, indent=2)
            print(f"[student-scores] ========== 返回的 JSON 结果 ==========")
            print(response_json)
            print(f"[student-scores] ========== JSON 结果打印结束 ==========")
            #app_logger.info(f"[student-scores] 返回的 JSON 结果: {json.dumps(response_data, ensure_ascii=False)}")
        except Exception as json_error:
            print(f"[student-scores] 打印 JSON 时出错: {json_error}")
            app_logger.warning(f"[student-scores] 打印 JSON 时出错: {json_error}")
        
        return safe_json_response(response_data)
    except mysql.connector.Error as e:
        error_response = {'message': '数据库错误', 'code': 500}
        app_logger.error(f"Database error during api_get_student_scores: {e}")
        try:
            error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            print(f"[student-scores] ========== 返回的 JSON 结果（数据库错误）==========")
            print(error_json)
            print(f"[student-scores] ========== JSON 结果打印结束 ==========")
            app_logger.error(f"[student-scores] 返回的 JSON 结果（数据库错误）: {json.dumps(error_response, ensure_ascii=False)}")
        except Exception as json_error:
            print(f"[student-scores] 打印 JSON 时出错: {json_error}")
        return safe_json_response(error_response, status_code=500)
    except Exception as e:
        error_response = {'message': '未知错误', 'code': 500}
        app_logger.error(f"Unexpected error during api_get_student_scores: {e}")
        try:
            error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            print(f"[student-scores] ========== 返回的 JSON 结果（未知错误）==========")
            print(error_json)
            print(f"[student-scores] ========== JSON 结果打印结束 ==========")
            app_logger.error(f"[student-scores] 返回的 JSON 结果（未知错误）: {json.dumps(error_response, ensure_ascii=False)}")
        except Exception as json_error:
            print(f"[student-scores] 打印 JSON 时出错: {json_error}")
        return safe_json_response(error_response, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching student scores.")


@router.get("/student-scores/get")
async def api_get_student_score(
    class_id: Optional[str] = Query(None, description="班级ID（与 group_id 二选一；也可两者都传）"),
    group_id: Optional[str] = Query(None, description="班级群ID（与 class_id 二选一；也可两者都传）"),
    exam_name: Optional[str] = Query(None, description="考试名称（兼容字段：不再作为查询条件）"),
    term: str = Query(..., description="学期，如'2025-2026-1'")
):
    """
    查询学生成绩表（单个，如果查询到多个则返回最新的）
    返回 JSON:
    {
      "message": "查询成功",
      "code": 200,
      "data": {
        "id": 1,
        "class_id": "class_1001",
        "exam_name": "期中考试",
        "term": "2025-2026-1",
        "remark": "...",
        "excel_file_url": [
          {
            "filename": "期中成绩单.xlsx",
            "url": "https://..."
          },
          {
            "filename": "学生体质统计表.xlsx",
            "url": "https://..."
          }
        ],
        "created_at": "...",
        "updated_at": "...",
        "fields": [...],
        "scores": [
          {
            "id": 1,
            "student_id": "2024001",
            "student_name": "张三",
            "scores_json_full": {
              "语文_期中成绩单.xlsx": 100,
              "数学_期中成绩单.xlsx": 89
            }
          },
          ...
        ]
      }
    }
    """
    class_id = str(class_id).strip() if class_id is not None else None
    group_id = str(group_id).strip() if group_id is not None else None

    if not class_id and not group_id:
        return safe_json_response({"message": "缺少必要参数：class_id 或 group_id", "code": 400}, status_code=400)

    print("=" * 80)
    print(f"[student-scores/get] 收到查询请求 - class_id: {class_id}, group_id: {group_id}, term: {term}（忽略exam_name: {exam_name}）")
    app_logger.info(f"[student-scores/get] 收到查询请求 - class_id: {class_id}, group_id: {group_id}, term: {term}（忽略exam_name: {exam_name}）")
    
    connection = get_db_connection()
    if connection is None:
        print("[student-scores/get] 错误: 数据库连接失败")
        app_logger.error(f"[student-scores/get] 数据库连接失败 - class_id: {class_id}, group_id: {group_id}, exam_name: {exam_name}, term: {term}")
        return safe_json_response({'message': '数据库连接失败', 'code': 500}, status_code=500)
    
    print("[student-scores/get] 数据库连接成功")
    app_logger.info(f"[student-scores/get] 数据库连接成功 - class_id: {class_id}")

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # group_id -> classid 映射（同 /student-scores）
        resolved_class_id: Optional[str] = class_id
        if group_id:
            group_classid: Optional[str] = None
            try:
                cursor.execute("SELECT classid FROM `groups` WHERE group_id = %s LIMIT 1", (group_id,))
                row = cursor.fetchone() or {}
                raw_classid = row.get("classid")
                if raw_classid is not None:
                    group_classid = str(raw_classid).strip()
            except Exception:
                group_classid = None

            if group_classid:
                if resolved_class_id and resolved_class_id != group_classid:
                    return safe_json_response(
                        {"message": "参数不一致：class_id 与 group_id 对应的 classid 不一致", "code": 400},
                        status_code=400,
                    )
                resolved_class_id = group_classid
            else:
                if resolved_class_id and resolved_class_id != group_id:
                    return safe_json_response(
                        {"message": "无法从 group_id 解析班级ID(classid)，请只传 class_id，或先在 groups 表补齐 classid", "code": 400},
                        status_code=400,
                    )
                resolved_class_id = resolved_class_id or group_id

        if not resolved_class_id:
            return safe_json_response({"message": "无法确定班级ID（class_id）", "code": 400}, status_code=400)

        class_id = resolved_class_id
        
        # 查询成绩表头，如果有多个则按创建时间降序排列，取最新的
        print(f"[student-scores/get] 查询成绩表头...")
        app_logger.info(f"[student-scores/get] 开始查询成绩表头 - class_id: {class_id}, exam_name: {exam_name}, term: {term}")
        cursor.execute(
            "SELECT id, class_id, exam_name, term, remark, excel_file_url, created_at, updated_at "
            "FROM ta_student_score_header "
            "WHERE class_id = %s AND term = %s "
            "ORDER BY created_at DESC, updated_at DESC "
            "LIMIT 1",
            (class_id, term)
        )
        
        header = cursor.fetchone()
        
        if not header:
            print(f"[student-scores/get] 未找到成绩表 - class_id: {class_id}, exam_name: {exam_name}, term: {term}")
            app_logger.warning(f"[student-scores/get] 未找到成绩表 - class_id: {class_id}, exam_name: {exam_name}, term: {term}")
            return safe_json_response({
                'message': '没有找到数据：未找到成绩表',
                'code': 200,
                'data': None
            }, status_code=200)
        
        print(f"[student-scores/get] 找到成绩表头 - id: {header['id']}, created_at: {header.get('created_at')}")
        app_logger.info(f"[student-scores/get] 找到成绩表头 - id: {header['id']}, class_id: {class_id}, exam_name: {exam_name}, term: {term}, created_at: {header.get('created_at')}")
        
        # 查询字段定义（包含 excel_filename）
        score_header_id = header['id']
        print(f"[student-scores/get] 查询字段定义 - score_header_id: {score_header_id}")
        app_logger.info(f"[student-scores/get] 开始查询字段定义 - score_header_id: {score_header_id}")
        cursor.execute(
            "SELECT field_name, excel_filename, field_type, field_order, is_total "
            "FROM ta_student_score_field "
            "WHERE score_header_id = %s "
            "ORDER BY excel_filename ASC, field_order ASC",
            (score_header_id,)
        )
        fields = cursor.fetchall() or []
        # 补充 score_header_id，便于前端直接使用
        for f in fields:
            if isinstance(f, dict) and 'score_header_id' not in f:
                f['score_header_id'] = score_header_id
        # 收集所有字段名（可能有重复，来自不同Excel）
        field_names = list({f['field_name'] for f in fields})  # 去重
        
        # 构建字段名到Excel文件名的映射（用于解析复合键名）
        field_excel_map = {}
        for f in fields:
            field_name = f['field_name']
            excel_filename = f.get('excel_filename', '')
            if field_name not in field_excel_map:
                field_excel_map[field_name] = []
            if excel_filename and excel_filename not in field_excel_map[field_name]:
                field_excel_map[field_name].append(excel_filename)
        
        # 查询成绩明细（包含 field_source_json）
        print(f"[student-scores/get] 查询成绩明细 - score_header_id: {score_header_id}")
        app_logger.info(f"[student-scores/get] 开始查询成绩明细 - score_header_id: {score_header_id}")
        cursor.execute(
            "SELECT id, student_id, student_name, scores_json, field_source_json, comments_json, total_score "
            "FROM ta_student_score_detail "
            "WHERE score_header_id = %s "
            "ORDER BY total_score DESC, student_name ASC",
            (score_header_id,)
        )
        score_rows = cursor.fetchall() or []
        
        print(f"[student-scores/get] 查询到 {len(score_rows)} 条成绩明细")
        app_logger.info(f"[student-scores/get] 查询到 {len(score_rows)} 条成绩明细 - score_header_id: {score_header_id}")
        
        # 解析JSON字段并构建成绩列表
        scores = []
        for row in score_rows:
            # 仅返回结构化成绩信息（scores_json_full + 注释等），避免重复下发
            score_dict = {
                'id': row['id'],
                'score_header_id': score_header_id,
                'student_id': row.get('student_id'),
                'student_name': row.get('student_name')
            }
            
            # 解析成绩JSON字段（处理复合键名：字段名_Excel文件名）
            if row.get('scores_json'):
                try:
                    if isinstance(row['scores_json'], str):
                        scores_data = json.loads(row['scores_json'])
                    else:
                        scores_data = row['scores_json']
                    # 仅返回完整的scores_json（包含所有复合键名），由客户端按复合键名取值
                    score_dict['scores_json_full'] = scores_data
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"[api_get_student_score] 解析JSON失败: {e}, scores_json={row.get('scores_json')}")
                    app_logger.warning(f"[api_get_student_score] 解析JSON失败: {e}")
            
            # 解析注释JSON字段（支持复合键名：字段名_Excel文件名）
            comments_dict = {}
            if row.get('comments_json'):
                try:
                    if isinstance(row['comments_json'], str):
                        comments_dict = json.loads(row['comments_json'])
                    else:
                        comments_dict = row['comments_json']
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"[api_get_student_score] 解析注释JSON失败: {e}, comments_json={row.get('comments_json')}")
                    app_logger.warning(f"[api_get_student_score] 解析注释JSON失败: {e}")
            
            # 仅返回去重后的 comments（规则同 /student-scores）
            filtered_comments = comments_dict
            try:
                if isinstance(comments_dict, dict) and comments_dict:
                    all_excel_filenames = {fn for fns in field_excel_map.values() for fn in fns if fn}
                    has_composite = False

                    if all_excel_filenames:
                        for k in comments_dict.keys():
                            if any(k.endswith(f"_{fn}") for fn in all_excel_filenames):
                                has_composite = True
                                break
                    else:
                        has_composite = any('_' in k for k in comments_dict.keys())

                    if has_composite:
                        if all_excel_filenames:
                            filtered_comments = {
                                k: v
                                for k, v in comments_dict.items()
                                if any(k.endswith(f"_{fn}") for fn in all_excel_filenames)
                            }
                        else:
                            filtered_comments = {k: v for k, v in comments_dict.items() if '_' in k}
            except Exception:
                filtered_comments = comments_dict

            score_dict['comments'] = filtered_comments
            
            scores.append(score_dict)
        
        # 转换 Decimal 类型为 float（用于 JSON 序列化）
        from decimal import Decimal
        def convert_decimal(obj):
            """递归转换 Decimal 类型为 float"""
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, dict):
                return {k: convert_decimal(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_decimal(item) for item in obj]
            return obj
        
        # 转换成绩明细中的 Decimal 类型
        scores = convert_decimal(scores)
        
        # 转换 datetime 为字符串
        if header.get('created_at') and isinstance(header['created_at'], datetime.datetime):
            header['created_at'] = header['created_at'].strftime("%Y-%m-%d %H:%M:%S")
        if header.get('updated_at') and isinstance(header['updated_at'], datetime.datetime):
            header['updated_at'] = header['updated_at'].strftime("%Y-%m-%d %H:%M:%S")
        
        # 转换 header 中的 Decimal 类型（如果有）
        header = convert_decimal(header)
        
        # 解析excel_file_url为数组格式
        excel_file_url_raw = header.get('excel_file_url')
        excel_file_urls = parse_excel_file_url(excel_file_url_raw)
        
        result = {
            'id': header['id'],
            'score_header_id': header['id'],
            'class_id': header['class_id'],
            'exam_name': header['exam_name'],
            'term': header.get('term'),
            'remark': header.get('remark'),
            'excel_file_url': excel_file_urls,  # 返回数组格式
            'created_at': header.get('created_at'),
            'updated_at': header.get('updated_at'),
            'fields': fields,  # 字段定义列表
            'scores': scores
        }
        
        print(f"[student-scores/get] 返回结果 - id: {result['id']}, scores_count: {len(scores)}")
        app_logger.info(f"[student-scores/get] 查询成功 - score_header_id: {result['id']}, class_id: {class_id}, exam_name: {exam_name}, term: {term}, scores_count: {len(scores)}")
        
        response_data = {
            'message': '查询成功',
            'code': 200,
            'data': result
        }
        
        # 打印返回的 JSON 结果
        try:
            response_json = json.dumps(response_data, ensure_ascii=False, indent=2)
            print(f"[student-scores/get] 返回的 JSON 结果:\n{response_json}")
            app_logger.info(f"[student-scores/get] 返回的 JSON 结果: {json.dumps(response_data, ensure_ascii=False)}")
        except Exception as json_error:
            print(f"[student-scores/get] 打印 JSON 时出错: {json_error}")
            app_logger.warning(f"[student-scores/get] 打印 JSON 时出错: {json_error}")
        
        print("=" * 80)
        
        return safe_json_response(response_data)
        
    except mysql.connector.Error as e:
        print(f"[student-scores/get] 数据库错误: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        app_logger.error(f"[student-scores/get] 数据库错误 - class_id: {class_id}, exam_name: {exam_name}, term: {term}, error: {e}\n{traceback_str}")
        return safe_json_response({'message': '数据库错误', 'code': 500}, status_code=500)
    except Exception as e:
        print(f"[student-scores/get] 未知错误: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[student-scores/get] 错误堆栈: {traceback_str}")
        app_logger.error(f"[student-scores/get] 未知错误 - class_id: {class_id}, exam_name: {exam_name}, term: {term}, error: {e}\n{traceback_str}")
        return safe_json_response({'message': '未知错误', 'code': 500}, status_code=500)
    finally:
        if cursor:
            cursor.close()
            print("[student-scores/get] 游标已关闭")
        if connection and connection.is_connected():
            connection.close()
            print("[student-scores/get] 数据库连接已关闭")
            app_logger.info(f"[student-scores/get] 数据库连接已关闭 - class_id: {class_id}")


@router.post("/student-scores/set-comment")
async def api_set_student_score_comment(request: Request):
    """
    设置特定学生特定属性的注释
    请求体 JSON:
    {
      "score_header_id": 1,              // 成绩表头ID（必需）
      "student_name": "张子晨",           // 学生姓名（必需）
      "student_id": "2024001",           // 学号（可选，如果提供会更精确匹配）
      "field_name": "数学",               // 字段名称（必需，如：数学、早读、语文等）
      "excel_filename": "期中成绩单.xlsx", // Excel文件名（可选，如果提供则使用复合键名保存）
      "comment": "需要加强练习"           // 注释内容（必需，如果要删除注释可以传空字符串）
    }
    注意：如果提供了 excel_filename，将使用复合键名（field_name_excel_filename）保存注释，
         这样可以支持不同Excel文件中相同字段名的注释不互相覆盖。
         为避免重复数据，本接口在存在 excel_filename 时**不会再额外写入简单字段名键**。
    """
    print("=" * 80)
    print("[student-scores/set-comment] ========== 收到设置注释请求 ==========")
    
    try:
        body = await request.json()
        print(f"[student-scores/set-comment] 客户端发送的消息: {body}")
        score_header_id = body.get('score_header_id')
        class_id = body.get('class_id')
        term = body.get('term')
        student_name = body.get('student_name')
        student_id = body.get('student_id')  # 可选
        field_name = body.get('field_name')
        excel_filename = body.get('excel_filename')  # 可选，如果提供则使用复合键名
        comment = body.get('comment')
        
        # 参数验证
        # 兼容：允许不传 score_header_id，改用 class_id + term 定位表头
        if not score_header_id:
            class_id = str(class_id).strip() if class_id is not None else ""
            if not class_id:
                return safe_json_response({'message': '缺少必需参数: score_header_id 或 class_id', 'code': 400}, status_code=400)
        
        if not student_name:
            return safe_json_response({
                'message': '缺少必需参数: student_name',
                'code': 400
            }, status_code=400)
        
        if not field_name:
            return safe_json_response({
                'message': '缺少必需参数: field_name',
                'code': 400
            }, status_code=400)
        
        if comment is None:
            return safe_json_response({
                'message': '缺少必需参数: comment',
                'code': 400
            }, status_code=400)
        
        print(f"[student-scores/set-comment] 参数 - score_header_id: {score_header_id}, class_id: {class_id}, term: {term}, student_name: {student_name}, student_id: {student_id}, field_name: {field_name}, excel_filename: {excel_filename}, comment: {comment}")
        app_logger.info(f"[student-scores/set-comment] 收到设置注释请求 - score_header_id: {score_header_id}, class_id: {class_id}, term: {term}, student_name: {student_name}, student_id: {student_id}, field_name: {field_name}, excel_filename: {excel_filename}")
        
        connection = get_db_connection()
        if connection is None:
            return safe_json_response({
                'message': '数据库连接失败',
                'code': 500
            }, status_code=500)
        
        cursor = connection.cursor(dictionary=True)

        # 如果没有提供 score_header_id，则尝试用 class_id + term 找到表头
        if not score_header_id:
            cursor.execute(
                "SELECT id FROM ta_student_score_header "
                "WHERE class_id = %s AND ((%s IS NULL AND term IS NULL) OR term = %s) "
                "ORDER BY created_at DESC, updated_at DESC "
                "LIMIT 1",
                (class_id, term, term),
            )
            header_row = cursor.fetchone()
            if not header_row:
                return safe_json_response({'message': '没有找到数据：未找到学生成绩表头（请确认 class_id/term）', 'code': 200}, status_code=200)
            score_header_id = header_row.get("id")
        
        # 如果没有提供 excel_filename，尝试从字段定义中查找
        if not excel_filename:
            cursor.execute(
                "SELECT excel_filename FROM ta_student_score_field "
                "WHERE score_header_id = %s AND field_name = %s "
                "LIMIT 1",
                (score_header_id, field_name)
            )
            field_result = cursor.fetchone()
            if field_result and field_result.get('excel_filename'):
                excel_filename = field_result['excel_filename']
                print(f"[student-scores/set-comment] 从字段定义中获取 excel_filename: {excel_filename}")
        
        # 确定使用的键名（如果提供了 excel_filename，使用复合键名）
        comment_key = f"{field_name}_{excel_filename}" if excel_filename else field_name
        
        # 查询学生成绩记录
        if student_id:
            cursor.execute(
                "SELECT id, comments_json FROM ta_student_score_detail "
                "WHERE score_header_id = %s AND student_name = %s AND student_id = %s "
                "LIMIT 1",
                (score_header_id, student_name, student_id)
            )
        else:
            cursor.execute(
                "SELECT id, comments_json FROM ta_student_score_detail "
                "WHERE score_header_id = %s AND student_name = %s "
                "LIMIT 1",
                (score_header_id, student_name)
            )
        
        record = cursor.fetchone()
        
        if not record:
            return safe_json_response({
                'message': f'没有找到数据：未找到学生成绩记录: {student_name}',
                'code': 200
            }, status_code=200)
        
        record_id = record['id']
        existing_comments_json = record.get('comments_json')
        
        # 解析现有的注释JSON
        if existing_comments_json:
            if isinstance(existing_comments_json, str):
                try:
                    comments_dict = json.loads(existing_comments_json)
                except json.JSONDecodeError:
                    comments_dict = {}
            else:
                comments_dict = existing_comments_json
        else:
            comments_dict = {}
        
        # 更新或添加注释
        # 规则：
        # - 有 excel_filename：只写入复合键名（field_name_excel_filename），并清理同名简单键 field_name（避免重复）
        # - 无 excel_filename：写入简单键 field_name
        if comment.strip():  # 如果注释不为空，则设置
            comments_dict[comment_key] = comment
            if excel_filename:
                # 清理历史兼容数据导致的重复键（如 "数学": "...", "数学_文件.xlsx": "..."）
                comments_dict.pop(field_name, None)
            else:
                comments_dict[field_name] = comment
        else:  # 如果注释为空字符串，则删除该字段的注释
            comments_dict.pop(comment_key, None)
            if excel_filename:
                # 同步清理简单键，避免残留重复
                comments_dict.pop(field_name, None)
            else:
                    comments_dict.pop(field_name, None)
        
        # 将更新后的字典转换为JSON字符串
        comments_json_str = json.dumps(comments_dict, ensure_ascii=False)
        
        # 更新数据库
        cursor.execute(
            "UPDATE ta_student_score_detail "
            "SET comments_json = %s, updated_at = NOW() "
            "WHERE id = %s",
            (comments_json_str, record_id)
        )
        
        connection.commit()
        
        print(f"[student-scores/set-comment] ✅ 注释设置成功 - record_id: {record_id}, field_name: {field_name}, comment: {comment}")
        app_logger.info(f"[student-scores/set-comment] ✅ 注释设置成功 - record_id: {record_id}, student_name: {student_name}, field_name: {field_name}, comment: {comment}")
        
        return safe_json_response({
            'message': '注释设置成功',
            'code': 200,
            'data': {
                'record_id': record_id,
                'student_name': student_name,
                'field_name': field_name,
                'comment': comment if comment.strip() else None,
                'comments_json': comments_dict
            }
        })
        
    except json.JSONDecodeError:
        error_msg = '请求体JSON格式错误'
        print(f"[student-scores/set-comment] ❌ {error_msg}")
        app_logger.error(f"[student-scores/set-comment] ❌ {error_msg}")
        return safe_json_response({
            'message': error_msg,
            'code': 400
        }, status_code=400)
    except mysql.connector.Error as e:
        error_msg = f"数据库错误: {e}"
        print(f"[student-scores/set-comment] ❌ {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[student-scores/set-comment] 错误堆栈:\n{traceback_str}")
        app_logger.error(f"[student-scores/set-comment] ❌ {error_msg}\n{traceback_str}")
        return safe_json_response({
            'message': f'数据库错误: {str(e)}',
            'code': 500
        }, status_code=500)
    except Exception as e:
        error_msg = f"未知错误: {e}"
        print(f"[student-scores/set-comment] ❌ {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[student-scores/set-comment] 错误堆栈:\n{traceback_str}")
        app_logger.error(f"[student-scores/set-comment] ❌ {error_msg}\n{traceback_str}")
        return safe_json_response({
            'message': f'未知错误: {str(e)}',
            'code': 500
        }, status_code=500)
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
            print("[student-scores/set-comment] 🔒 游标已关闭")
        if 'connection' in locals() and connection and connection.is_connected():
            connection.close()
            print("[student-scores/set-comment] 🔒 数据库连接已关闭")
            app_logger.info(f"[student-scores/set-comment] 数据库连接已关闭")
        print("[student-scores/set-comment] ========== 设置注释请求处理完成 ==========")
        print("=" * 80)


@router.post("/student-scores/set-score")
async def api_set_student_score_value(request: Request):
    """
    设置/更新特定学生特定字段的分数（更新 ta_student_score_detail.scores_json）

    请求体 JSON:
    {
      "score_header_id": 1,               // 成绩表头ID（必需）
      "student_name": "张三",              // 学生姓名（必需）
      "student_id": "2024001",            // 学号（可选，如果提供会更精确匹配）
      "field_name": "数学",                // 字段名称（必需）
      "excel_filename": "期中成绩单.xlsx",  // Excel文件名（可选；不传则尝试从字段定义表推断）
      "score": 98                          // 分数（必需；传 null/空字符串表示删除该字段的分数）
    }

    规则：
    - 有 excel_filename：使用复合键名 field_name_excel_filename 写入，并清理同名简单键 field_name（避免重复）
    - 无 excel_filename：写入简单键 field_name
    - 更新后会同步重算 total_score（用于排序）
    """
    def _parse_score_value(v):
        """尽量把输入转换为数值；失败则按原值保留。"""
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                return float(s)
            except ValueError:
                return s
        return v

    def _to_float_or_none(v) -> Optional[float]:
        """把值尽量转成 float；失败返回 None。"""
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                return float(s)
            except Exception:
                return None
        return None

    def _excel_filename_base(name: Optional[str]) -> str:
        """把 excel_filename 归一到“去扩展名”的基础名，用于清理重复键。"""
        if not name:
            return ""
        s = str(name).strip()
        if not s:
            return ""
        lower = s.lower()
        if lower.endswith(".xlsx"):
            return s[:-5]
        if lower.endswith(".xls"):
            return s[:-4]
        if lower.endswith(".csv"):
            return s[:-4]
        return s

    def _candidate_score_keys(field: str, excel_filename: Optional[str]) -> set:
        """
        生成该字段可能存在的所有 key（用于清理重复/旧数据）。
        例如：field='纪律', excel='学生体质统计表.xlsx'
        会包含：
        - '纪律'
        - '纪律_学生体质统计表.xlsx'
        - '纪律_学生体质统计表'
        - '纪律_学生体质统计表.xls'
        """
        keys = set()
        field_s = str(field).strip()
        if not field_s:
            return keys
        keys.add(field_s)

        if excel_filename:
            fn = str(excel_filename).strip()
            base = _excel_filename_base(fn)
            for suffix in {fn, base, f"{base}.xlsx" if base else "", f"{base}.xls" if base else ""}:
                suffix = (suffix or "").strip()
                if suffix:
                    keys.add(f"{field_s}_{suffix}")
        return keys

    def _recalc_total_score(scores_dict: dict) -> Optional[float]:
        """
        重算记录的 total_score（用于排序）：
        - 如果存在多个 “总分_* / total_*” 数值字段：取它们的和
        - 否则：对所有数值字段求和（排除总分字段本身）
        """
        try:
            totals: List[float] = []
            for k, v in (scores_dict or {}).items():
                ks = str(k)
                if ks.startswith("总分") or ks.lower().startswith("total"):
                    fv = _to_float_or_none(v)
                    if fv is not None:
                        totals.append(fv)
            if totals:
                return float(sum(totals))

            s = 0.0
            has_number = False
            for k, v in (scores_dict or {}).items():
                ks = str(k)
                if ks.startswith("总分") or ks.lower().startswith("total"):
                    continue
                fv = _to_float_or_none(v)
                if fv is not None:
                    s += fv
                    has_number = True
            return s if has_number else None
        except Exception:
            return None

    def _recalc_total_for_excel(scores_dict: dict, excel_filename: Optional[str]) -> Optional[float]:
        """
        对指定 excel_filename 重新计算 “总分_<excel>”：
        - 清理同一 excel 的 “总分_<excel>” 旧变体（含是否带 .xlsx/.xls）
        - 再把该 excel 下的所有数值字段求和写回
        """
        if not excel_filename:
            return None
        fn = str(excel_filename).strip()
        if not fn:
            return None

        base = _excel_filename_base(fn)
        variants = {fn, base}
        if base:
            variants.add(f"{base}.xlsx")
            variants.add(f"{base}.xls")
            variants.add(f"{base}.csv")
        variants = {v.strip() for v in variants if v and str(v).strip()}
        if not variants:
            return None

        # 先清理旧的总分键（同一 excel 的变体）
        for var in list(variants):
            scores_dict.pop(f"总分_{var}", None)
            scores_dict.pop(f"total_{var}", None)

        # 求和：只统计该 excel 的字段（key 以 _<excel> 结尾），排除总分自身
        s = 0.0
        has_number = False
        for k, v in list((scores_dict or {}).items()):
            ks = str(k)
            if ks.startswith("总分_") or ks.lower().startswith("total_"):
                continue
            matched = False
            for var in variants:
                if ks.endswith(f"_{var}"):
                    matched = True
                    break
            if not matched:
                continue
            fv = _to_float_or_none(v)
            if fv is not None:
                s += fv
                has_number = True

        if not has_number:
            return None

        # 只保留一个 canonical 的总分键（用 fn 本身）
        scores_dict[f"总分_{fn}"] = float(s)
        return float(s)

    try:
        body = await request.json()
        # 打印客户端发送的完整JSON消息
        print(f"[student-scores/set-score] ========== 收到客户端请求 ==========")
        print(f"[student-scores/set-score] 客户端发送的原始数据: {body}")
        try:
            json_str = json.dumps(body, ensure_ascii=False, indent=2)
            print(f"[student-scores/set-score] ========== 客户端发送的完整JSON消息（格式化）==========")
            print(json_str)
            print(f"[student-scores/set-score] ========== JSON消息打印结束 ==========")
            app_logger.info(f"[student-scores/set-score] ========== 客户端发送的完整JSON消息 ==========\n{json_str}")
        except Exception as print_e:
            print(f"[student-scores/set-score] 打印JSON消息失败: {print_e}")
            app_logger.warning(f"[student-scores/set-score] 打印JSON消息失败: {print_e}")
        
        score_header_id = body.get('score_header_id')
        class_id = body.get('class_id')
        term = body.get('term')
        student_name = body.get('student_name')
        student_id = body.get('student_id')  # 可选
        field_name = body.get('field_name')
        excel_filename = body.get('excel_filename')  # 可选
        score_raw = body.get('score')

        # 参数验证
        # 兼容：允许不传 score_header_id，改用 class_id + term 定位表头
        if not score_header_id:
            class_id = str(class_id).strip() if class_id is not None else ""
            if not class_id:
                return safe_json_response({'message': '缺少必需参数: score_header_id 或 class_id', 'code': 400}, status_code=400)
        if not student_name:
            return safe_json_response({'message': '缺少必需参数: student_name', 'code': 400}, status_code=400)
        if not field_name:
            return safe_json_response({'message': '缺少必需参数: field_name', 'code': 400}, status_code=400)
        # score 字段必须出现（允许为 null/空字符串，用于删除）
        if 'score' not in body:
            return safe_json_response({'message': '缺少必需参数: score', 'code': 400}, status_code=400)

        app_logger.info(
            f"[student-scores/set-score] request score_header_id={score_header_id}, "
            f"class_id={class_id}, term={term}, student_id={student_id}, field_name={field_name}, excel_filename={excel_filename}"
        )

        connection = get_db_connection()
        if connection is None:
            return safe_json_response({'message': '数据库连接失败', 'code': 500}, status_code=500)

        cursor = connection.cursor(dictionary=True)

        # 如果没有提供 score_header_id，则尝试用 class_id + term 找到表头
        if not score_header_id:
            cursor.execute(
                "SELECT id FROM ta_student_score_header "
                "WHERE class_id = %s AND ((%s IS NULL AND term IS NULL) OR term = %s) "
                "ORDER BY created_at DESC, updated_at DESC "
                "LIMIT 1",
                (class_id, term, term),
            )
            header_row = cursor.fetchone()
            if not header_row:
                return safe_json_response({'message': '没有找到数据：未找到学生成绩表头（请确认 class_id/term）', 'code': 200}, status_code=200)
            score_header_id = header_row.get("id")

        # 如果没有提供 excel_filename，尝试从字段定义中查找
        if not excel_filename:
            cursor.execute(
                "SELECT excel_filename FROM ta_student_score_field "
                "WHERE score_header_id = %s AND field_name = %s "
                "LIMIT 1",
                (score_header_id, field_name)
            )
            field_result = cursor.fetchone()
            if field_result and field_result.get('excel_filename'):
                excel_filename = field_result['excel_filename']

        if isinstance(excel_filename, str):
            excel_filename = excel_filename.strip()

        score_key = f"{field_name}_{excel_filename}" if excel_filename else field_name

        # 查询学生成绩记录
        if student_id:
            cursor.execute(
                "SELECT id, scores_json, total_score FROM ta_student_score_detail "
                "WHERE score_header_id = %s AND student_name = %s AND student_id = %s "
                "LIMIT 1",
                (score_header_id, student_name, student_id)
            )
        else:
            cursor.execute(
                "SELECT id, scores_json, total_score FROM ta_student_score_detail "
                "WHERE score_header_id = %s AND student_name = %s "
                "LIMIT 1",
                (score_header_id, student_name)
            )

        record = cursor.fetchone()
        if not record:
            return safe_json_response({'message': f'没有找到数据：未找到学生成绩记录: {student_name}', 'code': 200}, status_code=200)

        record_id = record['id']
        existing_scores_json = record.get('scores_json')

        # 解析现有的成绩JSON
        if existing_scores_json:
            if isinstance(existing_scores_json, str):
                try:
                    scores_dict = json.loads(existing_scores_json)
                except json.JSONDecodeError:
                    scores_dict = {}
            else:
                scores_dict = existing_scores_json
        else:
            scores_dict = {}

        # 更新或删除分数字段
        score_value = _parse_score_value(score_raw)
        candidate_keys = _candidate_score_keys(field_name, excel_filename)
        if score_value is None:
            # 删除：清理所有可能的重复键
            for k in candidate_keys:
                scores_dict.pop(k, None)
        else:
            # 写入：先清理同字段的旧键（含是否带扩展名的 excel_filename 变体），只保留最新插入的 key
            for k in candidate_keys:
                if k != score_key:
                    scores_dict.pop(k, None)
            scores_dict[score_key] = score_value
            # 无 excel_filename 时，兼容旧逻辑：确保简单键存在
            if not excel_filename:
                scores_dict[field_name] = score_value

        # 每次修改字段后，重算该 excel 对应的 “总分_<excel>”
        # 这样可以避免出现：总分_学生体质统计表 与 总分_学生体质统计表.xlsx 等重复/不一致
        recalced_excel_total = _recalc_total_for_excel(scores_dict, excel_filename)

        # 重算 total_score
        new_total_score = _recalc_total_score(scores_dict)

        scores_json_str = json.dumps(scores_dict, ensure_ascii=False)
        cursor.execute(
            "UPDATE ta_student_score_detail "
            "SET scores_json = %s, total_score = %s, updated_at = NOW() "
            "WHERE id = %s",
            (scores_json_str, new_total_score, record_id)
        )
        connection.commit()

        action = "delete" if score_value is None else "set"
        app_logger.info(
            f"[student-scores/set-score] success action={action}, record_id={record_id}, "
            f"score_key={score_key}, excel_total={recalced_excel_total}, total_score={new_total_score}"
        )

        return safe_json_response({
            'message': '分数设置成功',
            'code': 200,
            'data': {
                'record_id': record_id,
                'student_name': student_name,
                'field_name': field_name,
                'excel_filename': excel_filename,
                'score_key': score_key,
                'score': score_value,
                'excel_total_score': recalced_excel_total,
                'total_score': new_total_score,
                'scores_json': scores_dict
            }
        })

    except json.JSONDecodeError:
        error_msg = '请求体JSON格式错误'
        app_logger.error(f"[student-scores/set-score] {error_msg}")
        return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    except mysql.connector.Error as e:
        error_msg = f"数据库错误: {e}"
        app_logger.error(f"[student-scores/set-score] {error_msg}", exc_info=True)
        return safe_json_response({'message': f'数据库错误: {str(e)}', 'code': 500}, status_code=500)
    except Exception as e:
        error_msg = f"未知错误: {e}"
        app_logger.error(f"[student-scores/set-score] {error_msg}", exc_info=True)
        return safe_json_response({'message': f'未知错误: {str(e)}', 'code': 500}, status_code=500)
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection and connection.is_connected():
            connection.close()
            app_logger.debug("[student-scores/set-score] db connection closed")


@router.post("/group-scores/save")
async def api_save_group_scores(request: Request):
    """
    保存小组成绩表（支持动态字段，使用JSON存储）
    请求体 JSON:
    {
      "class_id": "class_1001",
      "exam_name": "期中考试",           // 考试/表名称（可选，仅展示字段）
      "term": "2025-2026-1",            // 可选
      "remark": "备注信息",              // 可选
      "operation_mode": "append",       // 可选，"append"（追加，默认）或 "replace"（替换）
      "excel_file_url": "...",          // 可选，单个Excel文件URL（旧格式）
      "excel_file_name": "...",          // 可选，Excel文件名
      "excel_file_description": "...",  // 可选，Excel文件说明
      "excel_files": [                  // 可选，多个Excel文件列表（新格式）
        {
          "filename": "期中成绩单.xlsx",
          "url": "https://...",
          "description": "说明:该表为统计表。包含以下科目/属性: 语文、数学、英语",
          "fields": ["语文", "数学", "英语", "总分"]
        }
      ],
      "fields": [                       // 可选，字段定义列表
        {
          "field_name": "语文",
          "field_type": "number",
          "field_order": 1,
          "is_total": 0
        }
      ],
      "scores": [                       // 成绩明细列表
        {
          "group_name": "1组",          // 小组名称/编号（必需）
          "student_id": "2024001",      // 可选
          "student_name": "张三",       // 必需
          "语文": 120,                  // 各科成绩（动态字段）
          "数学": 90,
          "英语": 149,
          "总分": 359,                  // 个人总分（可选，可自动计算）
          "group_total_score": 1000     // 小组总分（可选，会自动计算）
        },
        {
          "group_name": "1组",
          "student_name": "李四",
          "语文": 100,
          "数学": 85,
          "英语": 120
          // total_score 和 group_total_score 会自动计算
        }
      ]
    }
    
    支持两种请求格式：
    1. application/json: 直接发送JSON数据
    2. multipart/form-data: 包含data字段（JSON字符串）和excel_file字段（Excel文件）
    """
    print(f"[group-scores/save] ========== 收到保存请求 ==========")
    app_logger.info(f"[group-scores/save] ========== 收到保存请求 ==========")
    
    data = None
    excel_file = None
    excel_file_name = None
    excel_file_url = None
    excel_files = None
    
    # 记录请求头信息
    try:
        content_type = request.headers.get('content-type', '').lower()
        content_length = request.headers.get('content-length', '')
        print(f"[group-scores/save] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")
        app_logger.info(f"[group-scores/save] 请求头 - Content-Type: {content_type}, Content-Length: {content_length}")
    except Exception as e:
        print(f"[group-scores/save] 读取请求头失败: {e}")
        app_logger.warning(f"[group-scores/save] 读取请求头失败: {e}")
        content_type = ""
    
    # 根据Content-Type处理不同的请求格式
    if "multipart/form-data" in content_type:
        # 处理multipart/form-data格式
        print(f"[group-scores/save] ========== 处理 multipart/form-data 格式 ==========")
        app_logger.info(f"[group-scores/save] ========== 处理 multipart/form-data 格式 ==========")
        try:
            form_data = await request.form()
            print(f"[group-scores/save] ✅ 表单数据获取成功")
            app_logger.info(f"[group-scores/save] ✅ 表单数据获取成功")
            
            # 获取JSON数据（从data字段）
            data_str = form_data.get("data")
            if not data_str:
                error_msg = 'multipart请求中缺少data字段'
                print(f"[group-scores/save] ❌ {error_msg}")
                app_logger.error(f"[group-scores/save] ❌ {error_msg}")
                return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
            
            print(f"[group-scores/save] data字段类型: {type(data_str).__name__}")
            app_logger.info(f"[group-scores/save] data字段类型: {type(data_str).__name__}")
            
            # 解析JSON字符串（form_data.get返回的可能是字符串）
            try:
                if isinstance(data_str, str):
                    data = json.loads(data_str)
                else:
                    # 如果不是字符串，尝试转换为字符串再解析
                    data = json.loads(str(data_str))
                print(f"[group-scores/save] ✅ JSON解析成功")
                app_logger.info(f"[group-scores/save] ✅ JSON解析成功")
                # 打印完整的客户端上传的JSON消息
                try:
                    json_str = json.dumps(data, ensure_ascii=False, indent=2)
                    print(f"[group-scores/save] ========== 客户端上传的完整JSON消息 ==========")
                    print(json_str)
                    print(f"[group-scores/save] ========== JSON消息打印结束 ==========")
                    app_logger.info(f"[group-scores/save] ========== 客户端上传的完整JSON消息 ==========\n{json_str}")
                except Exception as print_e:
                    print(f"[group-scores/save] 打印JSON消息失败: {print_e}")
                    app_logger.warning(f"[group-scores/save] 打印JSON消息失败: {print_e}")
            except json.JSONDecodeError as e:
                error_msg = f'data字段中的JSON解析失败: {str(e)}'
                print(f"[group-scores/save] ❌ {error_msg}")
                app_logger.error(f"[group-scores/save] ❌ {error_msg}")
                return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
            
            # 获取Excel文件（可选）
            excel_file = form_data.get("excel_file")
            print(f"[group-scores/save] excel_file是否存在: {excel_file is not None}")
            app_logger.info(f"[group-scores/save] excel_file是否存在: {excel_file is not None}")
            
            if excel_file:
                print(f"[group-scores/save] ========== 开始处理Excel文件 ==========")
                app_logger.info(f"[group-scores/save] ========== 开始处理Excel文件 ==========")
                print(f"[group-scores/save] excel_file类型: {type(excel_file)}")
                print(f"[group-scores/save] excel_file类型名称: {type(excel_file).__name__}")
                app_logger.info(f"[group-scores/save] excel_file类型: {type(excel_file)}, 类型名称: {type(excel_file).__name__}")
                
                # 检查是否是UploadFile类型
                is_upload_file = isinstance(excel_file, UploadFile) or type(excel_file).__name__ == 'UploadFile'
                print(f"[group-scores/save] is_upload_file: {is_upload_file}")
                app_logger.info(f"[group-scores/save] is_upload_file: {is_upload_file}")
                
                if is_upload_file:
                    filename_value = getattr(excel_file, 'filename', None)
                    print(f"[group-scores/save] excel_file.filename值: {filename_value}")
                    app_logger.info(f"[group-scores/save] excel_file.filename值: {filename_value}")
                    
                    # 优先使用客户端JSON中的excel_file_name字段
                    excel_file_name = None
                    if data:
                        excel_file_name = data.get('excel_file_name')
                        if excel_file_name:
                            print(f"[group-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                            app_logger.info(f"[group-scores/save] ✅ 从JSON数据中获取excel_file_name: {excel_file_name}")
                    
                    # 如果JSON中没有，尝试使用excel_file.filename
                    if not excel_file_name and filename_value:
                        excel_file_name = filename_value
                        print(f"[group-scores/save] ✅ 使用excel_file.filename: {excel_file_name}")
                        app_logger.info(f"[group-scores/save] ✅ 使用excel_file.filename: {excel_file_name}")
                    
                    # 如果都没有，使用默认名称
                    if not excel_file_name:
                        timestamp = int(time.time())
                        excel_file_name = f"excel_{timestamp}.xlsx"
                        print(f"[group-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                        app_logger.warning(f"[group-scores/save] ⚠️ 使用默认文件名: {excel_file_name}")
                    
                    # 读取Excel文件内容并上传到OSS
                    try:
                        print(f"[group-scores/save] 📖 开始读取Excel文件内容...")
                        app_logger.info(f"[group-scores/save] 📖 开始读取Excel文件内容...")
                        excel_content = await excel_file.read()
                        print(f"[group-scores/save] ✅ Excel文件读取成功，文件大小: {len(excel_content)} bytes")
                        app_logger.info(f"[group-scores/save] ✅ Excel文件读取成功，文件大小: {len(excel_content)} bytes")
                        
                        # 生成OSS对象名称
                        timestamp = int(time.time())
                        file_ext = os.path.splitext(excel_file_name)[1] or '.xlsx'
                        oss_object_name = f"excel/group-scores/{timestamp}_{excel_file_name}"
                        print(f"[group-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                        app_logger.info(f"[group-scores/save] 📝 生成OSS对象名称: {oss_object_name}")
                        
                        # 上传到阿里云OSS
                        print(f"[group-scores/save] ☁️ 开始上传Excel文件到阿里云OSS...")
                        app_logger.info(f"[group-scores/save] ☁️ 开始上传Excel文件到阿里云OSS: {oss_object_name}")
                        excel_file_url = upload_excel_to_oss(excel_content, oss_object_name)
                        
                        if excel_file_url:
                            print(f"[group-scores/save] ✅ Excel文件上传成功，OSS URL: {excel_file_url}")
                            app_logger.info(f"[group-scores/save] ✅ Excel文件上传成功，OSS URL: {excel_file_url}")
                        else:
                            print(f"[group-scores/save] ❌ Excel文件上传失败，返回值为None或空")
                            app_logger.warning(f"[group-scores/save] ❌ Excel文件上传失败，返回值为None或空")
                    except Exception as e:
                        error_msg = f'读取或上传Excel文件时出错: {str(e)}'
                        print(f"[group-scores/save] ❌ 错误: {error_msg}")
                        app_logger.error(f"[group-scores/save] ❌ {error_msg}", exc_info=True)
                        import traceback
                        traceback_str = traceback.format_exc()
                        print(f"[group-scores/save] ❌ 错误堆栈:\n{traceback_str}")
                        app_logger.error(f"[group-scores/save] ❌ 错误堆栈:\n{traceback_str}")
                        # 继续处理，不阻止成绩数据保存
        except Exception as e:
            error_msg = f'处理multipart/form-data时出错: {str(e)}'
            print(f"[group-scores/save] ❌ {error_msg}")
            app_logger.error(f"[group-scores/save] ❌ {error_msg}", exc_info=True)
            import traceback
            traceback_str = traceback.format_exc()
            print(f"[group-scores/save] ❌ 错误堆栈:\n{traceback_str}")
            app_logger.error(f"[group-scores/save] ❌ 错误堆栈:\n{traceback_str}")
            return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    else:
        # 处理application/json格式
        print(f"[group-scores/save] ========== 处理 application/json 格式 ==========")
        app_logger.info(f"[group-scores/save] ========== 处理 application/json 格式 ==========")
        try:
            data = await request.json()
            print(f"[group-scores/save] ✅ JSON解析成功")
            app_logger.info(f"[group-scores/save] ✅ JSON解析成功")
            # 打印完整的客户端上传的JSON消息
            try:
                json_str = json.dumps(data, ensure_ascii=False, indent=2)
                print(f"[group-scores/save] ========== 客户端上传的完整JSON消息 ==========")
                print(json_str)
                print(f"[group-scores/save] ========== JSON消息打印结束 ==========")
                app_logger.info(f"[group-scores/save] ========== 客户端上传的完整JSON消息 ==========\n{json_str}")
            except Exception as print_e:
                print(f"[group-scores/save] 打印JSON消息失败: {print_e}")
                app_logger.warning(f"[group-scores/save] 打印JSON消息失败: {print_e}")
        except json.JSONDecodeError as e:
            error_msg = f'无效的 JSON 请求体: {str(e)}'
            print(f"[group-scores/save] ❌ {error_msg}")
            app_logger.error(f"[group-scores/save] ❌ {error_msg}")
            return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
        except Exception as e:
            error_msg = f'解析请求体失败: {str(e)}'
            print(f"[group-scores/save] ❌ {error_msg}")
            app_logger.error(f"[group-scores/save] ❌ {error_msg}", exc_info=True)
            return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    
    # 记录完整请求体（截断过长的内容）
    if data:
        try:
            request_body_str = json.dumps(data, ensure_ascii=False, indent=2)
            if len(request_body_str) > 2000:
                request_body_preview = request_body_str[:2000] + "... (已截断)"
            else:
                request_body_preview = request_body_str
            print(f"[group-scores/save] 请求体内容:\n{request_body_preview}")
            app_logger.info(f"[group-scores/save] 请求体内容:\n{request_body_preview}")
        except Exception as e:
            print(f"[group-scores/save] 序列化请求体失败: {e}")
            app_logger.warning(f"[group-scores/save] 序列化请求体失败: {e}")

    # 提取参数
    class_id = data.get('class_id') if data else None
    exam_name = data.get('exam_name') if data else None
    term = data.get('term') if data else None
    remark = data.get('remark') if data else None
    
    # 支持两种数据格式：scores（扁平）或 group_scores（嵌套）
    scores = data.get('scores', []) if data else []
    group_scores = data.get('group_scores', []) if data else []
    
    # 如果提供了 group_scores，转换为 scores 格式
    if group_scores and isinstance(group_scores, list) and len(group_scores) > 0:
        print(f"[group-scores/save] 检测到 group_scores 格式，开始转换...")
        app_logger.info(f"[group-scores/save] 检测到 group_scores 格式，开始转换...")
        converted_scores = []
        for group_item in group_scores:
            group_name = group_item.get('group_name', '').strip()
            group_total_score = group_item.get('group_total_score')
            students = group_item.get('students', [])
            
            for student in students:
                student_name = student.get('student_name', '').strip()
                if not student_name:
                    continue
                
                # 构建扁平化的学生记录
                student_record = {
                    'group_name': group_name,
                    'student_id': student.get('student_id'),
                    'student_name': student_name,
                    'group_total_score': group_total_score
                }
                
                # 处理 scores 字段（可能是对象或字典）
                student_scores = student.get('scores', {})
                if isinstance(student_scores, dict):
                    # 将 scores 对象中的字段平铺到顶层
                    for key, value in student_scores.items():
                        if key not in ['group_name', 'student_id', 'student_name', 'group_total_score']:
                            student_record[key] = value
                elif isinstance(student_scores, str):
                    # 如果是字符串，尝试解析为JSON
                    try:
                        scores_dict = json.loads(student_scores)
                        for key, value in scores_dict.items():
                            if key not in ['group_name', 'student_id', 'student_name', 'group_total_score']:
                                student_record[key] = value
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                # 如果学生记录中有其他字段（不在scores中），也添加进去
                for key, value in student.items():
                    if key not in ['scores', 'group_name', 'student_id', 'student_name', 'group_total_score']:
                        student_record[key] = value
                
                converted_scores.append(student_record)
        
        scores = converted_scores
        print(f"[group-scores/save] ✅ 转换完成，共 {len(scores)} 条学生记录")
        app_logger.info(f"[group-scores/save] ✅ 转换完成，共 {len(scores)} 条学生记录")
        
        # 显示转换后的前3条记录示例
        if len(scores) > 0:
            preview_count = min(3, len(scores))
            print(f"[group-scores/save] 转换后的前{preview_count}条记录示例:")
            app_logger.info(f"[group-scores/save] 转换后的前{preview_count}条记录示例:")
            for i in range(preview_count):
                try:
                    record_str = json.dumps(scores[i], ensure_ascii=False, indent=2)
                    print(f"[group-scores/save] 记录{i+1}:\n{record_str}")
                    app_logger.info(f"[group-scores/save] 记录{i+1}:\n{record_str}")
                except Exception as e:
                    print(f"[group-scores/save] 序列化记录{i+1}失败: {e}")
                    app_logger.warning(f"[group-scores/save] 序列化记录{i+1}失败: {e}")
    
    # 如果上传了文件，优先使用上传后的URL；否则使用data中的URL
    if excel_file_url:
        # 如果已经通过multipart上传了文件，使用上传后的URL
        print(f"[group-scores/save] ✅ 使用上传后的Excel文件URL: {excel_file_url}")
        app_logger.info(f"[group-scores/save] ✅ 使用上传后的Excel文件URL: {excel_file_url}")
        
        # 更新 excel_files 中的 URL（如果存在）
        if excel_files and isinstance(excel_files, list) and len(excel_files) > 0:
            updated_count = 0
            for ef in excel_files:
                fn = ef.get('filename') or ef.get('name') or ef.get('file_name')
                # 如果文件名匹配，或者没有指定excel_file_name但excel_files中有文件，就更新URL
                if (fn == excel_file_name) or (not excel_file_name and fn):
                    old_url = ef.get('url', '')
                    ef['url'] = excel_file_url
                    updated_count += 1
                    print(f"[group-scores/save] ✅ 更新 excel_files[{updated_count-1}] 中的 URL: {fn}")
                    print(f"[group-scores/save]   旧URL: {old_url}")
                    print(f"[group-scores/save]   新URL: {excel_file_url}")
                    app_logger.info(f"[group-scores/save] ✅ 更新 excel_files[{updated_count-1}] 中的 URL: {fn}, 旧URL: {old_url}, 新URL: {excel_file_url}")
            if updated_count == 0:
                print(f"[group-scores/save] ⚠️ 未找到匹配的文件名来更新URL (excel_file_name={excel_file_name})")
                app_logger.warning(f"[group-scores/save] ⚠️ 未找到匹配的文件名来更新URL (excel_file_name={excel_file_name})")
                # 如果没找到匹配的，尝试更新第一个文件的URL
                if len(excel_files) > 0:
                    ef = excel_files[0]
                    old_url = ef.get('url', '')
                    ef['url'] = excel_file_url
                    print(f"[group-scores/save] ✅ 更新 excel_files[0] 中的 URL (默认): {ef.get('filename', 'N/A')}")
                    print(f"[group-scores/save]   旧URL: {old_url}")
                    print(f"[group-scores/save]   新URL: {excel_file_url}")
                    app_logger.info(f"[group-scores/save] ✅ 更新 excel_files[0] 中的 URL (默认): {ef.get('filename', 'N/A')}, 旧URL: {old_url}, 新URL: {excel_file_url}")
    else:
        # 否则从data中获取
        excel_file_url = data.get('excel_file_url') if data else None
    
    excel_file_name = data.get('excel_file_name') if data else None
    excel_file_description = data.get('excel_file_description') if data else None
    operation_mode = data.get('operation_mode', 'append') if data else 'append'
    fields = data.get('fields') if data else None
    excel_files = data.get('excel_files') if data else None

    print(f"[group-scores/save] ========== 参数提取 ==========")
    print(f"[group-scores/save] class_id: {class_id} (type: {type(class_id).__name__})")
    print(f"[group-scores/save] exam_name: {exam_name} (type: {type(exam_name).__name__})")
    print(f"[group-scores/save] term: {term} (type: {type(term).__name__})")
    print(f"[group-scores/save] remark: {remark} (type: {type(remark).__name__})")
    print(f"[group-scores/save] operation_mode: {operation_mode} (type: {type(operation_mode).__name__})")
    print(f"[group-scores/save] scores数量: {len(scores) if isinstance(scores, list) else 'N/A'} (type: {type(scores).__name__})")
    print(f"[group-scores/save] excel_file_url: {excel_file_url} (type: {type(excel_file_url).__name__})")
    print(f"[group-scores/save] excel_file_name: {excel_file_name} (type: {type(excel_file_name).__name__})")
    print(f"[group-scores/save] excel_file_description: {excel_file_description} (type: {type(excel_file_description).__name__})")
    print(f"[group-scores/save] fields数量: {len(fields) if isinstance(fields, list) else 'N/A'} (type: {type(fields).__name__})")
    print(f"[group-scores/save] excel_files数量: {len(excel_files) if isinstance(excel_files, list) else 'N/A'} (type: {type(excel_files).__name__})")
    
    app_logger.info(f"[group-scores/save] 参数提取 - class_id={class_id}, exam_name={exam_name}, term={term}, operation_mode={operation_mode}, scores数量={len(scores) if isinstance(scores, list) else 0}")
    
    if excel_files:
        try:
            excel_files_str = json.dumps(excel_files, ensure_ascii=False, indent=2)
            print(f"[group-scores/save] excel_files详情:\n{excel_files_str}")
            app_logger.info(f"[group-scores/save] excel_files详情:\n{excel_files_str}")
        except Exception as e:
            print(f"[group-scores/save] 序列化excel_files失败: {e}")
            app_logger.warning(f"[group-scores/save] 序列化excel_files失败: {e}")
    
    if scores and isinstance(scores, list) and len(scores) > 0:
        try:
            first_record_str = json.dumps(scores[0], ensure_ascii=False, indent=2)
            print(f"[group-scores/save] scores第一条记录示例:\n{first_record_str}")
            app_logger.info(f"[group-scores/save] scores第一条记录示例:\n{first_record_str}")
        except Exception as e:
            print(f"[group-scores/save] 序列化第一条记录失败: {e}")
            app_logger.warning(f"[group-scores/save] 序列化第一条记录失败: {e}")
    
    if excel_files and isinstance(excel_files, list) and len(excel_files) > 0:
        try:
            excel_files_str = json.dumps(excel_files, ensure_ascii=False, indent=2)
            print(f"[group-scores/save] excel_files更新后的内容:\n{excel_files_str}")
            app_logger.info(f"[group-scores/save] excel_files更新后的内容:\n{excel_files_str}")
        except Exception as e:
            print(f"[group-scores/save] 序列化excel_files失败: {e}")
            app_logger.warning(f"[group-scores/save] 序列化excel_files失败: {e}")

    # 参数验证
    print(f"[group-scores/save] ========== 参数验证 ==========")
    app_logger.info(f"[group-scores/save] ========== 参数验证 ==========")
    
    if not data:
        error_msg = '请求数据为空'
        print(f"[group-scores/save] ❌ {error_msg}")
        app_logger.error(f"[group-scores/save] ❌ {error_msg}")
        return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    
    if not class_id:
        error_msg = '缺少必要参数 class_id'
        print(f"[group-scores/save] ❌ {error_msg}")
        app_logger.error(f"[group-scores/save] ❌ {error_msg}")
        return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    
    # exam_name 已改为可选（展示字段，不作为定位条件）
    
    if operation_mode not in ['append', 'replace']:
        error_msg = f'无效的 operation_mode: {operation_mode}，必须是 "append" 或 "replace"'
        print(f"[group-scores/save] ❌ {error_msg}")
        app_logger.error(f"[group-scores/save] ❌ {error_msg}")
        return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    
    if not isinstance(scores, list):
        error_msg = f'scores 必须是列表类型，当前类型: {type(scores).__name__}'
        print(f"[group-scores/save] ❌ {error_msg}")
        app_logger.error(f"[group-scores/save] ❌ {error_msg}")
        return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    
    if operation_mode == 'append' and len(scores) == 0:
        error_msg = '追加模式下 scores 不能为空'
        print(f"[group-scores/save] ❌ {error_msg}")
        app_logger.error(f"[group-scores/save] ❌ {error_msg}")
        return safe_json_response({'message': error_msg, 'code': 400}, status_code=400)
    
    print(f"[group-scores/save] ✅ 参数验证通过")
    app_logger.info(f"[group-scores/save] ✅ 参数验证通过")

    print(f"[group-scores/save] ========== 调用 save_group_scores 函数 ==========")
    app_logger.info(f"[group-scores/save] ========== 调用 save_group_scores 函数 ==========")
    
    try:
        result = save_group_scores(
            class_id=class_id,
            exam_name=exam_name,
            term=term,
            remark=remark,
            scores=scores,
            excel_file_url=excel_file_url,
            excel_file_name=excel_file_name,
            excel_file_description=excel_file_description,
            operation_mode=operation_mode,
            fields=fields,
            excel_files=excel_files
        )
        
        print(f"[group-scores/save] ========== save_group_scores 返回结果 ==========")
        print(f"[group-scores/save] result: {json.dumps(result, ensure_ascii=False, indent=2, default=str)}")
        app_logger.info(f"[group-scores/save] save_group_scores 返回结果: {json.dumps(result, ensure_ascii=False, indent=2, default=str)}")
        
        if result.get('success'):
            print(f"[group-scores/save] ✅ 保存成功 - score_header_id={result.get('score_header_id')}, inserted={result.get('inserted_count')}, updated={result.get('updated_count')}, deleted={result.get('deleted_student_count')}")
            app_logger.info(f"[group-scores/save] ✅ 保存成功 - score_header_id={result.get('score_header_id')}, inserted={result.get('inserted_count')}, updated={result.get('updated_count')}, deleted={result.get('deleted_student_count')}")
            return safe_json_response({'message': '保存成功', 'code': 200, 'data': result})
        else:
            error_msg = result.get('message', '保存失败')
            print(f"[group-scores/save] ❌ 保存失败: {error_msg}")
            app_logger.error(f"[group-scores/save] ❌ 保存失败: {error_msg}")
            return safe_json_response({'message': error_msg, 'code': 500}, status_code=500)
    except Exception as e:
        error_msg = f'调用 save_group_scores 时发生异常: {str(e)}'
        print(f"[group-scores/save] ❌ {error_msg}")
        app_logger.error(f"[group-scores/save] ❌ {error_msg}", exc_info=True)
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[group-scores/save] 异常堆栈:\n{traceback_str}")
        app_logger.error(f"[group-scores/save] 异常堆栈:\n{traceback_str}")
        return safe_json_response({'message': error_msg, 'code': 500}, status_code=500)


@router.get("/group-scores")
async def api_get_group_scores(
    request: Request,
    class_id: str = Query(..., description="班级ID"),
    exam_name: Optional[str] = Query(None, description="考试名称（可选，仅展示字段；不再作为查询条件）"),
    term: Optional[str] = Query(None, description="学期，可选")
):
    """
    查询小组成绩表
    返回 JSON:
    {
      "message": "查询成功",
      "code": 200,
      "data": {
        "header": {
          "id": 1,
          "class_id": "class_1001",
          "exam_name": "期中考试",
          "term": "2025-2026-1",
          "remark": "...",
          "excel_file_url": {...},
          "created_at": "...",
          "updated_at": "..."
        },
        "group_scores": [
          {
            "group_name": "1组",
            "group_total_score": 765.0,  // 小组总分
            "students": [
              {
                "id": 1,
                "student_id": "2024001",
                "student_name": "张三",
                "语文": 120,
                "数学": 90,
                "英语": 149,
                "总分": 359,              // 个人总分
                "group_total_score": 765.0,  // 小组总分（同组同值）
                "scores": {               // 所有动态字段
                  "语文": 120,
                  "数学": 90,
                  "英语": 149
                }
              },
              ...
            ]
          },
          {
            "group_name": "2组",
            "group_total_score": 544.0,
            "students": [...]
          },
          ...
        ]
      }
    }
    """
    connection = get_db_connection()
    if connection is None:
        error_response = {'message': '数据库连接失败', 'code': 500}
        # try:
        #     error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
        #     print(f"[group-scores] 返回的 JSON 结果（数据库连接失败）:\n{error_json}")
        #     app_logger.error(f"[group-scores] 返回的 JSON 结果（数据库连接失败）: {json.dumps(error_response, ensure_ascii=False)}")
        # except Exception as json_error:
        #     print(f"[group-scores] 打印 JSON 时出错: {json_error}")
        return safe_json_response(error_response, status_code=500)

    try:
        cursor = connection.cursor(dictionary=True)
        
        # 查询小组成绩表头
        # 约定：class_id + term 唯一定位一张小组成绩表；exam_name 仅展示字段，不参与定位
        cursor.execute(
            "SELECT id, class_id, exam_name, term, remark, excel_file_url, created_at, updated_at "
            "FROM ta_group_score_header "
            "WHERE class_id = %s AND ((%s IS NULL AND term IS NULL) OR term = %s) "
            "ORDER BY created_at DESC LIMIT 1",
            (class_id, term, term)
        )
        
        header = cursor.fetchone()
        if not header:
            error_response = {'message': '没有找到数据：未找到小组成绩表', 'code': 200}
            # try:
            #     error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
            #     print(f"[group-scores] 返回的 JSON 结果（未找到数据）:\n{error_json}")
            #     app_logger.info(f"[group-scores] 返回的 JSON 结果（未找到数据）: {json.dumps(error_response, ensure_ascii=False)}")
            # except Exception as json_error:
            #     print(f"[group-scores] 打印 JSON 时出错: {json_error}")
            return safe_json_response(error_response, status_code=200)

        score_header_id = header['id']
        
        # 明细表列探测：comments_json 可能需要迁移后才存在
        cursor.execute("SHOW COLUMNS FROM `ta_group_score_detail`")
        detail_cols = [r.get("Field") for r in (cursor.fetchall() or []) if isinstance(r, dict)]
        has_comments_json = "comments_json" in detail_cols

        # 查询所有成绩明细，按小组名称和学生姓名排序
        detail_select_cols = [
            "id",
            "group_name",
            "student_id",
            "student_name",
            "scores_json",
            "total_score",
            "group_total_score",
        ]
        if has_comments_json:
            detail_select_cols.append("comments_json")

        cursor.execute(
            f"SELECT {', '.join(detail_select_cols)} "
            "FROM ta_group_score_detail "
            "WHERE score_header_id = %s "
            "ORDER BY group_name ASC, student_name ASC",
            (score_header_id,),
        )
        all_scores = cursor.fetchall() or []
        
        # 解析excel_file_url
        excel_file_url_parsed = None
        excel_filenames = []  # 收集所有Excel文件名
        if header.get('excel_file_url'):
            try:
                excel_file_url_parsed = json.loads(header['excel_file_url']) if isinstance(header['excel_file_url'], str) else header['excel_file_url']
                # 提取Excel文件名列表
                if isinstance(excel_file_url_parsed, dict):
                    excel_filenames = list(excel_file_url_parsed.keys())
            except (json.JSONDecodeError, TypeError):
                excel_file_url_parsed = header.get('excel_file_url')
        
        # 收集所有字段名（从scores_json中推断，支持复合键名）
        # 先从第一条记录中收集所有字段名，用于解析
        all_field_names = set()
        if all_scores:
            for score in all_scores:
                if score.get('scores_json'):
                    try:
                        scores_data = json.loads(score['scores_json']) if isinstance(score['scores_json'], str) else score['scores_json']
                        for key in scores_data.keys():
                            # 如果是复合键名（包含下划线和Excel文件名），提取字段名
                            if '_' in key:
                                for excel_filename in excel_filenames:
                                    if key.endswith(f"_{excel_filename}"):
                                        field_name = key[:-len(f"_{excel_filename}")]
                                        all_field_names.add(field_name)
                                        break
                                else:
                                    # 如果没匹配到，可能是其他格式的复合键，使用原键名
                                    all_field_names.add(key)
                            else:
                                # 简单字段名
                                all_field_names.add(key)
                    except (json.JSONDecodeError, TypeError):
                        pass
        
        # 按小组分组
        group_dict = {}
        for score in all_scores:
            group_name = score.get('group_name', '').strip() or '未分组'
            
            # 解析scores_json（支持复合键名）
            scores_data = {}
            scores_data_full = {}  # 完整的scores_json（包含所有复合键名）
            comments_data_full = {}  # 完整的comments_json（包含所有复合键名）
            if score.get('scores_json'):
                try:
                    scores_data_raw = json.loads(score['scores_json']) if isinstance(score['scores_json'], str) else score['scores_json']
                    scores_data_full = scores_data_raw
                    # 解析复合键名，转换为简单字段名
                    # 优先使用简单字段名（兼容旧数据），如果没有则使用复合键名
                    for field_name in all_field_names:
                        found_sources = []
                        # 优先使用简单字段名
                        if field_name in scores_data_raw:
                            scores_data[field_name] = scores_data_raw[field_name]
                            found_sources.append({
                                'excel_filename': None,  # 旧数据或单一来源
                                'value': scores_data_raw[field_name]
                            })
                        else:
                            # 尝试使用复合键名查找
                            for excel_filename in excel_filenames:
                                composite_key = f"{field_name}_{excel_filename}"
                                if composite_key in scores_data_raw:
                                    if field_name not in scores_data:
                                        scores_data[field_name] = scores_data_raw[composite_key]  # 第一个作为默认值
                                    found_sources.append({
                                        'excel_filename': excel_filename,
                                        'value': scores_data_raw[composite_key]
                                    })
                        
                    # 同时保留原始的scores_json（包含复合键名），方便调试
                    # scores_data 中现在包含解析后的简单字段名
                except (json.JSONDecodeError, TypeError):
                    scores_data = {}
                    scores_data_full = {}

            # 解析 comments_json（完整返回，避免重复下发）
            if has_comments_json and score.get("comments_json"):
                try:
                    comments_raw = score.get("comments_json")
                    comments_data_full = json.loads(comments_raw) if isinstance(comments_raw, str) else comments_raw
                    if not isinstance(comments_data_full, dict):
                        comments_data_full = {}
                except (json.JSONDecodeError, TypeError):
                    comments_data_full = {}
            
            if group_name not in group_dict:
                group_dict[group_name] = {
                    'group_name': group_name,
                    'group_total_score': score.get('group_total_score'),
                    'students': []
                }
            
            # 构建学生信息（仅返回 scores_json_full + comments_json_full，避免重复下发）
            student_info = {
                'id': score['id'],
                'student_id': score.get('student_id'),
                'student_name': score.get('student_name', ''),
                'total_score': float(score['total_score']) if score.get('total_score') is not None else None,
                'group_total_score': float(score['group_total_score']) if score.get('group_total_score') is not None else None,
                'scores_json_full': scores_data_full,  # 完整的scores_json（包含所有复合键名）
                'comments_json_full': comments_data_full if has_comments_json else {}  # 完整的comments_json（包含所有复合键名）
            }
            
            group_dict[group_name]['students'].append(student_info)
        
        # 转换为列表，按小组名称排序
        group_scores_list = sorted(group_dict.values(), key=lambda x: x['group_name'])
        
        # 转换 datetime 为字符串（用于 JSON 序列化）
        created_at = header.get('created_at')
        if created_at and isinstance(created_at, datetime.datetime):
            created_at = created_at.strftime("%Y-%m-%d %H:%M:%S")
        updated_at = header.get('updated_at')
        if updated_at and isinstance(updated_at, datetime.datetime):
            updated_at = updated_at.strftime("%Y-%m-%d %H:%M:%S")
        
        # 转换 Decimal 类型为 float（用于 JSON 序列化）
        from decimal import Decimal
        def convert_for_json(obj):
            """递归转换 Decimal 类型为 JSON 可序列化的类型"""
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, datetime.datetime):
                return obj.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_for_json(item) for item in obj]
            return obj
        
        # 转换 group_scores_list 以确保 JSON 序列化正常
        group_scores_list = convert_for_json(group_scores_list)

        response_data = {
            'message': '查询成功',
            'code': 200,
            'data': {
                'header': {
                    'id': header['id'],
                    'class_id': header['class_id'],
                    'exam_name': header.get('exam_name'),
                    'term': header.get('term'),
                    'remark': header.get('remark'),
                    'excel_file_url': excel_file_url_parsed,
                    'created_at': created_at,
                    'updated_at': updated_at
                },
                'group_scores': group_scores_list
            }
        }
        
        # 打印返回的 JSON 结果
        # try:
        #     response_json = json.dumps(response_data, ensure_ascii=False, indent=2)
        #     print(f"[group-scores] 返回的 JSON 结果:\n{response_json}")
        #     app_logger.info(f"[group-scores] 返回的 JSON 结果: {json.dumps(response_data, ensure_ascii=False)}")
        # except Exception as json_error:
        #     print(f"[group-scores] 打印 JSON 时出错: {json_error}")
        #     app_logger.warning(f"[group-scores] 打印 JSON 时出错: {json_error}")
        
        return safe_json_response(response_data)
    except mysql.connector.Error as e:
        error_response = {'message': '数据库错误', 'code': 500}
        app_logger.error(f"Database error during api_get_group_scores: {e}")
        # try:
        #     error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
        #     print(f"[group-scores] 返回的 JSON 结果（数据库错误）:\n{error_json}")
        #     app_logger.error(f"[group-scores] 返回的 JSON 结果（数据库错误）: {json.dumps(error_response, ensure_ascii=False)}")
        # except Exception as json_error:
        #     print(f"[group-scores] 打印 JSON 时出错: {json_error}")
        return safe_json_response(error_response, status_code=500)
    except Exception as e:
        error_response = {'message': f'未知错误: {str(e)}', 'code': 500}
        app_logger.error(f"Unexpected error during api_get_group_scores: {e}")
        import traceback
        traceback_str = traceback.format_exc()
        app_logger.error(f"错误堆栈:\n{traceback_str}")
        # try:
        #     error_json = json.dumps(error_response, ensure_ascii=False, indent=2)
        #     print(f"[group-scores] 返回的 JSON 结果（未知错误）:\n{error_json}")
        #     app_logger.error(f"[group-scores] 返回的 JSON 结果（未知错误）: {json.dumps(error_response, ensure_ascii=False)}")
        # except Exception as json_error:
        #     print(f"[group-scores] 打印 JSON 时出错: {json_error}")
        return safe_json_response(error_response, status_code=500)
    finally:
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching group scores.")


@router.post("/group-scores/set-score")
async def api_set_group_score_value(request: Request):
    """
    小组成绩：设置/更新某个学生某个字段的分数（更新 ta_group_score_detail.scores_json）。

    请求体 JSON:
    {
      "class_id": "class_1001",                 // 必需（用于定位 header：class_id + term）
      "term": "2025-2026-1",                    // 可选（不传则定位 term IS NULL 的那条）
      "student_id": "2024001",                  // 可选（与 student_name 二选一；推荐传）
      "student_name": "张三",                   // 可选（与 student_id 二选一）
      "field_name": "纪律",                     // 必需
      "excel_filename": "学生体质统计表.xlsx",   // 可选：用于复合键名 field_excel
      "table_name": "学生体质统计表",            // 可选：excel_filename 的基名替代；精确匹配到 *.xlsx
      "score": 98                               // 必需：分数；传 null/空字符串表示删除该字段
    }
    """
    def _parse_score_value(v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                return float(s)
            except ValueError:
                return s
        return v

    def _to_float_or_none(v) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                return float(s)
            except Exception:
                return None
        return None

    def _excel_filename_base(name: Optional[str]) -> str:
        if not name:
            return ""
        s = str(name).strip()
        if not s:
            return ""
        lower = s.lower()
        if lower.endswith(".xlsx"):
            return s[:-5]
        if lower.endswith(".xls"):
            return s[:-4]
        if lower.endswith(".csv"):
            return s[:-4]
        return s

    def _candidate_score_keys(field: str, excel_filename: Optional[str]) -> set:
        keys = set()
        field_s = str(field).strip()
        if not field_s:
            return keys
        keys.add(field_s)
        if excel_filename:
            fn = str(excel_filename).strip()
            base = _excel_filename_base(fn)
            for suffix in {fn, base, f"{base}.xlsx" if base else "", f"{base}.xls" if base else ""}:
                suffix = (suffix or "").strip()
                if suffix:
                    keys.add(f"{field_s}_{suffix}")
        return keys

    def _recalc_total_score(scores_dict: dict) -> Optional[float]:
        try:
            totals: List[float] = []
            for k, v in (scores_dict or {}).items():
                ks = str(k)
                if ks.startswith("总分") or ks.lower().startswith("total"):
                    fv = _to_float_or_none(v)
                    if fv is not None:
                        totals.append(fv)
            if totals:
                return float(sum(totals))

            s = 0.0
            has_number = False
            for k, v in (scores_dict or {}).items():
                ks = str(k)
                if ks.startswith("总分") or ks.lower().startswith("total"):
                    continue
                fv = _to_float_or_none(v)
                if fv is not None:
                    s += fv
                    has_number = True
            return s if has_number else None
        except Exception:
            return None

    def _recalc_total_for_excel(scores_dict: dict, excel_filename: Optional[str]) -> Optional[float]:
        if not excel_filename:
            return None
        fn = str(excel_filename).strip()
        if not fn:
            return None

        base = _excel_filename_base(fn)
        variants = {fn, base}
        if base:
            variants.add(f"{base}.xlsx")
            variants.add(f"{base}.xls")
            variants.add(f"{base}.csv")
        variants = {v.strip() for v in variants if v and str(v).strip()}
        if not variants:
            return None

        for var in list(variants):
            scores_dict.pop(f"总分_{var}", None)
            scores_dict.pop(f"total_{var}", None)

        s = 0.0
        has_number = False
        for k, v in list((scores_dict or {}).items()):
            ks = str(k)
            if ks.startswith("总分_") or ks.lower().startswith("total_"):
                continue
            matched = False
            for var in variants:
                if ks.endswith(f"_{var}"):
                    matched = True
                    break
            if not matched:
                continue
            fv = _to_float_or_none(v)
            if fv is not None:
                s += fv
                has_number = True

        if not has_number:
            return None

        scores_dict[f"总分_{fn}"] = float(s)
        return float(s)

    def _calc_total_for_excel(scores_dict: dict, excel_filename: Optional[str]) -> Optional[float]:
        """
        计算某个 excel 维度下的“表内总分”（不修改 scores_dict）。
        - 仅统计 key 形如：xxx_{excel} / xxx_{excel_base} 等变体
        - 跳过 总分_/total_ 开头的字段
        """
        if not excel_filename:
            return None
        try:
            fn = str(excel_filename).strip()
            if not fn:
                return None
            base = _excel_filename_base(fn)
            variants = {fn, base}
            if base:
                variants.add(f"{base}.xlsx")
                variants.add(f"{base}.xls")
                variants.add(f"{base}.csv")
            variants = {v.strip() for v in variants if v and str(v).strip()}
            if not variants:
                return None

            s = 0.0
            has_number = False
            for k, v in (scores_dict or {}).items():
                ks = str(k)
                if ks.startswith("总分_") or ks.lower().startswith("total_"):
                    continue
                matched = False
                for var in variants:
                    if ks.endswith(f"_{var}"):
                        matched = True
                        break
                if not matched:
                    continue
                fv = _to_float_or_none(v)
                if fv is not None:
                    s += fv
                    has_number = True
            return float(s) if has_number else None
        except Exception:
            return None

    def _return_with_log(payload: dict, status_code: int = 200):
        """
        统一打印并记录本接口的返回消息，便于排查前端/接口调用问题。
        - 控制台：print（开发/容器日志可见）
        - 文件日志：app_logger（logs/app.log）
        """
        try:
            msg = ""
            if isinstance(payload, dict):
                msg = str(payload.get("message") or "")
            response_json_pretty = json.dumps(payload, ensure_ascii=False, indent=2)
            print(f"[group-scores/set-score] 返回 message={msg} status_code={status_code}\n{response_json_pretty}")

            response_json_compact = json.dumps(payload, ensure_ascii=False)
            if status_code >= 500:
                app_logger.error(
                    f"[group-scores/set-score] 返回 message={msg} status_code={status_code} payload={response_json_compact}"
                )
            elif status_code >= 400:
                app_logger.warning(
                    f"[group-scores/set-score] 返回 message={msg} status_code={status_code} payload={response_json_compact}"
                )
            else:
                app_logger.info(
                    f"[group-scores/set-score] 返回 message={msg} status_code={status_code} payload={response_json_compact}"
                )
        except Exception as log_error:
            print(f"[group-scores/set-score] 打印返回消息时出错: {log_error}")
            try:
                app_logger.warning(f"[group-scores/set-score] 打印返回消息时出错: {log_error}")
            except Exception:
                pass

        return safe_json_response(payload, status_code=status_code)

    try:
        data = await request.json()
    except Exception:
        return _return_with_log({"message": "请求体JSON格式错误", "code": 400}, status_code=400)

    class_id = (data.get("class_id") or "").strip()
    term = data.get("term")
    student_id = (data.get("student_id") or "").strip() or None
    student_name = (data.get("student_name") or "").strip() or None
    field_name = (data.get("field_name") or "").strip()
    excel_filename = (data.get("excel_filename") or "").strip() or None
    table_name = (data.get("table_name") or "").strip() or None
    score_raw = data.get("score")

    if not class_id:
        return _return_with_log({"message": "缺少必要参数 class_id", "code": 400}, status_code=400)
    if not (student_id or student_name):
        return _return_with_log({"message": "缺少必要参数 student_id 或 student_name", "code": 400}, status_code=400)
    if not field_name:
        return _return_with_log({"message": "缺少必要参数 field_name", "code": 400}, status_code=400)
    if not (excel_filename or table_name):
        return _return_with_log({"message": "缺少必要参数：excel_filename 或 table_name（必须二选一传入）", "code": 400}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return _return_with_log({"message": "数据库连接失败", "code": 500}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 定位 header：class_id + term
        cursor.execute(
            "SELECT id, class_id, term, excel_file_url FROM ta_group_score_header "
            "WHERE class_id = %s AND ((%s IS NULL AND term IS NULL) OR term = %s) "
            "ORDER BY created_at DESC LIMIT 1",
            (class_id, term, term),
        )
        header_row = cursor.fetchone()
        if not header_row:
            return _return_with_log({"message": "没有找到数据：未找到小组成绩表头（请确认 class_id/term）", "code": 200}, status_code=200)

        shid = int(header_row["id"])

        # 解析 header.excel_file_url 中的 excel 文件名（用于 table_name -> excel_filename）
        excel_filenames: List[str] = []
        try:
            raw = header_row.get("excel_file_url")
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                excel_filenames = [str(k) for k in parsed.keys() if k]
        except Exception:
            excel_filenames = []

        def _resolve_excel_filename() -> Optional[str]:
            if excel_filename:
                return excel_filename
            if table_name:
                base = table_name[:-5] if table_name.lower().endswith(".xlsx") else table_name
                base = base.strip()
                for fn in excel_filenames:
                    b = fn[:-5] if fn.lower().endswith(".xlsx") else fn
                    if str(b).strip() == base:
                        return fn
                return None
            return None

        resolved_excel = _resolve_excel_filename()
        if not resolved_excel:
            return _return_with_log(
                {"message": "无法解析 excel_filename/table_name 到具体 Excel 文件名，请检查参数", "code": 400},
                status_code=400,
            )
        score_key = f"{field_name}_{resolved_excel}" if resolved_excel else field_name

        # 定位学生记录
        where = ["score_header_id = %s"]
        params: List[Any] = [shid]
        if student_id:
            where.append("student_id = %s")
            params.append(student_id)
        else:
            where.append("student_name = %s")
            params.append(student_name)

        cursor.execute(
            "SELECT id, group_name, student_id, student_name, scores_json "
            "FROM ta_group_score_detail "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY id DESC LIMIT 1",
            tuple(params),
        )
        record = cursor.fetchone()
        if not record:
            return _return_with_log({"message": "没有找到数据：未找到该学生的小组成绩明细", "code": 200}, status_code=200)

        record_id = int(record["id"])
        record_group_name = (record.get("group_name") or "").strip() or ""

        # 解析 scores_json
        scores_json_raw = record.get("scores_json")
        if scores_json_raw:
            if isinstance(scores_json_raw, str):
                try:
                    scores_dict = json.loads(scores_json_raw)
                except json.JSONDecodeError:
                    scores_dict = {}
            else:
                scores_dict = scores_json_raw
        else:
            scores_dict = {}
        if not isinstance(scores_dict, dict):
            scores_dict = {}

        score_value = _parse_score_value(score_raw)
        candidate_keys = _candidate_score_keys(field_name, resolved_excel) if resolved_excel else {field_name}

        if score_value is None:
            for k in candidate_keys:
                scores_dict.pop(k, None)
        else:
            for k in candidate_keys:
                if k != score_key:
                    scores_dict.pop(k, None)
            scores_dict[score_key] = score_value
            if not resolved_excel:
                scores_dict[field_name] = score_value

        excel_total = _recalc_total_for_excel(scores_dict, resolved_excel) if resolved_excel else None
        new_total_score = _recalc_total_score(scores_dict)

        cursor.execute(
            "UPDATE ta_group_score_detail "
            "SET scores_json = %s, total_score = %s, updated_at = NOW() "
            "WHERE id = %s",
            (json.dumps(scores_dict, ensure_ascii=False), new_total_score, record_id),
        )

        # 重算该组 group_total_score：求和该组所有学生 total_score，并回写到同组所有行
        group_total_score = None
        if record_group_name:
            # 注意：这里的 group_total_score 需要表示“当前表（resolved_excel）的小组总分”，
            # 不能简单 SUM(total_score)（total_score 往往是跨表累计的总分）。
            cursor.execute(
                "SELECT scores_json FROM ta_group_score_detail "
                "WHERE score_header_id = %s AND group_name = %s",
                (shid, record_group_name),
            )
            rows = cursor.fetchall() or []
            s = 0.0
            has_number = False
            for r in rows:
                raw_scores = (r or {}).get("scores_json")
                sd = {}
                if raw_scores:
                    if isinstance(raw_scores, str):
                        try:
                            sd = json.loads(raw_scores)
                        except Exception:
                            sd = {}
                    elif isinstance(raw_scores, dict):
                        sd = raw_scores
                if not isinstance(sd, dict):
                    sd = {}

                # 优先使用已保存的“表内总分”字段，总能更快也更稳定
                key_total = f"总分_{resolved_excel}" if resolved_excel else None
                ft = _to_float_or_none(sd.get(key_total)) if key_total else None
                if ft is None:
                    ft = _calc_total_for_excel(sd, resolved_excel)
                if ft is not None:
                    s += float(ft)
                    has_number = True
            group_total_score = float(s) if has_number else None

            cursor.execute(
                "UPDATE ta_group_score_detail "
                "SET group_total_score = %s, updated_at = NOW() "
                "WHERE score_header_id = %s AND group_name = %s",
                (group_total_score, shid, record_group_name),
            )

        connection.commit()

        action = "delete" if score_value is None else "set"
        return _return_with_log(
            {
                "message": "分数设置成功",
                "code": 200,
                "data": {
                    "action": action,
                    "score_header_id": shid,
                    "record_id": record_id,
                    "group_name": record_group_name,
                    "student_id": record.get("student_id"),
                    "student_name": record.get("student_name"),
                    "field_name": field_name,
                    "excel_filename": resolved_excel,
                    "table_name": table_name,
                    "score_key": score_key,
                    "score": score_value,
                    "excel_total_score": excel_total,
                    "total_score": new_total_score,
                    "group_total_score": group_total_score,
                },
            },
            status_code=200,
        )
    except mysql.connector.Error as e:
        if connection:
            try:
                connection.rollback()
            except Exception:
                pass
        app_logger.error(f"[group-scores/set-score] Database error: {e}", exc_info=True)
        return _return_with_log({"message": f"数据库错误: {str(e)}", "code": 500}, status_code=500)
    except Exception as e:
        if connection:
            try:
                connection.rollback()
            except Exception:
                pass
        app_logger.error(f"[group-scores/set-score] Unexpected error: {e}", exc_info=True)
        return _return_with_log({"message": f"未知错误: {str(e)}", "code": 500}, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if connection and connection.is_connected():
                connection.close()
        except Exception:
            pass


@router.post("/group-scores/set-comment")
async def api_set_group_score_comment(request: Request):
    """
    小组成绩：设置/更新某个学生某个字段的注释（更新 ta_group_score_detail.comments_json）。

    注意：需要先给 ta_group_score_detail 增加 comments_json 字段（JSON）。
    """
    def _return_with_log(payload: dict, status_code: int = 200):
        """
        统一打印并记录本接口的返回消息，便于排查前端/接口调用问题。
        - 控制台：print（开发/容器日志可见）
        - 文件日志：app_logger（logs/app.log）
        """
        try:
            msg = ""
            if isinstance(payload, dict):
                msg = str(payload.get("message") or "")
            response_json_pretty = json.dumps(payload, ensure_ascii=False, indent=2)
            print(f"[group-scores/set-comment] 返回 message={msg} status_code={status_code}\n{response_json_pretty}")

            response_json_compact = json.dumps(payload, ensure_ascii=False)
            if status_code >= 500:
                app_logger.error(
                    f"[group-scores/set-comment] 返回 message={msg} status_code={status_code} payload={response_json_compact}"
                )
            elif status_code >= 400:
                app_logger.warning(
                    f"[group-scores/set-comment] 返回 message={msg} status_code={status_code} payload={response_json_compact}"
                )
            else:
                app_logger.info(
                    f"[group-scores/set-comment] 返回 message={msg} status_code={status_code} payload={response_json_compact}"
                )
        except Exception as log_error:
            print(f"[group-scores/set-comment] 打印返回消息时出错: {log_error}")
            try:
                app_logger.warning(f"[group-scores/set-comment] 打印返回消息时出错: {log_error}")
            except Exception:
                pass

        return safe_json_response(payload, status_code=status_code)

    def _to_float_or_none(v) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                return float(s)
            except Exception:
                return None
        return None

    def _excel_filename_base(name: Optional[str]) -> str:
        if not name:
            return ""
        s = str(name).strip()
        if not s:
            return ""
        lower = s.lower()
        if lower.endswith(".xlsx"):
            return s[:-5]
        if lower.endswith(".xls"):
            return s[:-4]
        if lower.endswith(".csv"):
            return s[:-4]
        return s

    def _calc_total_for_excel(scores_dict: dict, excel_filename: Optional[str]) -> Optional[float]:
        """
        计算某个 excel 维度下的“表内总分”（不修改 scores_dict）。
        - 仅统计 key 形如：xxx_{excel} / xxx_{excel_base} 等变体
        - 跳过 总分_/total_ 开头的字段
        """
        if not excel_filename:
            return None
        try:
            fn = str(excel_filename).strip()
            if not fn:
                return None
            base = _excel_filename_base(fn)
            variants = {fn, base}
            if base:
                variants.add(f"{base}.xlsx")
                variants.add(f"{base}.xls")
                variants.add(f"{base}.csv")
            variants = {v.strip() for v in variants if v and str(v).strip()}
            if not variants:
                return None

            s = 0.0
            has_number = False
            for k, v in (scores_dict or {}).items():
                ks = str(k)
                if ks.startswith("总分_") or ks.lower().startswith("total_"):
                    continue
                matched = False
                for var in variants:
                    if ks.endswith(f"_{var}"):
                        matched = True
                        break
                if not matched:
                    continue
                fv = _to_float_or_none(v)
                if fv is not None:
                    s += fv
                    has_number = True
            return float(s) if has_number else None
        except Exception:
            return None

    try:
        data = await request.json()
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        print(f"[group-scores/set-comment] 客户端发送的消息: {data}")
        app_logger.info(f"[group-scores/set-comment] 客户端发送的消息:\n{json_str}")
    except Exception:
        return _return_with_log({"message": "请求体JSON格式错误", "code": 400}, status_code=400)

    class_id = (data.get("class_id") or "").strip()
    term = data.get("term")
    student_id = (data.get("student_id") or "").strip() or None
    student_name = (data.get("student_name") or "").strip() or None
    field_name = (data.get("field_name") or "").strip()
    excel_filename = (data.get("excel_filename") or "").strip() or None
    table_name = (data.get("table_name") or "").strip() or None
    comment = data.get("comment")

    if not class_id:
        return _return_with_log({"message": "缺少必要参数 class_id", "code": 400}, status_code=400)
    if not (student_id or student_name):
        return _return_with_log({"message": "缺少必要参数 student_id 或 student_name", "code": 400}, status_code=400)
    if not field_name:
        return _return_with_log({"message": "缺少必要参数 field_name", "code": 400}, status_code=400)
    if not (excel_filename or table_name):
        return _return_with_log({"message": "缺少必要参数：excel_filename 或 table_name（必须二选一传入）", "code": 400}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return _return_with_log({"message": "数据库连接失败", "code": 500}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 检查 comments_json 字段是否存在
        cursor.execute("SHOW COLUMNS FROM `ta_group_score_detail`")
        cols = [r.get("Field") for r in (cursor.fetchall() or []) if isinstance(r, dict)]
        if "comments_json" not in cols:
            return _return_with_log(
                {
                    "message": "小组成绩表缺少 comments_json 字段，请先执行 add_comments_json_to_group_score_detail.sql",
                    "code": 500,
                },
                status_code=500,
            )

        # 定位 header：class_id + term
        cursor.execute(
            "SELECT id, class_id, term, excel_file_url FROM ta_group_score_header "
            "WHERE class_id = %s AND ((%s IS NULL AND term IS NULL) OR term = %s) "
            "ORDER BY created_at DESC LIMIT 1",
            (class_id, term, term),
        )
        header_row = cursor.fetchone()
        if not header_row:
            return _return_with_log({"message": "没有找到数据：未找到小组成绩表头（请确认 class_id/term）", "code": 200}, status_code=200)

        shid = int(header_row["id"])

        # excel 文件名列表
        excel_filenames: List[str] = []
        try:
            raw = header_row.get("excel_file_url")
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                excel_filenames = [str(k) for k in parsed.keys() if k]
        except Exception:
            excel_filenames = []

        # 定位学生记录（读出 comments_json/field_source_json 便于推断 excel_filename）
        where = ["score_header_id = %s"]
        params: List[Any] = [shid]
        if student_id:
            where.append("student_id = %s")
            params.append(student_id)
        else:
            where.append("student_name = %s")
            params.append(student_name)

        select_cols = ["id", "group_name", "student_id", "student_name", "comments_json", "scores_json"]
        if "field_source_json" in cols:
            select_cols.append("field_source_json")

        cursor.execute(
            f"SELECT {', '.join(select_cols)} FROM ta_group_score_detail WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT 1",
            tuple(params),
        )
        record = cursor.fetchone()
        if not record:
            return _return_with_log({"message": "没有找到数据：未找到该学生的小组成绩明细", "code": 200}, status_code=200)

        def _resolve_excel_filename_from_context() -> Optional[str]:
            if excel_filename:
                return excel_filename
            if table_name:
                base = table_name[:-5] if table_name.lower().endswith(".xlsx") else table_name
                base = base.strip()
                for fn in excel_filenames:
                    b = fn[:-5] if fn.lower().endswith(".xlsx") else fn
                    if str(b).strip() == base:
                        return fn
                return None
            return None

        resolved_excel = _resolve_excel_filename_from_context()
        if not resolved_excel:
            return _return_with_log(
                {"message": "无法解析 excel_filename/table_name 到具体 Excel 文件名，请检查参数", "code": 400},
                status_code=400,
            )
        comment_key = f"{field_name}_{resolved_excel}" if resolved_excel else field_name
        record_group_name = (record.get("group_name") or "").strip() or ""

        # 解析 comments_json
        comments_raw = record.get("comments_json")
        if comments_raw:
            if isinstance(comments_raw, str):
                try:
                    comments_dict = json.loads(comments_raw)
                except json.JSONDecodeError:
                    comments_dict = {}
            else:
                comments_dict = comments_raw
        else:
            comments_dict = {}
        if not isinstance(comments_dict, dict):
            comments_dict = {}

        # 删除/写入
        if comment is None or (isinstance(comment, str) and not comment.strip()):
            comments_dict.pop(comment_key, None)
        else:
            # 有复合键时，避免简单键重复
            if resolved_excel:
                comments_dict.pop(field_name, None)
            comments_dict[comment_key] = comment

        cursor.execute(
            "UPDATE ta_group_score_detail SET comments_json = %s, updated_at = NOW() WHERE id = %s",
            (json.dumps(comments_dict, ensure_ascii=False), int(record["id"])),
        )

        # 计算并回写“当前表维度”的小组总分（便于前端刷新展示）
        group_total_score = None
        if record_group_name:
            cursor.execute(
                "SELECT scores_json FROM ta_group_score_detail "
                "WHERE score_header_id = %s AND group_name = %s",
                (shid, record_group_name),
            )
            rows = cursor.fetchall() or []
            s = 0.0
            has_number = False
            for r in rows:
                raw_scores = (r or {}).get("scores_json")
                sd = {}
                if raw_scores:
                    if isinstance(raw_scores, str):
                        try:
                            sd = json.loads(raw_scores)
                        except Exception:
                            sd = {}
                    elif isinstance(raw_scores, dict):
                        sd = raw_scores
                if not isinstance(sd, dict):
                    sd = {}

                key_total = f"总分_{resolved_excel}" if resolved_excel else None
                ft = _to_float_or_none(sd.get(key_total)) if key_total else None
                if ft is None:
                    ft = _calc_total_for_excel(sd, resolved_excel)
                if ft is not None:
                    s += float(ft)
                    has_number = True
            group_total_score = float(s) if has_number else None

            cursor.execute(
                "UPDATE ta_group_score_detail "
                "SET group_total_score = %s, updated_at = NOW() "
                "WHERE score_header_id = %s AND group_name = %s",
                (group_total_score, shid, record_group_name),
            )

        connection.commit()

        return _return_with_log(
            {
                "message": "注释设置成功",
                "code": 200,
                "data": {
                    "score_header_id": shid,
                    "record_id": int(record["id"]),
                    "group_name": record.get("group_name"),
                    "student_id": record.get("student_id"),
                    "student_name": record.get("student_name"),
                    "field_name": field_name,
                    "excel_filename": resolved_excel,
                    "table_name": table_name,
                    "comment_key": comment_key,
                    "comment": comment,
                    "comments_json": comments_dict,
                    "group_total_score": group_total_score,
                },
            },
            status_code=200,
        )
    except mysql.connector.Error as e:
        if connection:
            try:
                connection.rollback()
            except Exception:
                pass
        app_logger.error(f"[group-scores/set-comment] Database error: {e}", exc_info=True)
        return _return_with_log({"message": f"数据库错误: {str(e)}", "code": 500}, status_code=500)
    except Exception as e:
        if connection:
            try:
                connection.rollback()
            except Exception:
                pass
        app_logger.error(f"[group-scores/set-comment] Unexpected error: {e}", exc_info=True)
        return _return_with_log({"message": f"未知错误: {str(e)}", "code": 500}, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if connection and connection.is_connected():
                connection.close()
        except Exception:
            pass


@router.get("/group-scores/get-student-attr")
async def api_get_group_student_attr(
    class_id: Optional[str] = Query(None, description="班级ID（与 group_id 二选一；也可两者都传）"),
    group_id: Optional[str] = Query(None, description="班级群ID（与 class_id 二选一；也可两者都传）"),
    term: Optional[str] = Query(None, description="学期（可选；不传则 term IS NULL）"),
    student_id: Optional[str] = Query(None, description="学号（可选，与 student_name 二选一）"),
    student_name: Optional[str] = Query(None, description="学生姓名（可选，与 student_id 二选一）"),
    field_name: Optional[str] = Query(None, description="字段名，如：纪律/早读/语文（可选；不传则查询全部字段）"),
    excel_filename: Optional[str] = Query(None, description="可选：Excel文件名，如：学生体质统计表.xlsx（不传则查询所有Excel表）"),
    table_name: Optional[str] = Query(None, description="可选：Excel表名基名，如：学生体质统计表（精确匹配到 *.xlsx；不传则查询所有Excel表）"),
):
    """小组成绩：查询某个学生的属性（值 + 注释）。
    - field_name 不传：查询全部字段
    - excel_filename/table_name 都不传：查询所有Excel表
    - 返回结构：如果查询全部，返回列表；如果查询特定字段，返回单个对象
    """
    sid = (str(student_id).strip() if student_id is not None else "") or None
    sname = (str(student_name).strip() if student_name is not None else "") or None
    ef = (str(excel_filename).strip() if excel_filename is not None else "") or None
    tn = (str(table_name).strip() if table_name is not None else "") or None
    fname = (str(field_name).strip() if field_name is not None else "") or None

    if not (sid or sname):
        return safe_json_response({"message": "缺少必要参数 student_id 或 student_name", "code": 400}, status_code=400)

    # 兼容：class_id / group_id 二选一；也可两者都传
    if not class_id and not group_id:
        return safe_json_response({"message": "缺少必要参数：class_id 或 group_id", "code": 400}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return safe_json_response({"message": "数据库连接失败", "code": 500}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        # 如果传了 group_id，优先尝试从 groups 表解析出对应的 classid
        resolved_class_id = class_id
        if group_id:
            try:
                cursor.execute("SELECT classid FROM `groups` WHERE group_id = %s LIMIT 1", (group_id,))
                group_row = cursor.fetchone()
                if group_row and group_row.get("classid"):
                    resolved_class_id_from_group = group_row.get("classid")
                    # 如果同时传了 class_id，验证一致性
                    if class_id:
                        if resolved_class_id_from_group != class_id:
                            return safe_json_response(
                                {"message": "参数不一致：class_id 与 group_id 对应的 classid 不一致", "code": 400},
                                status_code=400,
                            )
                    resolved_class_id = resolved_class_id_from_group
                else:
                    # 如果从 group_id 解析失败，且没有传 class_id，返回错误
                    if not class_id:
                        return safe_json_response(
                            {"message": "无法从 group_id 解析班级ID(classid)，请只传 class_id，或先在 groups 表补齐 classid", "code": 400},
                            status_code=400,
                        )
                    # 如果解析失败但有 class_id，使用 class_id
                    resolved_class_id = resolved_class_id or group_id
            except Exception as e:
                app_logger.error(f"[group-scores/get-student-attr] 解析 group_id 失败: {e}", exc_info=True)
                if not class_id:
                    return safe_json_response(
                        {"message": "无法从 group_id 解析班级ID(classid)，请只传 class_id", "code": 400},
                        status_code=400,
                    )
                resolved_class_id = resolved_class_id or group_id

        # 使用解析后的 class_id
        final_class_id = resolved_class_id or class_id
        if not final_class_id:
            return safe_json_response({"message": "无法确定班级ID，请提供 class_id 或有效的 group_id", "code": 400}, status_code=400)

        # header：class_id + term
        cursor.execute(
            "SELECT id, class_id, term, excel_file_url FROM ta_group_score_header "
            "WHERE class_id = %s AND ((%s IS NULL AND term IS NULL) OR term = %s) "
            "ORDER BY created_at DESC LIMIT 1",
            (final_class_id, term, term),
        )
        header_row = cursor.fetchone()
        if not header_row:
            return safe_json_response({"message": "没有找到数据：未找到小组成绩表头（请确认 class_id/term）", "code": 200}, status_code=200)

        shid = int(header_row["id"])

        # columns & excel filenames
        cursor.execute("SHOW COLUMNS FROM `ta_group_score_detail`")
        cols = [r.get("Field") for r in (cursor.fetchall() or []) if isinstance(r, dict)]

        excel_filenames: List[str] = []
        try:
            raw = header_row.get("excel_file_url")
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                excel_filenames = [str(k) for k in parsed.keys() if k]
        except Exception:
            excel_filenames = []

        def _resolve_excel() -> Optional[str]:
            """解析excel文件名，如果都不传返回None（表示查询所有）"""
            # 如果传了excel_filename，直接使用
            if ef:
                return ef
            # 如果传了table_name，尝试匹配
            if tn:
                base = tn[:-5] if tn.lower().endswith(".xlsx") else tn
                base = base.strip()
                for fn in excel_filenames:
                    b = fn[:-5] if fn.lower().endswith(".xlsx") else fn
                    if str(b).strip() == base:
                        return fn
                return None
            # 都不传，返回None表示查询所有
            return None

        resolved_excel = _resolve_excel()
        # 如果指定了excel_filename或table_name但解析失败，返回错误
        if (ef or tn) and not resolved_excel:
            return safe_json_response(
                {"message": "无法解析 excel_filename/table_name 到具体 Excel 文件名，请检查参数", "code": 400},
                status_code=400,
            )

        # record
        where = ["score_header_id = %s"]
        params: List[Any] = [shid]
        if sid:
            where.append("student_id = %s")
            params.append(sid)
        else:
            where.append("student_name = %s")
            params.append(sname)

        select_cols = ["id", "group_name", "student_id", "student_name", "scores_json", "total_score", "group_total_score"]
        if "comments_json" in cols:
            select_cols.append("comments_json")

        cursor.execute(
            f"SELECT {', '.join(select_cols)} FROM ta_group_score_detail WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT 1",
            tuple(params),
        )
        row = cursor.fetchone()
        if not row:
            return safe_json_response({"message": "没有找到数据：未找到该学生的小组成绩明细", "code": 200}, status_code=200)

        scores_raw = row.get("scores_json")
        scores_dict = json.loads(scores_raw) if isinstance(scores_raw, str) else (scores_raw or {})
        if not isinstance(scores_dict, dict):
            scores_dict = {}

        comments_dict = {}
        if "comments_json" in cols:
            comments_raw = row.get("comments_json")
            comments_dict = json.loads(comments_raw) if isinstance(comments_raw, str) else (comments_raw or {})
            if not isinstance(comments_dict, dict):
                comments_dict = {}

        # 确定要查询的excel文件列表
        target_excel_files: List[str] = []
        if resolved_excel:
            target_excel_files = [resolved_excel]
        else:
            target_excel_files = excel_filenames

        # 构建返回数据
        result_data: List[Dict[str, Any]] = []

        for excel_file in target_excel_files:
            if fname:
                # 查询特定字段
                score_key = f"{fname}_{excel_file}"
                comment_key = score_key
                value = scores_dict.get(score_key)
                comment_value = comments_dict.get(comment_key) if comments_dict else None

                result_data.append({
                    "score_header_id": shid,
                    "record_id": int(row["id"]),
                    "group_name": row.get("group_name"),
                    "student_id": row.get("student_id"),
                    "student_name": row.get("student_name"),
                    "field_name": fname,
                    "excel_filename": excel_file,
                    "table_name": tn if tn else (excel_file[:-5] if excel_file.lower().endswith(".xlsx") else excel_file),
                    "score_key": score_key,
                    "value": value,
                    "comment": comment_value,
                    "total_score": row.get("total_score"),
                    "group_total_score": row.get("group_total_score"),
                })
            else:
                # 查询所有字段：遍历scores_dict，找出属于当前excel_file的所有字段
                excel_base = excel_file[:-5] if excel_file.lower().endswith(".xlsx") else excel_file
                excel_base = excel_base.strip()
                
                for score_key, value in scores_dict.items():
                    # 检查score_key是否属于当前excel_file
                    # score_key格式：字段名_Excel文件名.xlsx
                    if score_key.endswith(f"_{excel_file}"):
                        field_name_part = score_key[:-(len(excel_file) + 1)]  # 去掉 _Excel文件名.xlsx
                        comment_key = score_key
                        comment_value = comments_dict.get(comment_key) if comments_dict else None

                        result_data.append({
                            "score_header_id": shid,
                            "record_id": int(row["id"]),
                            "group_name": row.get("group_name"),
                            "student_id": row.get("student_id"),
                            "student_name": row.get("student_name"),
                            "field_name": field_name_part,
                            "excel_filename": excel_file,
                            "table_name": excel_base,
                            "score_key": score_key,
                            "value": value,
                            "comment": comment_value,
                            "total_score": row.get("total_score"),
                            "group_total_score": row.get("group_total_score"),
                        })

        # 如果只查询一个特定字段和一个特定excel，返回单个对象；否则返回列表
        if fname and resolved_excel and len(result_data) == 1:
            return safe_json_response(
                {
                    "message": "查询成功",
                    "code": 200,
                    "data": result_data[0],
                }
            )
        else:
            return safe_json_response(
                {
                    "message": "查询成功",
                    "code": 200,
                    "data": result_data,
                    "count": len(result_data),
                }
            )
    except mysql.connector.Error as e:
        app_logger.error(f"[group-scores/get-student-attr] Database error: {e}", exc_info=True)
        return safe_json_response({"message": f"数据库错误: {str(e)}", "code": 500}, status_code=500)
    except Exception as e:
        app_logger.error(f"[group-scores/get-student-attr] Unexpected error: {e}", exc_info=True)
        return safe_json_response({"message": f"未知错误: {str(e)}", "code": 500}, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if connection and connection.is_connected():
                connection.close()
        except Exception:
            pass


@router.get("/group-scores/student-record")
async def api_get_group_student_record(
    class_id: str = Query(..., description="班级ID"),
    term: Optional[str] = Query(None, description="学期（可选；不传则 term IS NULL）"),
    student_id: Optional[str] = Query(None, description="学号（可选，与 student_name 二选一）"),
    student_name: Optional[str] = Query(None, description="学生姓名（可选，与 student_id 二选一）"),
    table_name: Optional[str] = Query(None, description="可选：Excel表名/文件基名，如：学生体质统计表（可带 .xlsx；支持变体）。不传则返回所有Excel表的数据"),
    limit: int = Query(20, ge=1, le=100, description="最多返回匹配的记录数（按 id 倒序），默认 20，最大 100"),
):
    """
    查询学生在小组表中的所有属性：
    - 如果提供 table_name：仅返回该 Excel 表的数据（精确匹配）
    - 如果不提供 table_name：返回该学生在所有 Excel 表中的所有字段
    
    从 scores_json（复合键名：字段名_Excel文件名）中筛选字段
    同时从 comments_json 中筛选对应注释（如果存在）
    """
    sid = str(student_id).strip() if student_id is not None else None
    sname = str(student_name).strip() if student_name is not None else None
    cid = str(class_id).strip() if class_id is not None else None
    
    if not sid and not sname:
        return safe_json_response({"message": "缺少必要参数：student_id 或 student_name", "code": 400}, status_code=400)
    
    if not cid:
        return safe_json_response({"message": "缺少必要参数：class_id", "code": 400}, status_code=400)
    
    table_base = None
    if table_name:
        table_base = _normalize_excel_table_name(table_name)
        if not table_base:
            return safe_json_response({"message": "table_name 格式无效", "code": 400}, status_code=400)
    
    connection = get_db_connection()
    if connection is None:
        return safe_json_response({"message": "数据库连接失败", "code": 500}, status_code=500)
    
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 查询小组成绩表头
        cursor.execute(
            "SELECT id, class_id, term, excel_file_url FROM ta_group_score_header "
            "WHERE class_id = %s AND ((%s IS NULL AND term IS NULL) OR term = %s) "
            "ORDER BY created_at DESC LIMIT 1",
            (cid, term, term),
        )
        header_row = cursor.fetchone()
        if not header_row:
            return safe_json_response({"message": "没有找到数据：未找到小组成绩表头（请确认 class_id/term）", "code": 200}, status_code=200)
        
        shid = int(header_row["id"])
        
        # 获取所有 Excel 文件名
        excel_filenames: List[str] = []
        try:
            raw = header_row.get("excel_file_url")
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                excel_filenames = [str(k) for k in parsed.keys() if k]
        except Exception:
            excel_filenames = []
        
        # 查询学生记录
        where_parts = ["score_header_id = %s"]
        params: List[Any] = [shid]
        
        if sid:
            where_parts.append("student_id = %s")
            params.append(sid)
        else:
            where_parts.append("student_name = %s")
            params.append(sname)
        
        # 检查是否有 comments_json 字段
        cursor.execute("SHOW COLUMNS FROM `ta_group_score_detail`")
        cols = [r.get("Field") for r in (cursor.fetchall() or []) if isinstance(r, dict)]
        has_comments_json = "comments_json" in cols
        
        select_cols = ["id", "group_name", "student_id", "student_name", "scores_json", "total_score", "group_total_score"]
        if has_comments_json:
            select_cols.append("comments_json")
        
        sql = f"SELECT {', '.join(select_cols)} FROM ta_group_score_detail WHERE {' AND '.join(where_parts)} ORDER BY id DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(sql, tuple(params))
        detail_rows = cursor.fetchall() or []
        
        if not detail_rows:
            return safe_json_response(
                {"message": "没有找到数据：未找到学生小组成绩明细", "code": 200, "data": {"table_name": table_base if table_base else "all", "rows": []}},
                status_code=200,
            )
        
        results: List[Dict[str, Any]] = []
        for row in detail_rows:
            # 解析 scores_json 和 comments_json
            scores_raw = row.get("scores_json")
            scores_data = json.loads(scores_raw) if isinstance(scores_raw, str) else (scores_raw or {})
            if not isinstance(scores_data, dict):
                scores_data = {}
            
            comments_data = {}
            if has_comments_json:
                comments_raw = row.get("comments_json")
                comments_data = json.loads(comments_raw) if isinstance(comments_raw, str) else (comments_raw or {})
                if not isinstance(comments_data, dict):
                    comments_data = {}
            
            # 如果指定了 table_name，进行过滤；否则返回所有 Excel 文件的数据
            if table_base:
                # 匹配 Excel 文件名
                matched_excel_filenames = []
                for fn in excel_filenames:
                    if _excel_filename_matches_table_name(fn, table_base):
                        matched_excel_filenames.append(fn)
                
                # 如果字段定义表里没有，尝试从 JSON key 中推断
                if not matched_excel_filenames:
                    for k in scores_data.keys():
                        if not isinstance(k, str):
                            continue
                        if k.endswith(".xlsx") and "_" in k:
                            maybe_excel = k.split("_")[-1]
                            if _excel_filename_matches_table_name(maybe_excel, table_base):
                                matched_excel_filenames.append(maybe_excel)
                    matched_excel_filenames = sorted(list(set(matched_excel_filenames)))
                
                if not matched_excel_filenames:
                    continue
            else:
                # 不指定 table_name，返回所有 Excel 文件的数据
                matched_excel_filenames = excel_filenames.copy()
                
                # 如果字段定义表里没有，从 JSON key 中提取
                if not matched_excel_filenames:
                    extracted_filenames = set()
                    for k in scores_data.keys():
                        if not isinstance(k, str):
                            continue
                        if k.endswith(".xlsx") and "_" in k:
                            maybe_excel = k.split("_")[-1]
                            extracted_filenames.add(maybe_excel)
                    matched_excel_filenames = sorted(list(extracted_filenames))
                
                if not matched_excel_filenames:
                    continue
            
            # 按 Excel 文件名分组提取字段
            scores_by_excel: Dict[str, Dict[str, Any]] = {}
            comments_by_excel: Dict[str, Dict[str, Any]] = {}
            
            for fn in matched_excel_filenames:
                suffix = f"_{fn}"
                scores_by_excel[fn] = {}
                comments_by_excel[fn] = {}
                
                for k, v in scores_data.items():
                    if isinstance(k, str) and k.endswith(suffix):
                        field_name = k[: -len(suffix)]
                        scores_by_excel[fn][field_name] = v
                
                if has_comments_json:
                    for k, v in comments_data.items():
                        if isinstance(k, str) and k.endswith(suffix):
                            field_name = k[: -len(suffix)]
                            comments_by_excel[fn][field_name] = v
            
            results.append(
                {
                    "id": row.get("id"),
                    "score_header_id": shid,
                    "group_name": row.get("group_name"),
                    "student_id": row.get("student_id"),
                    "student_name": row.get("student_name"),
                    "table_name": table_base if table_base else "all",
                    "matched_excel_filenames": matched_excel_filenames,
                    "scores": scores_by_excel,
                    "comments": comments_by_excel if has_comments_json else {},
                    "total_score": row.get("total_score"),
                    "group_total_score": row.get("group_total_score"),
                }
            )
        
        if not results:
            return safe_json_response(
                {
                    "message": "没有找到数据：未找到该 table_name 对应的数据（该学生存在，但该 Excel 表名未匹配到字段）",
                    "code": 200,
                    "data": {"table_name": table_base if table_base else "all", "rows": []},
                },
                status_code=200,
            )
        
        return safe_json_response({"message": "查询成功", "code": 200, "data": {"rows": results}})
    except mysql.connector.Error as e:
        app_logger.error(f"[group-scores/student-record] Database error: {e}", exc_info=True)
        return safe_json_response({"message": "数据库错误", "code": 500}, status_code=500)
    except Exception as e:
        app_logger.error(f"[group-scores/student-record] Unexpected error: {e}", exc_info=True)
        return safe_json_response({"message": f"未知错误: {str(e)}", "code": 500}, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        if connection and connection.is_connected():
            connection.close()


@router.get("/student-scores/student-record")
async def api_get_student_record_from_excel_table(
    student_id: Optional[str] = Query(None, description="学号（可选，与 student_name 二选一）"),
    student_name: Optional[str] = Query(None, description="学生姓名（可选，与 student_id 二选一）"),
    class_id: Optional[str] = Query(None, description="可选：班级ID(classid)，用于限定查询范围"),
    table_name: Optional[str] = Query(None, description="可选：Excel表名/文件基名，如：期中成绩单（可带 .xlsx；支持期中成绩单1/2 等变体）。不传则返回所有Excel表的数据"),
    field_name: Optional[str] = Query(None, description="可选：字段名/科目名，如：语文、数学、纪律。不传则返回所有字段"),
    score_header_id: Optional[int] = Query(None, description="可选：限定在某个成绩表(score_header_id)内查询"),
    limit: int = Query(20, ge=1, le=100, description="最多返回匹配的成绩明细行数（按 id 倒序），默认 20，最大 100"),
):
    """
    查询学生在成绩表中的属性：
    - 如果提供 table_name：仅返回该 Excel 表的数据（精确匹配）
    - 如果不提供 table_name：返回该学生在所有 Excel 表中的数据
    - 如果提供 field_name：仅返回该字段/科目的数据（在所有匹配的Excel表中）
    - 如果不提供 field_name：返回所有字段的数据
    
    从 scores_json（复合键名：字段名_Excel文件名）中筛选字段
    同时从 comments_json 中筛选对应注释（如果存在）

    例：
    - student_name=张三&field_name=语文 -> 查询张三的语文成绩（在所有Excel表中）
    - student_name=张三&table_name=期中成绩单 -> 查询张三在期中成绩单中的所有字段
    - student_name=张三 -> 查询张三的所有字段（所有Excel表）
    """
    table_base = None
    if table_name:
        table_base = _normalize_excel_table_name(table_name)
        if not table_base:
            return safe_json_response({"message": "table_name 格式无效", "code": 400}, status_code=400)

    field_name_filter = None
    if field_name:
        field_name_filter = str(field_name).strip()
        if not field_name_filter:
            return safe_json_response({"message": "field_name 不能为空", "code": 400}, status_code=400)

    sid = str(student_id).strip() if student_id is not None else None
    sname = str(student_name).strip() if student_name is not None else None
    cid = str(class_id).strip() if class_id is not None else None
    if not sid and not sname:
        return safe_json_response({"message": "缺少必要参数：student_id 或 student_name", "code": 400}, status_code=400)

    connection = get_db_connection()
    if connection is None:
        return safe_json_response({"message": "数据库连接失败", "code": 500}, status_code=500)

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)

        student_where_parts: List[str] = []
        student_params: List[Any] = []
        if sid:
            student_where_parts.append("student_id = %s")
            student_params.append(sid)
        if sname:
            student_where_parts.append("student_name = %s")
            student_params.append(sname)

        student_where_sql = " OR ".join(student_where_parts)
        if len(student_where_parts) > 1:
            student_where_sql = f"({student_where_sql})"

        if score_header_id is not None:
            where_sql = f"{student_where_sql} AND score_header_id = %s"
            params: List[Any] = [*student_params, int(score_header_id)]
        else:
            where_sql = student_where_sql
            params = [*student_params]

        # 可选：通过表头过滤 class_id（score_header_id -> ta_student_score_header.id）
        if cid:
            sql = (
                "SELECT d.id, d.score_header_id, d.student_id, d.student_name, d.scores_json, d.comments_json, "
                "d.total_score, d.created_at, d.updated_at "
                "FROM ta_student_score_detail d "
                "JOIN ta_student_score_header h ON d.score_header_id = h.id "
                f"WHERE {where_sql} AND h.class_id = %s "
                "ORDER BY d.id DESC "
                "LIMIT %s"
            )
            params.append(cid)
        else:
            sql = (
                "SELECT id, score_header_id, student_id, student_name, scores_json, comments_json, total_score, created_at, updated_at "
                "FROM ta_student_score_detail "
                f"WHERE {where_sql} "
                "ORDER BY id DESC "
                "LIMIT %s"
            )
        params.append(limit)

        cursor.execute(sql, tuple(params))
        detail_rows = cursor.fetchall() or []
        if not detail_rows:
            return safe_json_response(
                {"message": "没有找到数据：未找到学生成绩明细", "code": 200, "data": {"table_name": table_base if table_base else "all", "rows": []}},
                status_code=200,
            )

        # 缓存每个 score_header_id 的 excel_filename 列表（来自字段定义表）
        excel_filenames_cache: Dict[int, List[str]] = {}

        results: List[Dict[str, Any]] = []
        for row in detail_rows:
            shid = row.get("score_header_id")
            if shid is None:
                continue
            shid_int = int(shid)

            if shid_int not in excel_filenames_cache:
                cursor.execute(
                    "SELECT DISTINCT excel_filename "
                    "FROM ta_student_score_field "
                    "WHERE score_header_id = %s AND excel_filename IS NOT NULL AND excel_filename != ''",
                    (shid_int,),
                )
                excel_rows = cursor.fetchall() or []
                excel_filenames_cache[shid_int] = [
                    (er.get("excel_filename") if isinstance(er, dict) else None) for er in excel_rows
                ]
                excel_filenames_cache[shid_int] = [fn for fn in excel_filenames_cache[shid_int] if fn]

            excel_filenames = excel_filenames_cache.get(shid_int, [])
            
            # 如果指定了 table_name，则进行过滤；否则返回所有 Excel 文件的数据
            if table_base:
                matched_excel_filenames = [fn for fn in excel_filenames if _excel_filename_matches_table_name(fn, table_base)]

                # 如果字段定义表里没有 excel_filename（极端情况），尝试从 JSON key 中推断（仅作为兜底）
                if not matched_excel_filenames:
                    try:
                        scores_raw = row.get("scores_json")
                        scores_data = json.loads(scores_raw) if isinstance(scores_raw, str) else (scores_raw or {})
                        if isinstance(scores_data, dict):
                            for k in scores_data.keys():
                                if not isinstance(k, str):
                                    continue
                                # 兜底假设 key 格式为 <field>_<excel>.xlsx（excel 里不含下划线）
                                if k.endswith(".xlsx") and "_" in k:
                                    maybe_excel = k.split("_")[-1]
                                    if _excel_filename_matches_table_name(maybe_excel, table_base):
                                        matched_excel_filenames.append(maybe_excel)
                        matched_excel_filenames = sorted(list(set(matched_excel_filenames)))
                    except Exception:
                        matched_excel_filenames = []

                if not matched_excel_filenames:
                    # 该明细行不包含目标 table_name 的字段，跳过
                    continue
            else:
                # 不指定 table_name，返回所有 Excel 文件的数据
                # 先从字段定义表获取，如果为空则从 JSON key 中提取
                matched_excel_filenames = excel_filenames.copy()
                
                if not matched_excel_filenames:
                    # 从 scores_json 中提取所有 Excel 文件名
                    try:
                        scores_raw = row.get("scores_json")
                        scores_data = json.loads(scores_raw) if isinstance(scores_raw, str) else (scores_raw or {})
                        if isinstance(scores_data, dict):
                            extracted_filenames = set()
                            for k in scores_data.keys():
                                if not isinstance(k, str):
                                    continue
                                # key 格式为 <field>_<excel>.xlsx
                                if k.endswith(".xlsx") and "_" in k:
                                    maybe_excel = k.split("_")[-1]
                                    extracted_filenames.add(maybe_excel)
                            matched_excel_filenames = sorted(list(extracted_filenames))
                    except Exception:
                        matched_excel_filenames = []
                
                # 如果仍然为空，跳过该行
                if not matched_excel_filenames:
                    continue

            # 解析 scores_json / comments_json，并按 matched_excel_filenames 过滤
            scores_raw = row.get("scores_json")
            scores_data = json.loads(scores_raw) if isinstance(scores_raw, str) else (scores_raw or {})
            if not isinstance(scores_data, dict):
                scores_data = {}

            comments_raw = row.get("comments_json")
            comments_data = json.loads(comments_raw) if isinstance(comments_raw, str) else (comments_raw or {})
            if not isinstance(comments_data, dict):
                comments_data = {}

            scores_by_excel: Dict[str, Dict[str, Any]] = {}
            comments_by_excel: Dict[str, Dict[str, Any]] = {}

            for fn in matched_excel_filenames:
                suffix = f"_{fn}"
                scores_by_excel[fn] = {}
                comments_by_excel[fn] = {}

                for k, v in scores_data.items():
                    if isinstance(k, str) and k.endswith(suffix):
                        extracted_field_name = k[: -len(suffix)]
                        # 如果指定了 field_name，只返回匹配的字段
                        if field_name_filter and extracted_field_name != field_name_filter:
                            continue
                        scores_by_excel[fn][extracted_field_name] = v

                for k, v in comments_data.items():
                    if isinstance(k, str) and k.endswith(suffix):
                        extracted_field_name = k[: -len(suffix)]
                        # 如果指定了 field_name，只返回匹配的字段
                        if field_name_filter and extracted_field_name != field_name_filter:
                            continue
                        comments_by_excel[fn][extracted_field_name] = v
                
                # 如果指定了 field_name 但该Excel文件中没有该字段，移除该Excel文件
                if field_name_filter and not scores_by_excel[fn]:
                    del scores_by_excel[fn]
                    if fn in comments_by_excel:
                        del comments_by_excel[fn]
                    continue
            
            # 如果指定了 field_name 但没有任何匹配的数据，跳过该记录
            if field_name_filter and not scores_by_excel:
                continue
            
            # 更新 matched_excel_filenames，只包含实际有数据的Excel文件
            actual_matched_filenames = list(scores_by_excel.keys())

            results.append(
                {
                    "id": row.get("id"),
                    "score_header_id": shid_int,
                    "student_id": row.get("student_id"),
                    "student_name": row.get("student_name"),
                    "table_name": table_base if table_base else "all",  # 如果不指定table_name，返回"all"
                    "field_name": field_name_filter if field_name_filter else "all",  # 如果不指定field_name，返回"all"
                    "matched_excel_filenames": actual_matched_filenames,
                    "scores": scores_by_excel,
                    "comments": comments_by_excel,
                }
            )

        if not results:
            # 给出一些可用的 excel_filename 供排查
            available_by_header: Dict[str, List[str]] = {}
            for shid, fns in excel_filenames_cache.items():
                available_by_header[str(shid)] = fns
            return safe_json_response(
                {
                    "message": "没有找到数据：未找到该 table_name 对应的数据（该学生存在，但该 Excel 表名未匹配到字段）",
                    "code": 200,
                    "data": {"table_name": table_base, "available_excel_filenames_by_score_header_id": available_by_header},
                },
                status_code=200,
            )

        return safe_json_response({"message": "查询成功", "code": 200, "data": {"rows": results}})
    except mysql.connector.Error as e:
        app_logger.error(f"[student-record] Database error: {e}", exc_info=True)
        return safe_json_response({"message": "数据库错误", "code": 500}, status_code=500)
    except Exception as e:
        app_logger.error(f"[student-record] Unexpected error: {e}", exc_info=True)
        return safe_json_response({"message": f"未知错误: {str(e)}", "code": 500}, status_code=500)
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        if connection and connection.is_connected():
            connection.close()

