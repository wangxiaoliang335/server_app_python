import json
from typing import Any, Dict, List

from mysql.connector import Error


def normalize_teachings_payload(data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    归一化“任教关系列表”输入，统一输出：
    [
      {"grade_level": "...", "grade": "...", "subject": "...", "class_taught": "..."}
    ]
    兼容入参：
    - teachings / teaching_assignments / teachingAssignments
    - 以及旧字段：grade_level/grade/subject/class_taught（作为兜底的一条）
    """
    raw = data.get("teachings") or data.get("teaching_assignments") or data.get("teachingAssignments")

    # 允许前端把列表当字符串传
    if isinstance(raw, str) and raw.strip():
        try:
            raw = json.loads(raw)
        except Exception:
            raw = None

    items: List[Dict[str, Any]] = raw if isinstance(raw, list) else []

    def _get_str(v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip()

    teachings: List[Dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        grade_level = _get_str(item.get("grade_level") or item.get("gradeLevel"))
        grade = _get_str(item.get("grade"))
        subject = _get_str(item.get("subject"))
        class_taught = _get_str(item.get("class_taught") or item.get("classTaught"))
        if not (grade_level or grade or subject or class_taught):
            continue
        teachings.append(
            {
                "grade_level": grade_level,
                "grade": grade,
                "subject": subject,
                "class_taught": class_taught,
            }
        )

    # 兜底：旧字段作为一条任教关系
    if not teachings:
        fallback = {
            "grade_level": _get_str(data.get("grade_level")),
            "grade": _get_str(data.get("grade")),
            "subject": _get_str(data.get("subject")),
            "class_taught": _get_str(data.get("class_taught")),
        }
        if any(fallback.values()):
            teachings.append(fallback)

    # 去重（保持顺序）
    seen = set()
    deduped: List[Dict[str, str]] = []
    for t in teachings:
        key = (
            t.get("grade_level", ""),
            t.get("grade", ""),
            t.get("subject", ""),
            t.get("class_taught", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(t)
    return deduped


def replace_user_teachings(cursor, phone: str, teachings: List[Dict[str, str]]) -> None:
    """
    用 teach 关系表保存“一个老师多条任教记录”。
    - 采用 delete + insert（带 sort_order），保持前端顺序
    """
    phone = str(phone or "").strip()
    if not phone:
        return

    try:
        cursor.execute("DELETE FROM ta_user_teachings WHERE phone = %s", (phone,))
        if teachings:
            insert_sql = """
                INSERT INTO ta_user_teachings
                (phone, grade_level, grade, subject, class_taught, sort_order)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            for idx, t in enumerate(teachings):
                cursor.execute(
                    insert_sql,
                    (
                        phone,
                        (t.get("grade_level") or "").strip(),
                        (t.get("grade") or "").strip(),
                        (t.get("subject") or "").strip(),
                        (t.get("class_taught") or "").strip(),
                        int(idx),
                    ),
                )
    except Error:
        raise


