# app.py
import os
from flask import Flask, request, jsonify, session
import mysql.connector
from mysql.connector import Error
import hashlib
import secrets
import datetime
import random
import string
import logging
import time
import base64
import os
import redis
from logging.handlers import TimedRotatingFileHandler

from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest

from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

IMAGE_DIR = "/var/www/images"  # 存头像的目录

if not os.path.exists('logs'):
    os.makedirs('logs')

app = Flask(__name__)
# 设置 Flask Session 密钥
#app.secret_key = 'a1b2c3d4e5f67890123456789012345678901234567890123456789012345678'
app.secret_key = os.getenv("FLASK_SECRET_KEY", "default_key")

# 创建一个 TimedRotatingFileHandler，每天 (midnight) 轮转，保留 30 天的日志
file_handler = TimedRotatingFileHandler(
    filename='logs/app.log',
    when='midnight',
    interval=1,
    backupCount=30,
    encoding='utf-8'
)

formatter = logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
file_handler.setFormatter(formatter)

app_logger = logging.getLogger('teacher-assistant')
app_logger.setLevel(logging.INFO)
app_logger.addHandler(file_handler)
app_logger.propagate = False

DB_CONFIG = {
    'host': 'rm-uf65y451aa995i174io.mysql.rds.aliyuncs.com',
    'database': 'teacher_assistant',
    'user': 'ta_user',
    'password': 'Ta_0909DB&'
}

# 短信服务配置 (模拟)
# SMS_CONFIG = {
#     'access_key_id': 'LTAI5tHt3ejFCgp5Qi4gjg2w',
#     'access_key_secret': 'itqsnPgUti737u0JdQ7WJTHHFeJyHv',
#     'sign_name': '临沂师悦数字科技有限公司',
#     'template_code': 'SMS_325560474'
# }

SMS_CONFIG = {
    'access_key_id': os.getenv("ALIYUN_AK_ID"),
    'access_key_secret': os.getenv("ALIYUN_AK_SECRET"),
    'sign_name': os.getenv("ALIYUN_SMS_SIGN"),
    'template_code': os.getenv("ALIYUN_SMS_TEMPLATE")
}

# 验证码有效期 (秒)
VERIFICATION_CODE_EXPIRY = 300 # 5分钟

from werkzeug.utils import secure_filename

# 根上传目录
UPLOAD_FOLDER = './uploads/audio'
ALLOWED_EXTENSIONS = {'mp3', 'wav', 'aac', 'ogg', 'm4a'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_daily_upload_folder():
    """
    获取当天的上传子目录，如 ./uploads/audio/2025-09-13
    """
    today = datetime.now().strftime('%Y-%m-%d')
    daily_folder = os.path.join(UPLOAD_FOLDER, today)
    os.makedirs(daily_folder, exist_ok=True)
    return daily_folder

def get_db_connection():
    """获取数据库连接"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        app_logger.info("Database connection established.")
        return connection
    except Error as e:
        app_logger.error(f"Error connecting to MySQL: {e}")
        return None

def hash_password(password, salt):
    return hashlib.sha256((password + salt).encode('utf-8')).hexdigest()

def generate_verification_code(length=6):
    return ''.join(random.choices(string.digits, k=length))

def send_sms_verification_code(phone, code):
    client = AcsClient(SMS_CONFIG['access_key_id'], SMS_CONFIG['access_key_secret'], 'cn-hangzhou')
    request = CommonRequest()
    request.set_accept_format('json')
    request.set_domain('dysmsapi.aliyuncs.com')
    request.set_method('POST')
    request.set_protocol_type('https')
    request.set_version('2017-05-25')
    request.set_action_name('SendSms')
    request.add_query_param('RegionId', "cn-hangzhou")
    request.add_query_param('PhoneNumbers', phone)
    request.add_query_param('SignName', SMS_CONFIG['sign_name'])
    request.add_query_param('TemplateCode', SMS_CONFIG['template_code'])
    request.add_query_param('TemplateParam', f"{{\"code\":\"{code}\"}}")
    response = client.do_action_with_exception(request)
    print(str(response, encoding='utf-8'))
    return True

    # 模拟发送成功
    app_logger.info(f"手机号: {phone}, 验证码: {code}")
    return True

@app.before_request
def log_request_info():
    app_logger.info(f"Incoming request: {request.method} {request.url} from {request.remote_addr}")

def verify_code_from_session(input_phone, input_code):
    stored_data = session.get('verification_code')
    if not stored_data:
        app_logger.warning(f"Verification failed for {input_phone}: No code sent or expired.")
        return False, "未发送验证码或验证码已过期"

    if stored_data['phone'] != input_phone:
        app_logger.warning(f"Verification failed for {input_phone}: Phone number mismatch.")
        return False, "手机号不匹配"

    #if datetime.datetime.now() > stored_data['expires_at']:
    if time.time() > stored_data['expires_at']:
        session.pop('verification_code', None)
        app_logger.info(f"Verification code expired for {input_phone}.")
        return False, "验证码已过期"

    if stored_data['code'] != input_code:
        app_logger.warning(f"Verification failed for {input_phone}: Incorrect code entered.")
        return False, "验证码错误"

    session.pop('verification_code', None)
    app_logger.info(f"Verification successful for {input_phone}.")
    return True, "验证成功"

# Redis 连接
r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

def get_max_code_from_mysql(connection):
    #"""从 MySQL 找最大号码"""
    print(" get_max_code_from_mysql 111\n");
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("SELECT MAX(id) AS max_id FROM ta_school")
        print(" get_max_code_from_mysql 222\n");
        row = cursor.fetchone()
        #row = cursor.fetchone()[0]
        print(" get_max_code_from_mysql 333\n", row);
        if row and row['max_id'] is not None:
            return int(row['max_id'])
        return 0

def generate_unique_code():
    #"""生成唯一 6 位数字"""
    connection = get_db_connection()
    if connection is None:
        app_logger.error("List schools failed: Database connection error.")
        print(" 数据库连接失败\n");
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'schools': []
            }
        }), 500

    print(" generate_unique_code 111\n");

    # 先从 Redis 缓存取
    max_code = r.get("unique_max_code")
    if max_code:
        new_code = int(max_code) + 1
    else:
        # Redis 没缓存，从 MySQL 查
        new_code = get_max_code_from_mysql(connection) + 1

    print(" get_max_code_from_mysql leave");
    if new_code >= 1000000:
        raise ValueError("6位数字已用完")

    code_str = f"{new_code:06d}"

    print(" INSERT INTO ta_school\n");

    cursor = None
    # 写入 MySQL
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("INSERT INTO ta_school (id) VALUES (%s)", (new_code,))
        connection.commit()
        cursor.close()
    except mysql.connector.errors.IntegrityError:
        # 如果主键冲突，递归重试
            return generate_unique_code()
    finally:
        if connection and connection.is_connected():
            connection.close()

        # 更新 Redis 缓存
    r.set("unique_max_code", new_code)
    print(" INSERT INTO code_str:", code_str, "\n");
    return code_str

@app.route('/unique6digit', methods=['GET'])
def unique_code_api():
    try:
        code = generate_unique_code()
        return jsonify({"code": code, "status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e), "status": "fail"}), 500

@app.route('/schools', methods=['GET'])
def list_schools():
    """
    获取学校列表 (支持根据学校名称进行模糊搜索 或 根据学校ID精确查询)
    Query Parameters:
        - name (str, optional): 学校名称，用于模糊搜索
        - id (int, optional): 学校ID，用于精确查询
    Returns:
        JSON: 包含状态信息和学校列表数据的响应
             { "data": { "message": "...", "code": ..., "schools": [...] } }
    注意: 如果同时提供了 name 和 id, id 优先。
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("List schools failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'schools': []
            }
        }), 500

    cursor = None
    try:
        # 1. 获取并解析查询参数
        school_id = request.args.get('id', type=int)
        name_filter = request.args.get('name', type=str)

        # 2. 构建 SQL 查询
        base_columns = "id, name, address"
        base_query = f"SELECT {base_columns} FROM ta_school WHERE 1=1"
        filters = []
        params = []

        # 优先根据 ID 查询
        if school_id is not None:
            filters.append("AND id = %s")
            params.append(school_id)
        # 如果没有 ID，则根据名称模糊搜索
        elif name_filter:
            filters.append("AND name LIKE %s")
            params.append(f"%{name_filter}%")

        # 3. 执行查询
        final_query = base_query + " " + " ".join(filters)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        schools = cursor.fetchall()

        # 4. 返回 JSON 响应 (包裹在 data 对象中)
        app_logger.info(f"Fetched {len(schools)} schools.")
        return jsonify({
            'data': {
                'message': '获取学校列表成功',
                'code': 200,
                'schools': schools
            }
        }), 200

    except Error as e:
        app_logger.error(f"Database error during fetching schools: {e}")
        return jsonify({
            'data': {
                'message': '获取学校列表失败',
                'code': 500,
                'schools': []
            }
        }), 500
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching schools: {e}")
        return jsonify({
            'data': {
                'message': '内部服务器错误',
                'code': 500,
                'schools': []
            }
        }), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching schools.")

@app.route('/updateUserInfo', methods=['POST'])
def updateUserInfo():
    data = request.get_json()
    phone = data.get('phone')
    id_number = data.get('id_number')
    avatar = data.get('avatar')

    if not id_number or (not avatar):
        app_logger.warning("Login failed: Missing phone and either password or verification code.")
        return jsonify({
            'data': {
                'message': '身份证号码和头像必须提供',
                'code': 400
            }
        }), 400

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Login failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }), 500
		
		# 解码成二进制
    avatar_bytes = base64.b64decode(avatar)
	
	# 生成文件名，例如 Alice_时间戳.jpg
    filename = f"{id_number}_.png"
    file_path = os.path.join(IMAGE_DIR, filename)

    # 保存到文件系统
    with open(file_path, "wb") as f:
        f.write(avatar_bytes)

    cursor = None
    try:
        update_query = "UPDATE ta_user_details SET avatar = %s WHERE id_number = %s"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(update_query, (file_path, id_number))
        connection.commit()
        cursor.close()
        return jsonify({
            'data': {
                'message': '更新成功',
                'code': 200,
                #'user_id': user['id'] # 返回用户ID
            }
        }), 200

    except Error as e:
        app_logger.error(f"Database error during login for {phone}: {e}")
        return jsonify({
            'data': {
                'message': '更新失败',
                'code': 500
            }
        }), 500
    finally:
        if connection and connection.is_connected():
            connection.close()
            # app_logger.info("Database connection closed after login attempt.")

@app.route('/userInfo', methods=['GET'])
def list_userInfo():
    """
    获取用户信息 (支持手机号码精确查询)
    Query Parameters:
        - phone (str, optional): 手机号码，用于模糊搜索
    Returns:
        JSON: 包含状态信息和用户数据的响应
             { "data": { "message": "...", "code": ..., "userinfo": [...] } }
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("Get User Info failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'schools': []
            }
        }), 500

    cursor = None
    try:
        # 1. 获取并解析查询参数
        phone_filter = request.args.get('phone', type=str)

        print(" 111111\n");
        print(phone_filter);
        print(" 222222\n");

        # 2. 构建 SQL 查询
        #base_columns = "id, name, address"
        base_query = f"SELECT * FROM ta_user_details WHERE "
        filters = []
        params = []

        filters.append("phone = %s")
        params.append(phone_filter)

        # # 优先根据 ID 查询
        # if school_id is not None:
        #     filters.append("AND id = %s")
        #     params.append(school_id)
        # # 如果没有 ID，则根据名称模糊搜索
        # elif name_filter:
        #     filters.append("AND name LIKE %s")
        #     params.append(f"%{name_filter}%")

        # 3. 执行查询
        final_query = base_query + " " + " ".join(filters)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        userinfo = cursor.fetchall()

         # 遍历每一条记录，读取 PNG 转 Base64
        for user in userinfo:
            avatar_path = user.get("avatar")  # 数据库里的文件名
            if avatar_path:
                full_path = os.path.join(IMAGE_DIR, avatar_path)
                if os.path.exists(full_path):
                    try:
                        with open(full_path, "rb") as img_file:
                            b64_str = base64.b64encode(img_file.read()).decode('utf-8')
                            user["avatar_base64"] = b64_str  # 新字段
                    except Exception as e:
                        app_logger.error(f"读取图片失败 {full_path}: {e}")
                        user["avatar_base64"] = None
                else:
                    user["avatar_base64"] = None
            else:
                user["avatar_base64"] = None

        # 4. 返回 JSON 响应 (包裹在 data 对象中)
        app_logger.info(f"Fetched {len(userinfo)} userinfo.")
        return jsonify({
            'data': {
                'message': '获取用户信息成功',
                'code': 200,
                'userinfo': userinfo
            }
        }), 200

    except Error as e:
        app_logger.error(f"Database error during fetching userinfo: {e}")
        return jsonify({
            'data': {
                'message': '获取用户信息失败',
                'code': 500,
                'userinfo': []
            }
        }), 500
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching userinfo: {e}")
        return jsonify({
            'data': {
                'message': '内部服务器错误',
                'code': 500,
                'userinfo': []
            }
        }), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching userinfo.")


@app.route('/updateClasses', methods=['POST'])
def updateClasses():
    data_list = request.get_json()  # 接收到的 JSON 数组

    if not isinstance(data_list, list) or len(data_list) == 0:
        return jsonify({
            'data': {
                'message': '必须提供班级数组数据',
                'code': 400
            }
        }), 400

    connection = get_db_connection()
    if connection is None:
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }), 500

    try:
        cursor = connection.cursor()

        # MySQL Upsert语句
        sql = """
        INSERT INTO ta_classes (
            class_code,
            school_stage,
            grade,
            class_name,
            remark,
            created_at
        ) VALUES (%s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            school_stage = VALUES(school_stage),
            grade        = VALUES(grade),
            class_name   = VALUES(class_name),
            remark       = VALUES(remark),
            created_at   = VALUES(created_at);
        """

        # 批量执行
        values = []
        for item in data_list:
            class_code   = item.get('class_code')
            school_stage = item.get('school_stage')
            grade        = item.get('grade')
            class_name   = item.get('class_name')
            remark       = item.get('remark')

            if not class_code:
                continue  # 没有主键跳过

            values.append((
                class_code,
                school_stage,
                grade,
                class_name,
                remark
            ))

        if values:
            cursor.executemany(sql, values)
            connection.commit()

        cursor.close()
        connection.close()

        return jsonify({
            'data': {
                'message': '批量插入/更新完成',
                'code': 200,
                'count': len(values)
            }
        }), 200

    except Error as e:
        return jsonify({
            'data': {
                'message': f'数据库操作失败: {e}',
                'code': 500
            }
        }), 500

@app.route('/getClassesByPrefix', methods=['POST'])
def get_classes_by_prefix():
    data = request.get_json()
    prefix = data.get("prefix")

    # 参数校验
    if not prefix or len(prefix) != 6 or not prefix.isdigit():
        return jsonify({
            "data": {
                "message": "必须提供6位数字前缀",
                "code": 400
            }
        }), 400

    connection = get_db_connection()
    if connection is None:
        return jsonify({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }), 500

    try:
        cursor = connection.cursor(dictionary=True)  # 返回字典型结果
        sql = """
        SELECT class_code, school_stage, grade, class_name, remark, created_at
        FROM ta_classes
        WHERE LEFT(class_code, 6) = %s
        """
        cursor.execute(sql, (prefix,))
        results = cursor.fetchall()

        cursor.close()
        connection.close()

        return jsonify({
            "data": {
                "message": "查询成功",
                "code": 200,
                "count": len(results),
                "classes": results
            }
        }), 200

    except Error as e:
        app.logger.error(f"查询失败: {e}")
        return jsonify({
            "data": {
                "message": "查询失败",
                "code": 500
            }
        }), 500


@app.route('/updateSchoolInfo', methods=['POST'])
def updateSchoolInfo():
    data = request.get_json()
    id = data.get('id')
    name = data.get('name')
    address = data.get('address')

    if not id:
        app_logger.warning("Login failed: Missing id.")
        return jsonify({
            'data': {
                'message': 'id值必须提供',
                'code': 400
            }
        }), 400

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Login failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }), 500

    cursor = None
    try:
        update_query = "UPDATE ta_school SET name = %s, address = %s WHERE id = %s"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(update_query, (name, address, id))
        connection.commit()
        cursor.close()
        return jsonify({
            'data': {
                'message': '更新成功',
                'code': 200,
                #'user_id': user['id'] # 返回用户ID
            }
        }), 200

    except Error as e:
        app_logger.error(f"Database error during login for {name}: {e}")
        return jsonify({
            'data': {
                'message': '更新失败',
                'code': 500
            }
        }), 500
    finally:
        if connection and connection.is_connected():
            connection.close()
            # app_logger.info("Database connection closed after login attempt.")

# # 生成教师唯一编号
# def generate_teacher_unique_id(school_id):
#     connection = get_db_connection()
#     if connection is None:
#         return None
#     cursor = None
#     try:
#         print(" generate_teacher_unique_id 00\n");
#         cursor = connection.cursor()

#         print(" generate_teacher_unique_id 01:", school_id, "\n");
#         cursor.execute("""
#             SELECT MAX(teacher_unique_id) 
#             FROM ta_teacher 
#             WHERE schoolId = %s
#         """, (school_id,))
#         print(" generate_teacher_unique_id 10\n");
#         result = cursor.fetchone()
#         print(" generate_teacher_unique_id 11", result, "\n");
#         if result and result[0]:
#             last_num = int(str(result[0])[6:])
#             new_num = last_num + 1
#         else:
#             new_num = 1

#         return int(f"{school_id}{str(new_num).zfill(4)}")
#     except Error as e:
#         app_logger.error(f"Error generating teacher_unique_id: {e}")
#         return None
#     finally:
#         if cursor:
#             cursor.close()
#         if connection and connection.is_connected():
#             connection.close()

def generate_teacher_unique_id(school_id):
    """
    并发安全生成 teacher_unique_id
    格式：前6位为schoolId（左补零），后4位为流水号（左补零），总长度10位
    """
    connection = get_db_connection()
    if connection is None:
        return None
    cursor = None

    try:
        cursor = connection.cursor()

        # 开启事务，锁定当前学校ID的记录，防并发冲突
        connection.start_transaction()

        cursor.execute("""
            SELECT MAX(teacher_unique_id)
            FROM ta_teacher
            WHERE schoolId = %s
            FOR UPDATE
        """, (school_id,))
        result = cursor.fetchone()

        if result and result[0]:
            # 补零到长度10位
            max_id_str = str(result[0]).zfill(10)
            # 取后4位流水号部分
            last_num = int(max_id_str[6:])
            new_num = last_num + 1
        else:
            new_num = 1

        # 拼接最终教师唯一编号
        teacher_unique_id_str = f"{str(school_id).zfill(6)}{str(new_num).zfill(4)}"
        return int(teacher_unique_id_str)

    except Error as e:
        app_logger.error(f"Error generating teacher_unique_id: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


# 📌 新增教师接口
@app.route("/add_teacher", methods=["POST"])
def add_teacher():
    data = request.json
    if not data or 'schoolId' not in data:
        return jsonify({
            'data': {
                'message': '缺少 schoolId',
                'code': 400
            }
        }), 400

    print("  000000000\n");
    school_id = data['schoolId']
    print("  0000000001111\n");
    teacher_unique_id = generate_teacher_unique_id(school_id)
    if teacher_unique_id is None:
        return jsonify({
            'data': {
                'message': '生成教师唯一编号失败',
                'code': 500
            }
        }), 500

    print("  11111111", teacher_unique_id, "\n");
    connection = get_db_connection()
    if connection is None:
        app_logger.error("Add teacher failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }), 500

    print("  22222222\n");

    is_admin_flag = data.get('is_Administarator')
    try:
        # 如果是布尔值 True/False，转成 1/0
        if isinstance(is_admin_flag, bool):
            is_admin_flag = int(is_admin_flag)
        else:
            # 如果是字符串，比如 "1" 或 "0"，也转成 int
            is_admin_flag = int(is_admin_flag) if is_admin_flag is not None else 0
    except ValueError:
        is_admin_flag = 0  # 不能转换就给默认值

    print("  22222222:", is_admin_flag);
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        # 插入记录
        sql_insert = """
        INSERT INTO ta_teacher 
        (name, icon, subject, gradeId, schoolId, is_Administarator, phone, id_card, sex, 
         teaching_tenure, education, graduation_institution, major, 
         teacher_certification_level, subjects_of_teacher_qualification_examination, 
         educational_stage, teacher_unique_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s)
        """
        cursor.execute(sql_insert, (
            data.get('name'),
            data.get('icon'),
            data.get('subject'),
            data.get('gradeId'),
            school_id,
            is_admin_flag,
            data.get('phone'),
            data.get('id_card'),
            data.get('sex'),
            data.get('teaching_tenure'),
            data.get('education'),
            data.get('graduation_institution'),
            data.get('major'),
            data.get('teacher_certification_level'),
            data.get('subjects_of_teacher_qualification_examination'),
            data.get('educational_stage'),
            teacher_unique_id
        ))

        print("  33333333333\n");
        connection.commit()

        # 查询刚插入的记录
        teacher_id = cursor.lastrowid
        cursor.execute("SELECT * FROM ta_teacher WHERE id = %s", (teacher_id,))
        teacher_info = cursor.fetchone()
        print("  444444444444\n");
        return jsonify({
            'data': {
                'message': '新增教师成功',
                'code': 200,
                'teacher': teacher_info
            }
        }), 200

    except Error as e:
        print(" 5555555:", e);
        connection.rollback()
        app_logger.error(f"Database error during adding teacher: {e}")
        return jsonify({
            'data': {
                'message': '新增教师失败',
                'code': 500
            }
        }), 500
    except Exception as e:
        print("  666666666\n");
        app_logger.error(f"Unexpected error during adding teacher: {e}")
        return jsonify({
            'data': {
                'message': '内部服务器错误',
                'code': 500
            }
        }), 500
    finally:
        print("  777777777\n");
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after adding teacher.")

@app.route("/delete_teacher", methods=["POST"])
def delete_teacher():
    data = request.get_json()
    if not data or "teacher_unique_id" not in data:
        return jsonify({
            "data": {
                "message": "缺少 teacher_unique_id",
                "code": 400
            }
        }), 400

    teacher_unique_id = str(data["teacher_unique_id"])

    connection = get_db_connection()
    if connection is None:
        return jsonify({
            "data": {
                "message": "数据库连接失败",
                "code": 500
            }
        }), 500

    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM ta_teacher WHERE teacher_unique_id = %s",
            (teacher_unique_id,)
        )
        connection.commit()

        if cursor.rowcount > 0:
            return jsonify({
                "data": {
                    "message": "删除教师成功",
                    "code": 200
                }
            }), 200
        else:
            return jsonify({
                "data": {
                    "message": "未找到对应教师",
                    "code": 404
                }
            }), 404
    except Exception as e:
        connection.rollback()
        app_logger.error(f"删除教师时数据库异常: {e}")
        return jsonify({
            "data": {
                "message": "删除教师失败",
                "code": 500
            }
        }), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


# 📌 查询教师列表接口
@app.route("/get_list_teachers", methods=["GET"])
def get_list_teachers():
    school_id = request.args.get("schoolId")
    final_query = "SELECT * FROM ta_teacher WHERE (%s IS NULL OR schoolId = %s)"
    params = (school_id, school_id)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("List teachers failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'teachers': []
            }
        }), 500

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, params)
        teachers = cursor.fetchall()
        app_logger.info(f"Fetched {len(teachers)} teachers.")

        return jsonify({
            'data': {
                'message': '获取老师列表成功',
                'code': 200,
                'teachers': teachers
            }
        }), 200

    except Error as e:
        app_logger.error(f"Database error during fetching teachers: {e}")
        return jsonify({
            'data': {
                'message': '获取老师列表失败',
                'code': 500,
                'teachers': []
            }
        }), 500
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching teachers: {e}")
        return jsonify({
            'data': {
                'message': '内部服务器错误',
                'code': 500,
                'teachers': []
            }
        }), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching teachers.")


@app.route('/teachers', methods=['GET'])
def list_teachers():
    """
    获取老师列表 (支持根据学校ID筛选和姓名模糊搜索)
    Query Parameters:
        - school_id (int, optional): 学校ID，用于筛选特定学校的老师
        - name (str, optional): 老师姓名，用于模糊搜索
        - grade_id (int, optional): 年级ID，用于筛选特定年级的老师
    Returns:
        JSON: 包含状态信息和老师列表数据的响应
             { "data": { "message": "...", "code": ..., "teachers": [...] } }
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("List teachers failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'teachers': []
            }
        }), 500

    cursor = None
    try:
        # 1. 获取并解析查询参数
        school_id_filter = request.args.get('school_id', type=int)
        grade_id_filter = request.args.get('grade_id', type=int)
        name_filter = request.args.get('name', type=str)

        # 2. 构建 SQL 查询
        base_columns = "id, name, icon, subject, gradeId,schoolId"
        base_query = f"SELECT {base_columns} FROM ta_teacher WHERE 1=1"
        filters = []
        params = []

        # 应用学校ID筛选
        if school_id_filter is not None:
            filters.append("AND schoolId = %s")
            params.append(school_id_filter)

        if grade_id_filter is not None:
            filters.append("AND gradeId = %s")
            params.append(grade_id_filter)

        # 应用姓名模糊搜索
        if name_filter:
            filters.append("AND name LIKE %s")
            params.append(f"%{name_filter}%")

        # 3. 执行查询
        final_query = base_query + " " + " ".join(filters)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        teachers = cursor.fetchall()

        # 4. 返回 JSON 响应 (包裹在 data 对象中)
        app_logger.info(f"Fetched {len(teachers)} teachers.")
        return jsonify({
            'data': {
                'message': '获取老师列表成功',
                'code': 200,
                'teachers': teachers
            }
        }), 200

    except Error as e:
        app_logger.error(f"Database error during fetching teachers: {e}")
        return jsonify({
            'data': {
                'message': '获取老师列表失败',
                'code': 500,
                'teachers': []
            }
        }), 500
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching teachers: {e}")
        return jsonify({
            'data': {
                'message': '内部服务器错误',
                'code': 500,
                'teachers': []
            }
        }), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching teachers.")

# 查询最近3天的消息列表
@app.route('/messages/recent', methods=['GET'])
def get_recent_messages():
    """
    获取最近3天的消息列表，并包含发送者的姓名和图标信息
    Query Parameters (可选):
        - school_id (int): 筛选特定学校的消息
        - class_id (int): 筛选特定班级的消息
        - sender_id (int): 筛选特定发送者的消息
    Returns:
        JSON: 包含状态信息和消息列表数据(含姓名和图标)的响应
             { "data": { "message": "...", "code": ..., "messages": [...] } }
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("Get recent messages failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'messages': []
            }
        }), 500

    cursor = None
    try:
        # 1. 获取并解析查询参数
        school_id = request.args.get('school_id', type=int)
        class_id = request.args.get('class_id', type=int)
        sender_id_filter = request.args.get('sender_id', type=int) # 重命名以避免与循环变量冲突

        # 2. 构建 SQL 查询消息
        # 计算3天前的日期时间
        three_days_ago = datetime.datetime.now() - datetime.timedelta(days=3)
        
        # 修改查询列，包含发送者ID以便后续查询教师信息
        base_columns = "id, sender_id, content_type, text_content, school_id, class_id, sent_at, created_at, updated_at"
        base_query = f"SELECT {base_columns} FROM ta_message WHERE sent_at >= %s and content_type='text'"
        filters = []
        params = [three_days_ago]

        # 应用可选筛选条件
        if school_id is not None:
            filters.append("AND school_id = %s")
            params.append(school_id)
        if class_id is not None:
            filters.append("AND class_id = %s")
            params.append(class_id)
        if sender_id_filter is not None: # 使用重命名后的变量
            filters.append("AND sender_id = %s")
            params.append(sender_id_filter)

        # 按发送时间降序排列 (最新的在前)
        order_clause = "ORDER BY sent_at DESC"

        # 3. 执行查询消息
        final_query = f"{base_query} {' '.join(filters)} {order_clause}"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        messages = cursor.fetchall()

        # 4. 提取所有唯一的 sender_id
        sender_ids = list(set(msg['sender_id'] for msg in messages))
        sender_info_map = {}

        # 5. 查询所有相关发送者的姓名和图标
        if sender_ids:
            # 使用 IN 子句一次性查询所有姓名和图标，提高效率
            placeholders = ','.join(['%s'] * len(sender_ids))
            # 假设 ta_teacher 表包含 id, name, icon 字段
            info_query = f"SELECT id, name, icon FROM ta_teacher WHERE id IN ({placeholders})"
            cursor.execute(info_query, tuple(sender_ids))
            teacher_infos = cursor.fetchall()
            # 构建 sender_id 到 {name, icon} 字典的映射字典
            sender_info_map = {
                teacher['id']: {
                    'sender_name': teacher['name'],
                    'sender_icon': teacher['icon']
                }
                for teacher in teacher_infos
            }
            app_logger.info(f"Fetched name and icon for {len(sender_info_map)} unique senders.")

        # 6. 将姓名和图标信息合并到消息数据中，并格式化时间
        for msg in messages:
            # 获取并添加发送者信息
            sender_info = sender_info_map.get(msg['sender_id'], {})
            msg['sender_name'] = sender_info.get('sender_name', '未知老师')
            msg['sender_icon'] = sender_info.get('sender_icon', None) # 如果找不到图标，则为 None
            
            # 将 datetime 对象转换为字符串，以便 JSON 序列化
            if isinstance(msg.get('sent_at'), datetime.datetime):
                 msg['sent_at'] = msg['sent_at'].strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(msg.get('created_at'), datetime.datetime):
                 msg['created_at'] = msg['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(msg.get('updated_at'), datetime.datetime):
                 msg['updated_at'] = msg['updated_at'].strftime('%Y-%m-%d %H:%M:%S')

        # 7. 返回 JSON 响应 (包裹在 data 对象中)
        app_logger.info(f"Fetched {len(messages)} recent messages with sender names and icons.")
        return jsonify({
            'data': {
                'message': '获取最近消息列表成功',
                'code': 200,
                'messages': messages
            }
        }), 200

    except Error as e:
        app_logger.error(f"Database error during fetching recent messages: {e}")
        return jsonify({
            'data': {
                'message': '获取最近消息列表失败',
                'code': 500,
                'messages': []
            }
        }), 500
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching recent messages: {e}")
        return jsonify({
            'data': {
                'message': '内部服务器错误',
                'code': 500,
                'messages': []
            }
        }), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching recent messages.")

# 添加新消息
@app.route('/messages', methods=['POST'])
def add_message():
    connection = get_db_connection()
    if not connection:
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'message': None
            }
        }), 500

    cursor = None
    try:
        content_type_header = request.content_type or ""
        
        sender_id = request.args.get('sender_id', type=int) or request.form.get('sender_id', type=int)

        # === 情况1: JSON 格式 - 发送文本消息 ===
        if content_type_header.startswith('application/json'):
            data = request.get_json()
            if not data:
                return jsonify({
                    'data': {
                        'message': '无效的 JSON 数据',
                        'code': 400,
                        'message': None
                    }
                }), 400

            sender_id = data.get('sender_id') or sender_id
            text_content = data.get('text_content')
            content_type = data.get('content_type', 'text').lower()
            school_id = data.get('school_id')
            class_id = data.get('class_id')
            sent_at_str = data.get('sent_at')

            if not sender_id:
                return jsonify({
                    'data': {
                        'message': '缺少 sender_id',
                        'code': 400,
                        'message': None
                    }
                }), 400

            if content_type != 'text':
                return jsonify({
                    'data': {
                        'message': 'content_type 必须为 text 才能发送文本',
                        'code': 400,
                        'message': None
                    }
                }), 400

            if not text_content or not text_content.strip():
                return jsonify({
                    'data': {
                        'message': 'text_content 不能为空',
                        'code': 400,
                        'message': None
                    }
                }), 400

            text_content = text_content.strip()

            # 时间处理
            sent_at = datetime.datetime.now()
            if sent_at_str:
                try:
                    sent_at = datetime.strptime(sent_at_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return jsonify({
                        'data': {
                            'message': 'sent_at 格式错误，应为 YYYY-MM-DD HH:MM:SS',
                            'code': 400
                        }
                    }), 400

            # 插入数据库
            insert_query = """
                INSERT INTO ta_message 
                (sender_id, content_type, text_content, audio_data, school_id, class_id, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor = connection.cursor()
            cursor.execute(insert_query, (
                sender_id,
                'text',
                text_content,
                None,        # audio_data
                school_id,
                class_id,
                sent_at
            ))
            connection.commit()
            new_message_id = cursor.lastrowid

            message_dict = {
                'id': new_message_id,
                'sender_id': sender_id,
                'content_type': 'text',
                'text_content': text_content,
                'audio_url': None,
                'school_id': school_id,
                'class_id': class_id,
                'sent_at': sent_at.strftime('%Y-%m-%d %H:%M:%S'),
                'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

            return jsonify({
                'data': {
                    'message': '文本消息发送成功',
                    'code': 201,
                    'message': message_dict
                }
            }), 201


        # === 情况2: 二进制流 - 发送音频消息 ===
        elif content_type_header.startswith('application/octet-stream'):
            if not sender_id:
                return jsonify({
                    'data': {
                        'message': '缺少 sender_id（请通过 query 或 form 传递）',
                        'code': 400,
                        'message': None
                    }
                }), 400

            # 强制要求 content_type=audio（可通过 query 或 header）
            msg_content_type = request.args.get('content_type') or request.headers.get('X-Content-Type')
            if msg_content_type != 'audio':
                return jsonify({
                    'data': {
                        'message': 'content_type 必须为 audio',
                        'code': 400,
                        'message': None
                    }
                }), 400

            # 读取音频二进制流
            audio_data = request.get_data()
            audio_data = request.get_data()  # 读取原始 body
            #with open("received.wav", "wb") as f:
            #    f.write(audio_data)
            if not audio_data:
                return jsonify({
                    'data': {
                        'message': '音频数据为空',
                        'code': 400,
                        'message': None
                    }
                }), 400

            # 验证音频 MIME 类型
            client_audio_type = request.headers.get('X-Audio-Content-Type') or content_type_header
            valid_types = ['audio/mpeg', 'audio/wav', 'audio/aac', 'audio/ogg', 'audio/mp4']
            if client_audio_type not in valid_types:
                return jsonify({
                    'data': {
                        'message': f'不支持的音频类型: {client_audio_type}',
                        'code': 400,
                        'message': None
                    }
                }), 400

            school_id = request.args.get('school_id', type=int)
            class_id = request.args.get('class_id', type=int)
            sent_at_str = request.args.get('sent_at')

            sent_at = datetime.datetime.now()
            if sent_at_str:
                try:
                    sent_at = datetime.strptime(sent_at_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return jsonify({'data': {
                        'message': 'sent_at 格式错误',
                        'code': 400
                    }}), 400

            # 插入音频消息
            insert_query = """
                INSERT INTO ta_message 
                (sender_id, content_type, text_content, audio_data, school_id, class_id, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor = connection.cursor()
            cursor.execute(insert_query, (
                sender_id,
                'audio',
                None,           # text_content
                audio_data,     # 存二进制流
                school_id,
                class_id,
                sent_at
            ))
            connection.commit()
            new_message_id = cursor.lastrowid

            # 返回动态播放链接
            audio_url = f"/api/audio/{new_message_id}"

            message_dict = {
                'id': new_message_id,
                'sender_id': sender_id,
                'content_type': 'audio',
                'text_content': None,
                'audio_url': audio_url,
                'school_id': school_id,
                'class_id': class_id,
                'sent_at': sent_at.strftime('%Y-%m-%d %H:%M:%S'),
                'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

            return jsonify({
                'data': {
                    'message': '音频消息发送成功',
                    'code': 201,
                    'message': message_dict
                }
            }), 201


        # === 其他 Content-Type 不支持 ===
        else:
            return jsonify({
                'data': {
                    'message': '仅支持 application/json（文本）或 application/octet-stream（音频）',
                    'code': 400,
                    'message': None
                }
            }), 400


    except Exception as e:
        app_logger.error(f"Error in add_message: {e}")
        if connection and connection.is_connected():
            connection.rollback()
        return jsonify({
            'data': {
                'message': '服务器内部错误',
                'code': 500,
                'message': None
            }
        }), 500


    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/audio/<int:message_id>', methods=['GET'])
def get_audio(message_id):
    connection = get_db_connection()
    if not connection:
        return 'Database error', 500

    cursor = None
    try:
        query = "SELECT audio_data FROM ta_message WHERE id = %s AND content_type = 'audio'"
        cursor = connection.cursor()
        cursor.execute(query, (message_id,))
        result = cursor.fetchone()

        if not result or not result[0]:
            return 'Audio not found', 404

        audio_data = result[0]

        # 可通过扩展名或 header 推断类型，这里默认 mp3
        response = app.response_class(
            response=audio_data,
            status=200,
            mimetype='audio/mpeg'  # 可根据实际调整
        )
        response.headers['Content-Length'] = len(audio_data)
        response.headers['Accept-Ranges'] = 'bytes'
        return response

    except Exception as e:
        app_logger.error(f"Error serving audio: {e}")
        return 'Internal error', 500
    finally:
        if cursor: cursor.close()
        if connection and connection.is_connected(): connection.close()

# --- 通知接口 ---

@app.route('/notifications', methods=['POST'])
def send_notification_to_class():
    """
    发送通知给指定班级
    Request Body (JSON):
        - sender_id (int): 发送者老师ID (必需)
        - class_id (int): 接收通知的班级ID (必需) -> 存入 receiver_id 字段
        - content (str): 通知内容 (必需)
    Returns:
        JSON: { "data": { "message": "...", "code": ..., "notification": {...} } }
              返回的通知对象包含发送者的 name 和 icon 字段。
    """
    connection = get_db_connection()
    if connection is None:
        return jsonify({'data': {'message': '数据库连接失败', 'code': 500}}), 500

    cursor = None
    try:
        data = request.get_json()
        sender_id = data.get('sender_id')
        class_id = data.get('class_id')
        content = data.get('content')

        # 基本验证
        if not all([sender_id, class_id, content]):
            return jsonify({'data': {'message': '缺少必需参数: sender_id, class_id, content', 'code': 400}}), 400

        # 开始事务
        connection.start_transaction()

        cursor = connection.cursor(dictionary=True)
        # 1. 插入通知，receiver_id 存储班级ID
        insert_query = """
            INSERT INTO ta_notification 
            (sender_id, receiver_id, content) 
            VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (sender_id, class_id, content))
        notification_id = cursor.lastrowid

        # 2. 获取刚插入的通知详情，并关联 ta_teacher 表获取发送者信息
        select_query = """
            SELECT n.*, t.name AS sender_name, t.icon AS sender_icon
            FROM ta_notification n
            JOIN ta_teacher t ON n.sender_id = t.id
            WHERE n.id = %s
        """
        cursor.execute(select_query, (notification_id,))
        new_notification = cursor.fetchone()

        if not new_notification:
             # 理论上不应该发生，但做个检查
             connection.rollback()
             app_logger.error(f"Failed to retrieve newly created notification {notification_id} with sender info.")
             return jsonify({'data': {'message': '创建通知后查询失败', 'code': 500}}), 500

        # 3. 格式化时间
        new_notification = format_notification_time(new_notification)

        # 提交事务
        connection.commit()

        app_logger.info(f"Notification sent by teacher {sender_id} (Name: {new_notification.get('sender_name', 'N/A')}) to class {class_id}: ID {notification_id}")
        return jsonify({
            'data': {
                'message': '通知发送成功',
                'code': 201,
                'notification': new_notification
            }
        }), 201

    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error sending notification to class {class_id}: {e}")
        return jsonify({'data': {'message': '发送通知失败', 'code': 500}}), 500
    except Exception as e:
        connection.rollback()
        app_logger.error(f"Unexpected error sending notification to class {class_id}: {e}")
        return jsonify({'data': {'message': '内部服务器错误', 'code': 500}}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after sending notification.")

def format_notification_time(notif_dict):
    """格式化通知中的时间字段"""
    for time_field in ['created_at', 'updated_at']:
        if isinstance(notif_dict.get(time_field), datetime.datetime):
            notif_dict[time_field] = notif_dict[time_field].strftime('%Y-%m-%d %H:%M:%S')
    return notif_dict

@app.route('/notifications/class/<int:class_id>', methods=['GET'])
def get_notifications_for_class(class_id):
    """
    获取指定班级的最新通知，并将这些通知标记为已读 (is_read=1)。
    Path Parameter:
        - class_id (int): 班级ID
    Query Parameters (可选):
        - limit (int): 限制返回的通知数量，默认 20，最大 100
    Returns:
        JSON: { "data": { "message": "...", "code": ..., "notifications": [...] } }
             返回的通知列表包含发送者的 name 和 icon 字段。
             返回的通知列表是调用此接口前未读的通知。
    """
    connection = get_db_connection()
    if connection is None:
        return jsonify({'data': {'message': '数据库连接失败', 'code': 500, 'notifications': []}}), 500

    cursor = None
    try:
        # 获取可选的 limit 参数
        limit = request.args.get('limit', default=20, type=int)
        # 限制 limit 范围
        limit = max(1, min(limit, 100))

        # 开始事务
        connection.start_transaction()

        cursor = connection.cursor(dictionary=True)

        # 1. 首先查询该班级未读的通知，并关联 ta_teacher 表获取发送者信息 (按创建时间倒序)
        select_query = """
            SELECT n.*, t.name AS sender_name, t.icon AS sender_icon
            FROM ta_notification n
            JOIN ta_teacher t ON n.sender_id = t.id
            WHERE n.receiver_id = %s AND n.is_read = 0
            ORDER BY n.created_at DESC
            LIMIT %s
        """
        cursor.execute(select_query, (class_id, limit))
        notifications = cursor.fetchall()

        # 2. 提取要更新为已读的通知ID
        notification_ids = [notif['id'] for notif in notifications]

        # 3. 如果有未读通知，则批量更新它们的 is_read 状态
        if notification_ids:
            ids_placeholder = ','.join(['%s'] * len(notification_ids))
            update_query = f"UPDATE ta_notification SET is_read = 1, updated_at = CURRENT_TIMESTAMP WHERE id IN ({ids_placeholder})"
            cursor.execute(update_query, tuple(notification_ids))
            # connection.commit() # 暂不提交，事务结束时统一提交
            app_logger.info(f"Marked {len(notification_ids)} notifications as read for class {class_id}.")
        else:
            app_logger.info(f"No unread notifications found for class {class_id}.")

        # 4. 格式化返回的通知的时间
        for notif in notifications:
             notif = format_notification_time(notif)

        # 提交事务
        connection.commit()

        app_logger.info(f"Fetched and marked {len(notifications)} notifications for class {class_id} (limit: {limit}).")
        return jsonify({
            'data': {
                'message': '获取班级通知成功',
                'code': 200,
                'notifications': notifications # 返回的是获取前未读的通知，已包含发送者信息
            }
        }), 200

    except Error as e:
        connection.rollback() # 回滚事务
        app_logger.error(f"Database error fetching/reading notifications for class {class_id}: {e}")
        return jsonify({'data': {'message': '获取/标记通知失败', 'code': 500, 'notifications': []}}), 500
    except Exception as e:
        connection.rollback()
        app_logger.error(f"Unexpected error fetching/reading notifications for class {class_id}: {e}")
        return jsonify({'data': {'message': '内部服务器错误', 'code': 500, 'notifications': []}}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching/reading notifications for class.")

# --- 修改后的壁纸列表接口 ---
@app.route('/wallpapers', methods=['GET'])
def list_wallpapers():
    """
    获取所有壁纸列表 (支持筛选、排序)
    Query Parameters:
        - is_enabled (int, optional): 是否启用 (1: 启用, 0: 禁用)
        - resolution (str, optional): 分辨率筛选 (例如 '1920x1080')
        - sort_by (str, optional): 排序字段 ('created_at', 'updated_at') (默认 'created_at')
        - order (str, optional): 排序方式 ('asc', 'desc') (默认 'desc')
    Returns:
        JSON: 包含状态信息和壁纸列表数据的响应
             { "data": { "message": "...", "code": ..., "wallpapers": [...] } }
    """
    connection = get_db_connection()
    if connection is None:
        app_logger.error("List wallpapers failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500,
                'wallpapers': []
            }
        }), 500

    cursor = None
    try:
        # 1. 获取并解析查询参数
        is_enabled_filter = request.args.get('is_enabled', type=int) # 1 or 0 or None
        resolution_filter = request.args.get('resolution', type=str) # e.g., '1920x1080' or None
        sort_by = request.args.get('sort_by', 'created_at', type=str) # Default sort
        order = request.args.get('order', 'desc', type=str) # Default order

        # 2. 验证排序参数
        valid_sort_fields = ['created_at', 'updated_at', 'id']
        valid_orders = ['asc', 'desc']
        if sort_by not in valid_sort_fields:
            sort_by = 'created_at'
        if order not in valid_orders:
            order = 'desc'

        # 3. 构建 SQL 查询
        base_columns = "id, title, image_url, resolution, file_size, file_type, uploader_id, is_enabled, created_at, updated_at"
        base_query = f"SELECT {base_columns} FROM ta_wallpaper WHERE 1=1"
        filters = []
        params = []

        # 应用筛选条件
        if is_enabled_filter is not None:
            filters.append("AND is_enabled = %s")
            params.append(is_enabled_filter)
        
        if resolution_filter:
            filters.append("AND resolution = %s")
            params.append(resolution_filter)

        # 应用排序
        order_clause = f"ORDER BY {sort_by} {order}"

        # 4. 执行查询
        final_query = base_query + " " + " ".join(filters) + " " + order_clause
        cursor = connection.cursor(dictionary=True)
        cursor.execute(final_query, tuple(params))
        wallpapers = cursor.fetchall()

        # 5. 返回 JSON 响应 (包裹在 data 对象中)
        app_logger.info(f"Fetched {len(wallpapers)} wallpapers.")
        return jsonify({
            'data': {
                'message': '获取壁纸列表成功',
                'code': 200,
                'wallpapers': wallpapers
            }
        }), 200

    except Error as e:
        app_logger.error(f"Database error during fetching wallpapers: {e}")
        return jsonify({
            'data': {
                'message': '获取壁纸列表失败',
                'code': 500,
                'wallpapers': []
            }
        }), 500
    except Exception as e:
        app_logger.error(f"Unexpected error during fetching wallpapers: {e}")
        return jsonify({
            'data': {
                'message': '内部服务器错误',
                'code': 500,
                'wallpapers': []
            }
        }), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            app_logger.info("Database connection closed after fetching wallpapers.")

# --- 壁纸列表接口结束 ---

@app.route('/send_verification_code', methods=['POST'])
def send_verification_code():
    """发送短信验证码接口"""
    data = request.get_json()
    phone = data.get('phone')

    if not phone:
        app_logger.warning("Send verification code failed: Phone number is missing.")
        return jsonify({
            'data': {
                'message': '手机号不能为空',
                'code': 400
            }
        }), 400

    code = generate_verification_code()
    session['verification_code'] = {
        'code': code,
        'phone': phone,
        #'expires_at': datetime.datetime.now() + datetime.timedelta(seconds=VERIFICATION_CODE_EXPIRY)
        'expires_at': time.time() + VERIFICATION_CODE_EXPIRY   # 例如 VERIFICATION_CODE_EXPIRY = 300
    }

    if send_sms_verification_code(phone, code):
        app_logger.info(f"Verification code sent successfully to {phone}.")
        return jsonify({
            'data': {
                'message': '验证码已发送',
                'code': 200
            }
        }), 200
    else:
        session.pop('verification_code', None)
        app_logger.error(f"Failed to send verification code to {phone}.")
        return jsonify({
            'data': {
                'message': '验证码发送失败',
                'code': 500
            }
        }), 500


@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    phone = data.get('phone')
    password = data.get('password')
    verification_code = data.get('verification_code')

    if not phone or not password or not verification_code:
        app_logger.warning("Registration failed: Missing phone, password, or verification code.")
        return jsonify({
            'data': {
                'message': '手机号、密码和验证码不能为空',
                'code': 400
            }
        }), 400

    is_valid, message = verify_code_from_session(phone, verification_code)
    if not is_valid:
        app_logger.warning(f"Registration failed for {phone}: {message}")
        return jsonify({
            'data': {
                'message': message,
                'code': 400
            }
        }), 400

    salt = secrets.token_hex(16)
    password_hash = hash_password(password, salt)

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Registration failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }), 500

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM ta_user WHERE phone = %s", (phone,))
        if cursor.fetchone():
            app_logger.info(f"Registration failed for {phone}: Phone number already registered.")
            cursor.close()
            return jsonify({
                'data': {
                    'message': '手机号已注册',
                    'code': 400
                }
            }), 400

        insert_query = """
            INSERT INTO ta_user (phone, password_hash, salt, is_verified, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (phone, password_hash, salt, 1, None))
        connection.commit()
        user_id = cursor.lastrowid
        cursor.close()
        app_logger.info(f"User registered successfully: Phone {phone}, User ID {user_id}.")
        return jsonify({
            'data': {
                'message': '注册成功',
                'code': 201,
                'user_id': user_id # 可以将具体数据放在 data 对象下
            }
        }), 201

    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error during registration for {phone}: {e}")
        return jsonify({
            'data': {
                'message': '注册失败',
                'code': 500
            }
        }), 500
    finally:
        if connection and connection.is_connected():
            connection.close()
            # app_logger.info("Database connection closed after registration attempt.")


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    phone = data.get('phone')
    password = data.get('password')
    verification_code = data.get('verification_code')

    if not phone or (not password and not verification_code):
        app_logger.warning("Login failed: Missing phone and either password or verification code.")
        return jsonify({
            'data': {
                'message': '手机号和密码或验证码必须提供',
                'code': 400
            }
        }), 400

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Login failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }), 500

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id, password_hash, salt, is_verified FROM ta_user WHERE phone = %s", (phone,))
        user = cursor.fetchone()

        if not user:
            app_logger.info(f"Login failed for {phone}: User not found.")
            cursor.close()
            return jsonify({
                'data': {
                    'message': '用户不存在',
                    'code': 404
                }
            }), 404

        if not user['is_verified']:
            app_logger.info(f"Login failed for {phone}: Account not verified.")
            cursor.close()
            return jsonify({
                'data': {
                    'message': '账户未验证',
                    'code': 403
                }
            }), 403

        # 验证方式
        auth_success = False
        if password:
            stored_hash = user['password_hash']
            salt = user['salt']
            input_hash = hash_password(password, salt)
            if input_hash == stored_hash:
                auth_success = True
                app_logger.info(f"Password login successful for user ID {user['id']}.")
            else:
                app_logger.warning(f"Password login failed for {phone}: Incorrect password.")
                cursor.close()
                return jsonify({
                    'data': {
                        'message': '密码错误',
                        'code': 401
                    }
                }), 401

        elif verification_code:
            is_valid, message = verify_code_from_session(phone, verification_code)
            if is_valid:
                auth_success = True
                app_logger.info(f"Verification code login successful for user ID {user['id']}.")
            else:
                app_logger.warning(f"Verification code login failed for {phone}: {message}")
                cursor.close()
                # 这里也用 400，因为是客户端传参问题
                return jsonify({
                    'data': {
                        'message': message,
                        'code': 400
                    }
                }), 400

        if auth_success:
            update_query = "UPDATE ta_user SET last_login_at = %s WHERE id = %s"
            cursor.execute(update_query, (datetime.datetime.now(), user['id']))
            connection.commit()
            cursor.close()
            return jsonify({
                'data': {
                    'message': '登录成功',
                    'code': 200,
                    'user_id': user['id'] # 返回用户ID
                }
            }), 200

    except Error as e:
        app_logger.error(f"Database error during login for {phone}: {e}")
        return jsonify({
            'data': {
                'message': '登录失败',
                'code': 500
            }
        }), 500
    finally:
        if connection and connection.is_connected():
            connection.close()
            # app_logger.info("Database connection closed after login attempt.")


@app.route('/verify_and_set_password', methods=['POST'])
def verify_and_set_password():
    """忘记密码 - 验证并重置密码"""
    data = request.get_json()
    phone = data.get('phone')
    verification_code = data.get('verification_code')
    new_password = data.get('new_password')

    if not phone or not verification_code or not new_password:
        app_logger.warning("Password reset failed: Missing phone, verification code, or new password.")
        return jsonify({
            'data': {
                'message': '手机号、验证码和新密码不能为空',
                'code': 400
            }
        }), 400

    is_valid, message = verify_code_from_session(phone, verification_code)
    if not is_valid:
        app_logger.warning(f"Password reset failed for {phone}: {message}")
        return jsonify({
            'data': {
                'message': message,
                'code': 400
            }
        }), 400

    connection = get_db_connection()
    if connection is None:
        app_logger.error("Password reset failed: Database connection error.")
        return jsonify({
            'data': {
                'message': '数据库连接失败',
                'code': 500
            }
        }), 500

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM ta_user WHERE phone = %s AND is_verified = 1", (phone,))
        user = cursor.fetchone()

        if not user:
            app_logger.info(f"Password reset failed for {phone}: User not found or not verified.")
            cursor.close()
            return jsonify({
                'data': {
                    'message': '用户不存在或账户未验证',
                    'code': 400
                }
            }), 400

        new_salt = secrets.token_hex(16)
        new_password_hash = hash_password(new_password, new_salt)

        update_query = """
            UPDATE ta_user
            SET password_hash = %s, salt = %s
            WHERE id = %s
        """
        cursor.execute(update_query, (new_password_hash, new_salt, user[0]))
        connection.commit()

        if cursor.rowcount == 0:
            app_logger.error(f"Password reset failed for user ID {user[0]}: Update query affected 0 rows.")
            cursor.close()
            return jsonify({
                'data': {
                    'message': '更新失败',
                    'code': 500
                }
            }), 500

        cursor.close()
        app_logger.info(f"Password reset successful for user ID {user[0]}.")
        return jsonify({
            'data': {
                'message': '密码重置成功',
                'code': 200
            }
        }), 200

    except Error as e:
        connection.rollback()
        app_logger.error(f"Database error during password reset for {phone}: {e}")
        return jsonify({
            'data': {
                'message': '密码重置失败',
                'code': 500
            }
        }), 500
    finally:
        if connection and connection.is_connected():
            connection.close()
            # app_logger.info("Database connection closed after password reset attempt.")


if __name__ == '__main__':
    app_logger.info("Flask application starting...")
    app.run(host="0.0.0.0", port=5000, debug=True)
