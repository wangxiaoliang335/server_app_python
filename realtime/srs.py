import os

# SRS 服务器配置（支持 WHIP/WHEP）
SRS_SERVER = os.getenv("SRS_SERVER", "47.100.126.194")  # SRS 服务器地址
SRS_PORT = os.getenv("SRS_PORT", "1985")  # SRS WebRTC API 端口（传统 API 使用 1985）
SRS_HTTPS_PORT = os.getenv("SRS_HTTPS_PORT", "443")  # HTTPS 端口（nginx 反向代理）
SRS_APP = os.getenv("SRS_APP", "live")  # SRS 应用名称，默认 'live'
SRS_USE_HTTPS = os.getenv("SRS_USE_HTTPS", "true").lower() == "true"  # 是否使用 HTTPS（默认启用）

# SRS_BASE_URL 用于 WHIP/WHEP（通过 nginx HTTPS 代理）
SRS_BASE_URL = f"{'https' if SRS_USE_HTTPS else 'http'}://{SRS_SERVER}"
if SRS_USE_HTTPS:
    # HTTPS 模式：通过 nginx 443 端口访问
    SRS_BASE_URL = f"https://{SRS_SERVER}"
    SRS_WEBRTC_API_URL = f"https://{SRS_SERVER}:{SRS_HTTPS_PORT}"
else:
    # HTTP 模式：直接访问 SRS 1985 端口
    SRS_BASE_URL = f"http://{SRS_SERVER}"
    SRS_WEBRTC_API_URL = f"http://{SRS_SERVER}:{SRS_PORT}"

print(
    f"[启动检查] SRS 服务器配置: 协议={'HTTPS' if SRS_USE_HTTPS else 'HTTP'}, BASE_URL={SRS_BASE_URL}, WebRTC API: {SRS_WEBRTC_API_URL}, APP={SRS_APP}"
)


