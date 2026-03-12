# -*- coding: utf-8 -*-
"""
通知模块：发送钉钉、ntfy 消息
"""
import requests
import json
import time
import hmac
import hashlib
import base64
import urllib.parse
import config


def send_dingtalk_msg(content):
    """发送文本消息到钉钉群"""
    if not config.DINGTALK_WEBHOOK or "你的TOKEN" in config.DINGTALK_WEBHOOK:
        # 如果未配置Token，静默跳过
        return

    timestamp = str(round(time.time() * 1000))
    url = config.DINGTALK_WEBHOOK

    # 加签逻辑
    if config.DINGTALK_SECRET and "你的SECRET" not in config.DINGTALK_SECRET:
        secret = config.DINGTALK_SECRET
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        url = f"{url}&timestamp={timestamp}&sign={sign}"

    headers = {'Content-Type': 'application/json'}
    data = {
        "msgtype": "text",
        "text": {
            "content": f"【溢价监控】\n{content}"
        }
    }

    try:
        resp = requests.post(url, headers=headers, data=json.dumps(data), timeout=5)
        if resp.json().get('errcode') != 0:
            print(f"[Notify] 发送失败: {resp.text}")
    except Exception as e:
        print(f"[Notify] 网络/发送错误: {e}")


def send_ntfy_msg(content, title="Premium Monitor"):
    """发送文本消息到 ntfy.sh 频道

    注意：HTTP Header 只能使用 latin-1，可打印字符范围有限，避免在 Title 中使用表情或中文。
    """
    topic = getattr(config, "NTFY_TOPIC", None)
    if not topic:
        return
    url = f"https://ntfy.sh/{topic}"
    # Header 仅使用 ASCII，避免编码错误
    headers = {
        "Title": title,
        "Priority": "default",
        "Tags": "chart_with_upwards_trend,moneybag"
    }
    body = f"【溢价监控】\n{content}"
    try:
        # 可以把 timeout 调大一点，例如 10 秒
        resp = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=10)
        if resp.status_code >= 300:
            print(f"[Notify] ntfy 发送失败: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ ntfy 推送失败: {e}")