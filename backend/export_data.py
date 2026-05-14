"""
로컬 서버에서 캐시 데이터를 JSON 파일로 저장
실행: python export_data.py
"""
import requests
import json
import urllib3
urllib3.disable_warnings()

BASE = "http://localhost:8001"

print("쉼터 데이터 내보내는 중...")
r = requests.get(f"{BASE}/api/export/shelters", timeout=30)
data = r.json()
with open("shelters_cache.json", "w", encoding="utf-8") as f:
    json.dump(data["data"], f, ensure_ascii=False)
print(f"✅ shelters_cache.json 저장 완료 ({data['count']}개)")

print("가로수 데이터 내보내는 중...")
r = requests.get(f"{BASE}/api/export/trees", timeout=60)
data = r.json()
with open("trees_cache.json", "w", encoding="utf-8") as f:
    json.dump(data["data"], f, ensure_ascii=False)
print(f"✅ trees_cache.json 저장 완료 ({data['count']}개)")