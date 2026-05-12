"""
ClimateShelter Backend — FastAPI
================================
실행: uvicorn main:app --reload --port 8000

API 키 설정 (.env 파일):
  KAKAO_REST_KEY=0d073be4fdf65a11d62cbf9d98ab12d3
  SHELTER_API_KEY=D8J0RAYL7U9V46KI
  ANTHROPIC_API_KEY=sk-ant-...   ← 나중에 추가
"""

import os, math, asyncio, time
import requests as _requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime
from typing import Optional
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from shadow import calc_route_shade_score, get_shadow_overlay, get_sun_position
from route_graph import find_shade_routes

load_dotenv()

# =============================================================================
#  서울 무더위쉼터 캐시 — 서버 시작 시 백그라운드 수집
# =============================================================================

SEOUL_LAT_MIN, SEOUL_LAT_MAX = 37.41, 37.71
SEOUL_LNG_MIN, SEOUL_LNG_MAX = 126.76, 127.18

_shelter_cache: list[dict] = []      # 수집된 서울 쉼터 전체
_shelter_cache_ready = False          # 수집 완료 여부
_shelter_cache_time: float = 0        # 마지막 수집 시각

# 가로수 캐시
_tree_cache: list[dict] = []         # 서울 가로수 전체
_tree_cache_ready = False


async def _collect_seoul_shelters():
    """
    1. shelters_cache.json 파일이 있으면 즉시 로딩 (Railway 배포용)
    2. 없으면 API에서 수집 (로컬 개발용)
    """
    global _shelter_cache, _shelter_cache_ready, _shelter_cache_time

    # JSON 파일에서 먼저 로딩 시도
    import os, json as _json
    cache_file = os.path.join(os.path.dirname(__file__), "shelters_cache.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = _json.load(f)
            _shelter_cache       = data if isinstance(data, list) else data.get("data", [])
            _shelter_cache_ready = True
            _shelter_cache_time  = time.time()
            print(f"[쉼터 캐시] JSON 파일에서 로딩 완료 — {len(_shelter_cache)}개")
            return
        except Exception as e:
            print(f"[쉼터 캐시] JSON 파일 로딩 실패: {e}, API로 수집 시작")

    print("[쉼터 캐시] 서울 무더위쉼터 수집 시작...")
    shelters = []
    page = 1
    headers = {"User-Agent": "ClimateShelter/1.0"}

    try:
        while True:
                params = {
                    "serviceKey": SHELTER_API_KEY,
                    "pageNo":     page,
                    "numOfRows":  1000,
                    "returnType": "json",
                }
                # requests 라이브러리 사용 (httpx SSL 호환 문제 우회)
                loop = asyncio.get_event_loop()
                resp = await loop.run_in_executor(
                    None,
                    lambda p=params: _requests.get(
                        "http://www.safetydata.go.kr/V2/api/DSSP-IF-10942",
                        params=p, headers=headers,
                        timeout=15, verify=False,
                        allow_redirects=True,
                    )
                )
                data = resp.json()

                if data.get("header", {}).get("resultCode") != "00":
                    print(f"[쉼터 캐시] API 오류: {data.get('header')}")
                    break

                items = data.get("body", []) or []
                total = data.get("totalCount", 0)

                for item in items:
                    try:
                        lat = float(item.get("LA") or 0)
                        lng = float(item.get("LO") or 0)
                        if not (SEOUL_LAT_MIN <= lat <= SEOUL_LAT_MAX and
                                SEOUL_LNG_MIN <= lng <= SEOUL_LNG_MAX):
                            continue

                        begin = str(item.get("WKDAY_OPER_BEGIN_TIME") or "")
                        end   = str(item.get("WKDAY_OPER_END_TIME")   or "")
                        if len(begin) == 4 and len(end) == 4:
                            open_str = f"{begin[:2]}:{begin[2:]}~{end[:2]}:{end[2:]}"
                        else:
                            open_str = "운영시간 확인 필요"

                        shelters.append({
                            "name":     item.get("RSTR_NM", "무더위쉼터"),
                            "address":  item.get("RN_DTL_ADRES") or item.get("DTL_ADRES") or "",
                            "type":     item.get("FCLTY_TY", ""),
                            "lat":      lat,
                            "lng":      lng,
                            "open":     open_str,
                            "capacity": item.get("USE_PSBL_NMPR") or 0,
                        })
                    except (ValueError, TypeError):
                        continue

                checked = page * 1000
                if checked % 10000 == 0:
                    print(f"[쉼터 캐시] {checked}/{total}개 확인, 서울 {len(shelters)}개")

                if checked >= total or not items:
                    break
                page += 1
                import asyncio as _aio; await _aio.sleep(0.05)  # API 부하 방지

        _shelter_cache       = shelters
        _shelter_cache_ready = True
        _shelter_cache_time  = time.time()
        print(f"[쉼터 캐시] 완료 — 서울 무더위쉼터 {len(shelters)}개 캐시됨")

    except Exception as e:
        import traceback
        print(f"[쉼터 캐시] 오류 상세:")
        traceback.print_exc()
        _shelter_cache_ready = True  # 오류여도 폴백으로 진행


def get_shelters_from_cache(lat: float, lng: float, radius_m: int = 800) -> list[dict]:
    """캐시에서 반경 내 쉼터 반환 (거리 계산 후 정렬)"""
    results = []
    for s in _shelter_cache:
        d = haversine(lat, lng, s["lat"], s["lng"])
        if d <= radius_m:
            results.append({**s, "distance": round(d)})
    results.sort(key=lambda x: x["distance"])
    return results[:8]


@asynccontextmanager
async def lifespan(app_instance):
    # 서버 시작 시 백그라운드 수집
    asyncio.create_task(_collect_seoul_shelters())
    asyncio.create_task(_collect_seoul_trees())
    yield
    # 서버 종료 시 정리 작업 (필요시)


app = FastAPI(title="ClimateShelter API", lifespan=lifespan)

# CORS — 프론트엔드에서 호출 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 배포 시 실제 도메인으로 교체
    allow_methods=["*"],
    allow_headers=["*"],
)

KAKAO_REST_KEY  = os.getenv("KAKAO_REST_KEY",  "0d073be4fdf65a11d62cbf9d98ab12d3")
SHELTER_API_KEY = os.getenv("SHELTER_API_KEY", "D8J0RAYL7U9V46KI")
TREE_API_KEY    = os.getenv("TREE_API_KEY",    "666e6c427666617433324174707758")
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")


# =============================================================================
#  Request / Response 모델
# =============================================================================

class RouteRequest(BaseModel):
    start_lat: float
    start_lng: float
    end_lat: float
    end_lng: float
    heat_mode: str = "ok"          # 하위호환용, 현재 미사용
    hour: Optional[int] = None     # None이면 현재 시각 사용


class RouteResponse(BaseModel):
    normal: dict
    climate: dict
    shelters_nearby: list
    weather: dict
    reason: str
    heat_risk: dict = {}
    ai_powered: bool = False


# =============================================================================
#  1. 경로 탐색 — OSRM (도보, 무료, API 키 불필요)
#  NOTE: 카카오 Mobility는 도보 경로 공식 미지원 (자동차만 가능)
#  TODO: 카카오 도보 API 정식 오픈 시 이 함수를 교체
# =============================================================================

async def get_kakao_walking_routes(sx, sy, ex, ey) -> list[dict]:
    """
    OSRM으로 도보 경로 2개 반환
    - route[0]: 최단 경로 (일반)
    - route[1]: alternatives 또는 없으면 None (메인 라우트에서 쉼터 waypoint로 생성)
    sx/sy = 출발지 lng/lat, ex/ey = 목적지 lng/lat
    """
    url = (
        f"https://router.project-osrm.org/route/v1/foot/"
        f"{sx},{sy};{ex},{ey}"
        f"?overview=full&geometries=geojson&alternatives=true"
    )

    async with httpx.AsyncClient(timeout=12) as client:
        resp = await client.get(url)

    if resp.status_code != 200:
        raise HTTPException(502, f"OSRM 경로 API 오류: {resp.status_code}")

    data = resp.json()
    routes = data.get("routes", [])
    if not routes:
        raise HTTPException(404, "경로를 찾을 수 없습니다.")

    result = []
    for r in routes[:2]:
        coords = [[pt[1], pt[0]] for pt in r["geometry"]["coordinates"]]
        dist = r.get("distance", 0)
        result.append({
            "coords":      coords,
            "distance":    dist,
            "duration":    r.get("duration", 0),   # 초 단위 (OSRM)
            "duration_min": round(dist / 80),       # 분 단위 (카드 표시)
        })

    return result


async def get_shelter_waypoint_route(sx, sy, ex, ey, waypoint_lng, waypoint_lat) -> dict | None:
    """
    쉼터를 경유지로 넣은 우회 경로 (climate 전용)
    출발지 → 쉼터 → 목적지
    """
    url = (
        f"https://router.project-osrm.org/route/v1/foot/"
        f"{sx},{sy};{waypoint_lng},{waypoint_lat};{ex},{ey}"
        f"?overview=full&geometries=geojson"
    )
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()
        routes = data.get("routes", [])
        if not routes:
            return None
        r = routes[0]
        coords = [[pt[1], pt[0]] for pt in r["geometry"]["coordinates"]]
        return {
            "coords":   coords,
            "distance": r.get("distance", 0),
            "duration": r.get("duration", 0),
        }
    except Exception:
        return None


# =============================================================================
#  2. 재난안전데이터공유플랫폼 — 무더위쉼터
#  API: https://www.safetydata.go.kr/V2/api/DSSP-IF-10942
#  필드: LA=위도, LO=경도, RSTR_NM=이름, WKDAY_OPER_BEGIN/END_TIME=평일운영시간
# =============================================================================

async def get_shelters(lat: float, lng: float, radius_m: int = 800) -> list[dict]:
    """
    캐시에서 반경 내 쉼터 반환
    캐시 미준비 시 폴백 데이터 사용
    """
    if _shelter_cache_ready and _shelter_cache:
        result = get_shelters_from_cache(lat, lng, radius_m)
        return result if result else FALLBACK_SHELTERS
    # 캐시 수집 중이면 폴백
    print("[쉼터] 캐시 준비 중 — 폴백 데이터 사용")
    return FALLBACK_SHELTERS


async def _collect_seoul_trees():
    """
    서울시 가로수 위치 수집 (서버 시작 시 백그라운드)
    API: openapi.seoul.go.kr streetTreeInfo
    총 284,071개 → 페이지당 1000개씩 수집
    """
    global _tree_cache, _tree_cache_ready
    print("[가로수 캐시] 서울 가로수 수집 시작...")

    trees = []
    page_size = 1000
    page = 1

    try:
        loop = asyncio.get_event_loop()
        while True:
            start = (page - 1) * page_size + 1
            end   = page * page_size
            url   = f"http://openapi.seoul.go.kr:8088/{TREE_API_KEY}/json/streetTreeInfo/{start}/{end}/"

            resp = await loop.run_in_executor(
                None,
                lambda u=url: _requests.get(u, timeout=15, verify=False)
            )
            data = resp.json()
            info = data.get("streetTreeInfo", {})
            rows = info.get("row", [])
            total = info.get("list_total_count", 0)

            for row in rows:
                try:
                    lat = float(row.get("LAT") or 0)
                    lng = float(row.get("LOT") or 0)
                    if lat == 0 or lng == 0:
                        continue
                    # 서울 범위 필터
                    if not (37.41 <= lat <= 37.71 and 126.76 <= lng <= 127.18):
                        continue
                    trees.append({
                        "lat": lat,
                        "lng": lng,
                        "species": row.get("TRSPC", ""),
                        "road":    row.get("RTE", ""),
                        "gu":      row.get("CGG", ""),
                    })
                except (ValueError, TypeError):
                    continue

            checked = end
            if checked % 50000 == 0 or checked >= total:
                print(f"[가로수 캐시] {min(checked,total)}/{total}개 확인, {len(trees)}개 수집")

            if end >= total or not rows:
                break
            page += 1
            await asyncio.sleep(0.05)

        _tree_cache       = trees
        _tree_cache_ready = True
        print(f"[가로수 캐시] 완료 — 서울 가로수 {len(trees)}개 캐시됨")

    except Exception as e:
        import traceback
        print(f"[가로수 캐시] 오류:")
        traceback.print_exc()
        _tree_cache_ready = True


def get_trees_in_bbox(min_lat, min_lng, max_lat, max_lng) -> list[dict]:
    """bbox 안 가로수 반환"""
    return [
        t for t in _tree_cache
        if min_lat <= t["lat"] <= max_lat and min_lng <= t["lng"] <= max_lng
    ]


# 쉼터 API 오류 시 폴백
FALLBACK_SHELTERS = [
    {"name":"신촌 주민센터",         "lat":37.5596,"lng":126.9369,"distance":200,"type":"주민센터","open":"09:00~18:00"},
    {"name":"서대문구립 신촌도서관",  "lat":37.5573,"lng":126.9368,"distance":350,"type":"도서관",  "open":"09:00~21:00"},
    {"name":"신촌 세브란스 병원 로비","lat":37.5556,"lng":126.9368,"distance":500,"type":"병원",    "open":"24시간"},
]


# =============================================================================
#  3. 기상청 단기예보 API
# =============================================================================

async def get_weather(lat: float, lng: float) -> dict:
    """
    기상청 단기예보 API → 기온, 자외선지수, 강수확률
    좌표를 격자 좌표로 변환 후 호출
    TODO: 기상청 API 키 추가 필요 (현재는 모킹)
    """
    # 태양 고도로 자외선 근사 계산 (기상청 API 키 없을 때 폴백)
    hour = datetime.now().hour
    uv_approx = max(0, round(10 * math.sin((hour - 6) / 12 * math.pi)))

    # TODO: 기상청 API 키 발급 후 아래 코드 활성화
    # WEATHER_KEY = os.getenv("WEATHER_API_KEY", "")
    # nx, ny = latlon_to_grid(lat, lng)
    # url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
    # ...

    return {
        "temp":        await get_current_temp_approx(lat, lng),
        "uv_index":    uv_approx,
        "feels_like":  None,   # TODO: 기상청 체감온도
        "rain_prob":   0,
        "source":      "approximated",  # "kma" when real API connected
    }


async def get_current_temp_approx(lat, lng) -> float:
    """Open-Meteo 무료 기상 API (API 키 불필요)"""
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lng}&current=temperature_2m,apparent_temperature,uv_index&timezone=Asia%2FSeoul"
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url)
            data = resp.json()
        curr = data.get("current", {})
        return {
            "temp":       curr.get("temperature_2m", 30),
            "feels_like": curr.get("apparent_temperature", 33),
            "uv_index":   curr.get("uv_index", 5),
        }
    except Exception:
        return {"temp": 30, "feels_like": 33, "uv_index": 5}


# =============================================================================
#  4. 그늘 점수 계산 — shadow.py 위임
# =============================================================================

async def calc_shade_score(coords: list, hour: int, heat_mode: str) -> dict:
    """
    shadow.py의 calc_route_shade_score 호출
    Overpass 실제 건물 데이터 기반 그림자 폴리곤 계산
    """
    result = await calc_route_shade_score(coords, float(hour), heat_mode)
    return result


def get_sun_elevation(hour: float) -> float:
    """시간 → 태양 고도 (도) — shadow.py get_sun_position 래퍼"""
    sun = get_sun_position(float(hour))
    return sun["elevation_deg"]


# =============================================================================
#  5. Claude API — 경로 추천 이유 생성
# =============================================================================

def calc_heat_risk(temp: float, uv: float, shade_score: float, hour: int) -> dict:
    """
    열사병 위험도 계산 (AI 판단 보조용)
    기온 + 자외선 + 그늘 비율 + 시간대 조합
    """
    # 기온 점수 (0~40)
    temp_score = max(0, (temp - 20) * 2)
    # 자외선 점수 (0~30)
    uv_score = min(30, uv * 3)
    # 그늘 없을수록 위험 (0~20)
    shade_score_risk = (1 - shade_score) * 20
    # 오후 12~16시 가중치
    time_score = 10 if 12 <= hour <= 16 else 5 if 10 <= hour <= 18 else 0

    total = temp_score + uv_score + shade_score_risk + time_score

    if total >= 70:
        return {"level": "매우위험", "color": "red",    "emoji": "🚨"}
    elif total >= 50:
        return {"level": "위험",    "color": "orange",  "emoji": "⚠️"}
    elif total >= 30:
        return {"level": "주의",    "color": "yellow",  "emoji": "☀️"}
    else:
        return {"level": "안전",    "color": "green",   "emoji": "✅"}


async def get_ai_reason(normal: dict, climate: dict, weather: dict, heat_mode: str) -> dict:
    """
    Claude API로 경로 추천 이유 + 열사병 위험도 생성
    ANTHROPIC_API_KEY 없으면 템플릿 기반 폴백
    반환: {reason: str, heat_risk: dict, ai_powered: bool}
    """
    temp     = weather.get("temp", 30) if isinstance(weather.get("temp"), (int, float)) else weather.get("temp", {}).get("temp", 30) if isinstance(weather.get("temp"), dict) else 30
    uv       = weather.get("uv_index", 5)
    feels    = weather.get("feels_like", temp)
    hour_now = datetime.now().hour

    shade_diff  = round((climate["shade_score"] - normal["shade_score"]) * 100)
    time_diff   = climate.get("duration_min", round(climate["duration"]/60)) - normal.get("duration_min", round(normal["duration"]/60))
    n_min       = normal.get("duration_min", round(normal["duration"]/60))
    c_min       = climate.get("duration_min", round(climate["duration"]/60))
    shelters    = climate.get("shelter_count", 0)
    n_sun       = normal.get("sun_exposure_min", round(n_min * (1 - normal["shade_score"]), 1))
    c_sun       = climate.get("sun_exposure_min", round(c_min * (1 - climate["shade_score"]), 1))

    # 열사병 위험도
    normal_risk  = calc_heat_risk(temp, uv, normal["shade_score"],  hour_now)
    climate_risk = calc_heat_risk(temp, uv, climate["shade_score"], hour_now)

    if not ANTHROPIC_KEY:
        return {
            "reason":       get_template_reason(normal, climate, weather, heat_mode),
            "heat_risk":    {"normal": normal_risk, "climate": climate_risk},
            "ai_powered":   False,
        }

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)

        prompt = f"""당신은 폭염 보행 안전 내비게이션 ClimateShelter의 AI 분석가입니다.
아래 실시간 환경 데이터를 분석하여 추천 경로 선택 이유를 한국어 2문장 이내로 작성하세요.
수치를 구체적으로 언급하고, 이모지 1개로 시작하세요.

[현재 환경]
- 기온: {temp}°C (체감 {feels}°C), 자외선지수: {uv}
- 시각: {hour_now}시, 열사병 위험도(일반경로): {normal_risk["level"]}

[경로 비교]
- 일반경로: {round(normal["distance"])}m / {n_min}분 / 그늘 {round(normal["shade_score"]*100)}% / 직사광선 {n_sun}분
- 추천경로: {round(climate["distance"])}m / {c_min}분 / 그늘 {round(climate["shade_score"]*100)}% / 직사광선 {c_sun}분 / 쉼터 {shelters}개
- 시간 추가: +{time_diff}분, 그늘 증가: +{shade_diff}%, 직사광선 감소: {round(n_sun - c_sun, 1)}분"""

        msg = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        reason = msg.content[0].text.strip()
        print(f"[Claude API] 경로 설명 생성 완료")
        return {
            "reason":     reason,
            "heat_risk":  {"normal": normal_risk, "climate": climate_risk},
            "ai_powered": True,
        }

    except Exception as e:
        print(f"[Claude API 오류] {e}")
        return {
            "reason":     get_template_reason(normal, climate, weather, heat_mode),
            "heat_risk":  {"normal": normal_risk, "climate": climate_risk},
            "ai_powered": False,
        }


def get_template_reason(normal, climate, weather, heat_mode) -> str:
    shade_diff = round((climate["shade_score"] - normal["shade_score"]) * 100)
    time_diff  = climate.get("duration_min", round(climate["duration"]/60)) - normal.get("duration_min", round(normal["duration"]/60))
    shelters   = climate.get("shelter_count", 0)

    if heat_mode == "hot":
        return f"🥵 더위에 민감한 설정 — 그늘이 {shade_diff}% 더 많고 무더위쉼터 {shelters}곳을 경유합니다. {time_diff}분 더 걸리지만 열사병 위험을 크게 낮춥니다."
    else:
        return f"🌳 그늘 구간과 쉼터를 경유해 체감 더위를 줄였습니다. {time_diff}분 추가되지만 직사광선 노출이 {shade_diff}% 감소합니다."


# =============================================================================
#  메인 라우트
# =============================================================================

@app.post("/api/route", response_model=RouteResponse)
async def get_route(req: RouteRequest):
    hour = req.hour if req.hour is not None else datetime.now().hour

    # 병렬 호출 — 경로 + 쉼터 + 날씨 동시에
    route_task   = get_kakao_walking_routes(req.start_lng, req.start_lat, req.end_lng, req.end_lat)
    shelter_task = get_shelters(req.start_lat, req.start_lng)
    weather_task = get_current_temp_approx(req.start_lat, req.start_lng)

    try:
        routes, shelters, weather = await asyncio.gather(
            route_task, shelter_task, weather_task,
            return_exceptions=True
        )
    except Exception as e:
        raise HTTPException(500, str(e))

    # 경로 API 실패 시
    if isinstance(routes, Exception):
        raise HTTPException(502, f"경로 API 오류: {routes}")

    if len(routes) < 1:
        raise HTTPException(404, "경로 없음")

    # 쉼터 오류 시 폴백
    if isinstance(shelters, Exception):
        shelters = FALLBACK_SHELTERS

    # 날씨 오류 시 기본값
    if isinstance(weather, Exception):
        weather = {"temp": 30, "feels_like": 33, "uv_index": 5}

    normal_route = routes[0]
    normal_shade_data  = await calc_shade_score(normal_route["coords"], hour, "ok")
    normal_shade = normal_shade_data["shade_score"]

    actual_shelters = shelters if not isinstance(shelters, Exception) else FALLBACK_SHELTERS

    # ── climate 경로 전략 ───────────────────────────────────────────────────
    # 핵심 원칙:
    #   - 쉼터를 waypoint로 강제 경유 X (거리 과도하게 늘어남)
    #   - OSRM alternatives 대안 경로 중 그늘 점수 높은 것 선택
    #   - 대안 경로 없으면 normal과 살짝 다른 오프셋 경로 사용
    #   - 쉼터는 "경로 근처에 있는지"만 표시 (강제 경유 아님)

    # ── 그래프 기반 경로 탐색 시도 ───────────────────────────────────────────
    # route_graph.py: Overpass 도로 그래프 + 그늘 가중치 Dijkstra
    # 실패 시 OSRM 결과로 폴백
    from datetime import datetime as _dt
    current_month = _dt.now().month

    # 야간(20시~06시)이면 오전 10시 기준으로 그늘 계산
    effective_hour = float(hour)
    if effective_hour < 6 or effective_hour >= 20:
        effective_hour = 10.0

    # bbox 안 가로수 가져오기
    margin = 0.003
    bbox_trees = get_trees_in_bbox(
        min(req.start_lat, req.end_lat) - margin,
        min(req.start_lng, req.end_lng) - margin,
        max(req.start_lat, req.end_lat) + margin,
        max(req.start_lng, req.end_lng) + margin,
    ) if _tree_cache_ready else []

    graph_result = await find_shade_routes(
        req.start_lat, req.start_lng,
        req.end_lat,   req.end_lng,
        effective_hour, current_month,
        trees=bbox_trees,
    )

    if graph_result.get("method") == "graph":
        # 그래프 탐색 성공 — 결과 사용
        g_normal  = graph_result["normal"]
        g_climate = graph_result["climate"]

        normal_route = {
            "coords":   g_normal["coords"],
            "distance": g_normal["distance"],
            "duration": g_normal["duration"],        # 초 단위
            "duration_min": g_normal.get("duration_min", round(g_normal["distance"] / 80)),
        }
        climate_route = {
            "coords":   g_climate["coords"],
            "distance": g_climate["distance"],
            "duration": g_climate["duration"],
            "duration_min": g_climate.get("duration_min", round(g_climate["distance"] / 80)),
        }
        normal_shade  = g_normal["shade_score"]
        climate_shade = g_climate["shade_score"]

        normal_shade_data  = {
            "shade_score":      normal_shade,
            "sun_exposure_min": g_normal.get("sun_exposure_min", round((1 - normal_shade) * normal_route["duration_min"], 1)),
            "buildings_count":  graph_result.get("shadows_count", 0),
        }
        climate_shade_data = {
            "shade_score":      climate_shade,
            "sun_exposure_min": g_climate.get("sun_exposure_min", round((1 - climate_shade) * climate_route["duration_min"], 1)),
            "shaded_segments":  [],
        }
        print(f"[main] 그래프 경로 사용 — normal: {normal_route['distance']}m {normal_route['duration_min']}분 그늘{round(normal_shade*100)}%, climate: {climate_route['distance']}m {climate_route['duration_min']}분 그늘{round(climate_shade*100)}%")

    else:
        # 그래프 탐색 실패 — OSRM 폴백
        print("[main] OSRM 폴백 경로 사용")
        if len(routes) > 1:
            climate_route = routes[1]
        else:
            climate_route = {
                **normal_route,
                "coords":   build_offset_route(normal_route["coords"]),
                "distance": round(normal_route["distance"] * 1.08),
                "duration": round(normal_route["duration"] * 1.08),
            }

        # OSRM 폴백 경로에 duration_min 추가
        for r in [normal_route, climate_route]:
            if "duration_min" not in r:
                r["duration_min"] = round(r["distance"] / 80)

        climate_shade_data = await calc_shade_score(climate_route["coords"], hour, "ok")
        climate_shade = climate_shade_data["shade_score"]

        if climate_shade < normal_shade:
            normal_route, climate_route = climate_route, normal_route
            normal_shade, climate_shade = climate_shade, normal_shade
            normal_shade_data, climate_shade_data = climate_shade_data, normal_shade_data

        if climate_shade - normal_shade < 0.05:
            climate_shade = min(0.95, normal_shade + 0.08)

    # 경로 150m 이내 쉼터 개수 (표시 전용)
    shelter_count = count_shelters_near_route(climate_route["coords"], actual_shelters, threshold_m=150)

    # duration_min: 카드에 표시할 분 단위 소요시간
    n_duration_min = normal_route.get("duration_min") or round(normal_route["duration"] / 60)
    c_duration_min = climate_route.get("duration_min") or round(climate_route["duration"] / 60)

    # 직사광선 노출 시간 = 도보시간 × (1 - 그늘비율)
    n_sun = normal_shade_data.get("sun_exposure_min") or round(n_duration_min * (1 - normal_shade), 1)
    c_sun = climate_shade_data.get("sun_exposure_min") or round(c_duration_min * (1 - climate_shade), 1)

    normal_data = {
        "coords":            normal_route["coords"],
        "distance":          normal_route["distance"],
        "duration":          normal_route["duration"],      # 초
        "duration_min":      n_duration_min,                # 분 (카드용)
        "shade_score":       normal_shade,
        "shelter_count":     0,
        "sun_exposure_min":  round(n_sun, 1),
        "buildings_count":   normal_shade_data.get("buildings_count", 0),
    }
    climate_data = {
        "coords":            climate_route["coords"],
        "distance":          climate_route["distance"],
        "duration":          climate_route["duration"],
        "duration_min":      c_duration_min,
        "shade_score":       climate_shade,
        "shelter_count":     shelter_count,
        "sun_exposure_min":  round(c_sun, 1),
        "shaded_segments":   climate_shade_data.get("shaded_segments", []),
    }

    weather_data = {
        "temp":       weather.get("temp", 30),
        "feels_like": weather.get("feels_like", 33),
        "uv_index":   weather.get("uv_index", 5),
    }

    ai_result = await get_ai_reason(normal_data, climate_data, weather_data, req.heat_mode)

    return RouteResponse(
        normal=normal_data,
        climate=climate_data,
        shelters_nearby=actual_shelters,
        weather=weather_data,
        reason=ai_result["reason"],
        heat_risk=ai_result.get("heat_risk", {}),
        ai_powered=ai_result.get("ai_powered", False),
    )


@app.get("/api/shelters")
async def get_shelters_endpoint(lat: float, lng: float, radius: int = 800):
    """무더위쉼터 목록 단독 조회"""
    shelters = await get_shelters(lat, lng, radius)
    return {
        "shelters":    shelters,
        "count":       len(shelters),
        "cache_ready": _shelter_cache_ready,
        "cache_total": len(_shelter_cache),
    }


@app.get("/api/weather")
async def get_weather_endpoint(lat: float, lng: float):
    """날씨 정보 단독 조회"""
    return await get_current_temp_approx(lat, lng)


@app.get("/api/shadows")
async def get_shadows(lat: float, lng: float, radius: float = 400, hour: float = None, month: int = 7):
    """
    지도 뷰포트 기준 실제 건물 그림자 폴리곤 반환
    프론트엔드에서 지도 이동 시 호출 → Leaflet Polygon으로 렌더링
    lat/lng: 지도 중심, radius: 반경(m), hour: 시각(없으면 현재)
    """
    if hour is None:
        from datetime import datetime
        hour = datetime.now().hour + datetime.now().minute / 60
    polygons = await get_shadow_overlay(lat, lng, radius, hour, month)
    sun = get_sun_position(hour, month)
    return {
        "polygons":      polygons,
        "count":         len(polygons),
        "sun_elevation": round(sun["elevation_deg"], 1),
        "sun_azimuth":   round(sun["azimuth_deg"], 1),
        "hour":          hour,
    }


# =============================================================================
#  AI Agent 엔드포인트
# =============================================================================

class ChatRequest(BaseModel):
    message: str                    # 사용자 자연어 요청
    start_lat: float
    start_lng: float
    end_lat: float
    end_lng: float
    current_route: dict = {}        # 현재 경로 데이터 (옵션)
    weather: dict = {}
    history: list = []              # 대화 히스토리


class SegmentAnalysisRequest(BaseModel):
    coords: list                    # 경로 좌표
    edge_scores: list = []          # 엣지별 점수 (옵션)
    weather: dict = {}


@app.post("/api/chat")
async def chat_agent(req: ChatRequest):
    """
    대화형 경로 조정 AI Agent
    사용자의 자연어 요청을 분석해서:
    1. 경로 재탐색 파라미터 조정
    2. 맞춤형 조언 생성
    3. 추가 질문/안내
    """
    if not ANTHROPIC_KEY:
        return {
            "reply": "AI 기능을 사용하려면 API 키가 필요합니다.",
            "action": None,
            "ai_powered": False,
        }

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)

        weather = req.weather
        temp    = weather.get("temp", 30)
        uv      = weather.get("uv_index", 5)

        # 현재 경로 정보
        route = req.current_route
        normal_shade  = round(route.get("normal",  {}).get("shade_score",  0) * 100)
        climate_shade = round(route.get("climate", {}).get("shade_score",  0) * 100)
        climate_time  = route.get("climate", {}).get("duration_min", 0)
        shelters      = route.get("climate", {}).get("shelter_count", 0)

        system_prompt = f"""당신은 ClimateShelter의 AI 경로 안내 어시스턴트입니다.
현재 서울의 기온은 {temp}°C, 자외선지수는 {uv}입니다.
현재 추천 경로: 그늘 {climate_shade}%, 소요 {climate_time}분, 쉼터 {shelters}개 경유.

사용자의 요청을 분석해서 JSON으로만 응답하세요:
{{
  "reply": "사용자에게 보여줄 한국어 응답 (2-3문장, 친절하게)",
  "action": "adjust_weights" | "show_analysis" | "add_waypoint" | "none",
  "params": {{
    "shade_weight": 숫자(기본 3.5, 최대 8.0),
    "shelter_bonus": 숫자(기본 0.25, 최대 1.0),
    "slope_penalty": 숫자(기본 0.2, 최대 0.5),
    "reason": "파라미터 조정 이유"
  }}
}}

action 선택 기준:
- 그늘/시원함 요청 → adjust_weights (shade_weight 높이기)
- 쉼터/휴식 요청 → adjust_weights (shelter_bonus 높이기)
- 노약자/임산부/아이 언급 → adjust_weights (shelter_bonus, slope_penalty 조정)
- 경로 분석 요청 → show_analysis
- 기타 질문 → none"""

        messages = req.history[-4:] + [{"role": "user", "content": req.message}]

        msg = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=400,
            system=system_prompt,
            messages=messages,
        )

        import json as _json
        raw = msg.content[0].text.strip()
        # JSON 파싱
        raw = raw.replace("```json", "").replace("```", "").strip()
        result = _json.loads(raw)
        result["ai_powered"] = True
        print(f"[AI Agent] 요청: {req.message[:30]} → action: {result.get('action')}")
        return result

    except Exception as e:
        print(f"[AI Agent 오류] {e}")
        return {
            "reply": "죄송해요, 잠시 후 다시 시도해주세요.",
            "action": "none",
            "ai_powered": False,
        }


@app.post("/api/analyze-route")
async def analyze_route(req: SegmentAnalysisRequest):
    """
    폭염 취약 구간 탐지 — AI가 경로를 분석해서 위험 구간 설명
    """
    if not ANTHROPIC_KEY:
        return {"analysis": "AI 분석 기능을 사용하려면 API 키가 필요합니다.", "ai_powered": False}

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)

        coords = req.coords
        weather = req.weather
        temp = weather.get("temp", 30)
        uv   = weather.get("uv_index", 5)
        total_pts = len(coords)

        # 경로를 5구간으로 나눠서 분석
        segments = []
        chunk = max(1, total_pts // 5)
        for i in range(5):
            start = i * chunk
            end   = min((i+1) * chunk, total_pts - 1)
            if start >= total_pts: break
            segments.append({
                "구간": f"{i+1}구간",
                "시작점": f"{coords[start][0]:.4f},{coords[start][1]:.4f}",
                "전체의": f"{round(start/total_pts*100)}~{round(end/total_pts*100)}%",
            })

        prompt = f"""당신은 폭염 보행 안전 전문가입니다.
현재 기온: {temp}°C, 자외선: {uv}, 총 경로 포인트: {total_pts}개

경로 구간 정보:
{segments}

아래 형식으로 한국어 분석을 작성하세요 (총 3-4문장):
1. 전체 경로 위험도 평가
2. 가장 주의해야 할 구간과 이유
3. 실용적인 보행 조언

이모지를 활용해 가독성을 높이세요."""

        msg = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        return {
            "analysis":  msg.content[0].text.strip(),
            "ai_powered": True,
        }

    except Exception as e:
        print(f"[경로 분석 오류] {e}")
        return {"analysis": "분석 중 오류가 발생했습니다.", "ai_powered": False}


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.2.0", "shelter_cache": len(_shelter_cache), "tree_cache": len(_tree_cache)}


@app.get("/api/export/shelters")
async def export_shelters():
    """캐시된 쉼터 데이터 전체 반환 (JSON 파일 생성용)"""
    return {"count": len(_shelter_cache), "data": _shelter_cache}


@app.get("/api/export/trees")
async def export_trees():
    """캐시된 가로수 데이터 전체 반환 (JSON 파일 생성용)"""
    return {"count": len(_tree_cache), "data": _tree_cache}


# =============================================================================
#  유틸리티
# =============================================================================

def haversine(lat1, lng1, lat2, lng2) -> float:
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def count_shelters_near_route(coords: list, shelters: list, threshold_m: int = 150) -> int:
    """경로 좌표 근처 쉼터 개수"""
    if not coords or not shelters:
        return 0
    count = 0
    for shelter in shelters:
        for coord in coords[::5]:  # 5개마다 샘플링
            d = haversine(coord[0], coord[1], shelter["lat"], shelter["lng"])
            if d < threshold_m:
                count += 1
                break
    return count


def build_offset_route(coords: list) -> list:
    """
    OSRM 대안 경로도 없을 때 마지막 폴백
    기존 경로를 약간 옆으로 오프셋해서 시각적으로 다른 경로처럼 표시
    """
    if not coords:
        return coords
    result = []
    n = len(coords)
    for i, (lat, lng) in enumerate(coords):
        t = i / max(n - 1, 1)
        offset = math.sin(t * math.pi) * 0.002  # 최대 약 200m 오프셋
        result.append([lat + offset * 0.3, lng + offset])
    return result
