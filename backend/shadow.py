"""
shadow.py — 실제 건물 데이터 기반 그림자/그늘 계산
=====================================================
1. Overpass API로 경로 bounding box 안 건물 가져오기
2. 태양 방위각/고도 계산 (서울 기준)
3. 각 건물의 그림자 폴리곤 생성
4. 경로 세그먼트가 그림자 안에 있는 비율 → 그늘 점수

서울 전체를 한 번에 가져오지 않고,
경로 bounding box(+여유 300m)만 쿼리해서 속도 유지
"""

import math
import asyncio
import hashlib
import time
from typing import Optional
import httpx

# =============================================================================
#  캐시 (메모리) — 같은 bbox는 10분간 재사용
# =============================================================================

_building_cache: dict[str, tuple[float, list]] = {}  # key → (timestamp, buildings)
CACHE_TTL = 3600  # 1시간


def _cache_key(min_lat, min_lng, max_lat, max_lng) -> str:
    # 소수점 3자리로 반올림해서 키 생성 (약 100m 단위)
    coords = f"{min_lat:.3f},{min_lng:.3f},{max_lat:.3f},{max_lng:.3f}"
    return hashlib.md5(coords.encode()).hexdigest()[:12]


def _get_cached(key: str) -> Optional[list]:
    if key in _building_cache:
        ts, data = _building_cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
        del _building_cache[key]
    return None


def _set_cache(key: str, data: list):
    _building_cache[key] = (time.time(), data)


# =============================================================================
#  1. Overpass — 건물 데이터
# =============================================================================

async def fetch_buildings(min_lat: float, min_lng: float,
                          max_lat: float, max_lng: float) -> list[dict]:
    """
    Overpass API로 bounding box 안의 건물 가져오기
    반환: [{lat, lng, width, height_m, levels}, ...]
    """
    key = _cache_key(min_lat, min_lng, max_lat, max_lng)
    cached = _get_cached(key)
    if cached is not None:
        return cached

    # Overpass QL 쿼리
    # building 태그 있는 way만, 불필요한 태그 제외
    query = f"""
[out:json][timeout:15];
(
  way["building"]({min_lat},{min_lng},{max_lat},{max_lng});
);
out center tags;
"""

    headers = {
        "User-Agent": "ClimateShelter/1.0 (climate navigation app)",
        "Accept": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=18) as client:
            resp = await client.post(
                "https://overpass-api.de/api/interpreter",
                data={"data": query},
                headers=headers,
            )
        if resp.status_code != 200:
            return []

        data = resp.json()
        buildings = []

        for elem in data.get("elements", []):
            center = elem.get("center", {})
            if not center:
                continue

            tags = elem.get("tags", {})

            # 층수 → 높이 추정 (층당 3m, 기본 4층)
            levels = _parse_int(tags.get("building:levels", tags.get("levels", "4")), 4)
            height_m = _parse_float(tags.get("height", tags.get("building:height", "")),
                                     levels * 3.0)

            buildings.append({
                "lat":      center.get("lat", 0),
                "lng":      center.get("lon", 0),
                "height_m": height_m,
                "levels":   levels,
                # 건물 크기 추정 (없으면 기본값, 도로 그림자 계산용)
                "width":    _parse_float(tags.get("width", ""), 20.0),   # m
                "depth":    _parse_float(tags.get("depth", ""), 20.0),   # m
            })

        _set_cache(key, buildings)
        return buildings

    except Exception as e:
        print(f"[Overpass 오류] {e}")
        return []


# =============================================================================
#  2. 태양 위치 계산 (서울 기준)
# =============================================================================

def get_sun_position(hour: float, month: int = 7) -> dict:
    """
    서울(37.5°N, 127°E) 기준 태양 방위각/고도 계산
    hour: 0~24 (소수점 가능)
    month: 1~12 (여름=7 기본값, 계절별 보정)

    반환:
      azimuth_deg: 북 기준 시계방향 (0=북, 90=동, 180=남, 270=서)
      elevation_deg: 지평선 위 각도 (0~90)
      shadow_len_ratio: 건물 높이 대비 그림자 길이 비율
    """
    lat_rad = math.radians(37.5)

    # 적위 (declination) — 계절별
    # 여름(7월) ≈ +23.4°, 겨울(1월) ≈ -23.4°, 봄/가을 ≈ 0°
    declination_deg = 23.45 * math.sin(math.radians((284 + (month - 1) * 30) * 360 / 365))
    declination_rad = math.radians(declination_deg)

    # 시간각 (서울 경도 127° 기준)
    # 태양시 = 표준시 + (경도 - 표준경도) * 4분/도
    solar_time = hour + (127 - 135) * 4 / 60  # 한국 표준시는 135° 기준
    hour_angle_rad = math.radians((solar_time - 12) * 15)

    # 태양 고도
    sin_elev = (math.sin(lat_rad) * math.sin(declination_rad) +
                math.cos(lat_rad) * math.cos(declination_rad) * math.cos(hour_angle_rad))
    elevation_rad = math.asin(max(-1, min(1, sin_elev)))
    elevation_deg = math.degrees(elevation_rad)

    # 태양 방위각
    cos_az = ((math.sin(declination_rad) - math.sin(lat_rad) * sin_elev) /
              (math.cos(lat_rad) * math.cos(elevation_rad) + 1e-10))
    azimuth_rad = math.acos(max(-1, min(1, cos_az)))
    azimuth_deg = math.degrees(azimuth_rad)

    # 오후엔 서쪽 (방위각 > 180)
    if hour_angle_rad > 0:
        azimuth_deg = 360 - azimuth_deg

    # 그림자 길이 비율 (높이 1m당 그림자 m)
    if elevation_deg <= 0:
        shadow_len_ratio = 999  # 일몰/일출 전후
    else:
        shadow_len_ratio = 1.0 / math.tan(elevation_rad)

    return {
        "azimuth_deg":       azimuth_deg,
        "elevation_deg":     max(0, elevation_deg),
        "shadow_len_ratio":  min(shadow_len_ratio, 20),  # 최대 20배 제한
    }


# =============================================================================
#  3. 그림자 폴리곤 생성
# =============================================================================

def building_shadow_polygon(building: dict, sun: dict) -> list[tuple]:
    """
    건물 중심 + 태양 위치 → 그림자 폴리곤 꼭짓점 (lat/lng)
    단순화: 건물을 원형으로 근사해서 그림자를 타원으로 표현
    """
    blat = building["lat"]
    blng = building["lng"]
    h    = building["height_m"]
    r    = max(building["width"], building["depth"]) / 2  # 건물 반경 (m)

    az_rad  = math.radians(sun["azimuth_deg"])
    sh_len  = h * sun["shadow_len_ratio"]  # 그림자 길이 (m)

    # 그림자 방향 = 태양 반대 방향
    shadow_az_rad = az_rad + math.pi

    # 1m → 위도/경도 변환
    m_per_lat = 111320  # 위도 1도 ≈ 111,320m
    m_per_lng = 111320 * math.cos(math.radians(blat))

    # 그림자 끝점
    shadow_tip_lat = blat + (sh_len * math.cos(shadow_az_rad)) / m_per_lat
    shadow_tip_lng = blng + (sh_len * math.sin(shadow_az_rad)) / m_per_lng

    # 폴리곤: 건물 좌/우 양끝 + 그림자 끝 양끝
    perp_az = az_rad + math.pi / 2
    w = r / 2  # 폴리곤 폭

    def offset(lat, lng, dist_m, angle_rad):
        return (
            lat + (dist_m * math.cos(angle_rad)) / m_per_lat,
            lng + (dist_m * math.sin(angle_rad)) / m_per_lng,
        )

    p1 = offset(blat, blng, w, perp_az)
    p2 = offset(blat, blng, w, perp_az + math.pi)
    p3 = offset(shadow_tip_lat, shadow_tip_lng, w * 0.3, perp_az + math.pi)
    p4 = offset(shadow_tip_lat, shadow_tip_lng, w * 0.3, perp_az)

    return [p1, p2, p3, p4]


# =============================================================================
#  4. 경로 그늘 점수 계산
# =============================================================================

def point_in_polygon(lat: float, lng: float, polygon: list[tuple]) -> bool:
    """Ray casting으로 점이 폴리곤 안에 있는지 확인"""
    n = len(polygon)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if ((yi > lng) != (yj > lng)) and (lat < (xj - xi) * (lng - yi) / (yj - yi + 1e-10) + xi):
            inside = not inside
        j = i
    return inside


async def calc_route_shade_score(
    coords: list[list],
    hour: float,
    heat_mode: str,
    month: int = 7,
) -> dict:
    """
    경로 좌표 리스트 → 상세 그늘 분석 결과

    반환:
      shade_score: 0~1 (전체 경로 중 그늘 비율)
      shaded_segments: 그늘 구간 좌표 리스트
      sun_exposure_min: 직사광선 노출 시간 (분)
      buildings_count: 분석에 사용된 건물 수
    """
    if not coords or len(coords) < 2:
        return {"shade_score": 0.3, "shaded_segments": [], "sun_exposure_min": 0, "buildings_count": 0}

    # 경로 bounding box + 300m 여유
    lats = [c[0] for c in coords]
    lngs = [c[1] for c in coords]
    margin = 0.003  # 약 300m

    min_lat, max_lat = min(lats) - margin, max(lats) + margin
    min_lng, max_lng = min(lngs) - margin, max(lngs) + margin

    # 태양 위치
    sun = get_sun_position(hour, month)

    # 야간이면 그늘 100%
    if sun["elevation_deg"] <= 0:
        total_dist = sum(
            haversine_m(coords[i][0], coords[i][1], coords[i+1][0], coords[i+1][1])
            for i in range(len(coords)-1)
        )
        return {
            "shade_score": 1.0,
            "shaded_segments": coords,
            "sun_exposure_min": 0,
            "buildings_count": 0,
        }

    # 건물 데이터 가져오기
    buildings = await fetch_buildings(min_lat, min_lng, max_lat, max_lng)

    # 건물별 그림자 폴리곤 미리 계산
    shadow_polygons = []
    for b in buildings:
        if b["height_m"] < 6:  # 2층 이하 무시
            continue
        poly = building_shadow_polygon(b, sun)
        shadow_polygons.append(poly)

    # 경로 세그먼트마다 그늘 여부 체크
    total_dist   = 0.0
    shaded_dist  = 0.0
    shaded_segs  = []

    # 세그먼트 중간점 샘플링 (너무 촘촘하면 느려짐)
    sample_step = max(1, len(coords) // 80)

    for i in range(0, len(coords) - 1, sample_step):
        a, b_ = coords[i], coords[min(i + sample_step, len(coords) - 1)]
        mid_lat = (a[0] + b_[0]) / 2
        mid_lng = (a[1] + b_[1]) / 2
        seg_dist = haversine_m(a[0], a[1], b_[0], b_[1])
        total_dist += seg_dist

        # 이 세그먼트 중간점이 어떤 그림자 폴리곤 안에 있으면 그늘
        in_shade = any(
            point_in_polygon(mid_lat, mid_lng, poly)
            for poly in shadow_polygons
        )

        if in_shade:
            shaded_dist += seg_dist
            shaded_segs.append([a, b_])

    shade_score = round(shaded_dist / max(total_dist, 1), 3)

    # 더워요 모드는 그늘 경로를 선택한 결과이므로 자연스럽게 높은 점수가 나옴
    # (경로 자체가 다르기 때문 — B단계 구현 후 적용)

    # 직사광선 노출 시간 (분) = (1 - shade_score) * 총 도보시간
    # 도보 속도: 약 70m/분
    walk_min = total_dist / 70
    sun_exposure_min = round((1 - shade_score) * walk_min, 1)

    return {
        "shade_score":      shade_score,
        "shaded_segments":  shaded_segs[:50],  # 너무 많으면 응답 크기 과다
        "sun_exposure_min": sun_exposure_min,
        "buildings_count":  len(buildings),
    }


# =============================================================================
#  5. 그림자 폴리곤 직렬화 (프론트엔드 전송용)
# =============================================================================

async def get_shadow_overlay(
    center_lat: float,
    center_lng: float,
    radius_m: float,
    hour: float,
    month: int = 7,
) -> list[dict]:
    """
    지도 뷰포트 기준 그림자 폴리곤 목록 반환
    프론트엔드 Leaflet Polygon으로 바로 렌더링 가능

    반환: [{points: [[lat,lng],...], opacity: float}, ...]
    """
    margin = radius_m / 111320
    min_lat = center_lat - margin
    max_lat = center_lat + margin
    min_lng = center_lng - margin / math.cos(math.radians(center_lat))
    max_lng = center_lng + margin / math.cos(math.radians(center_lat))

    sun = get_sun_position(hour, month)
    if sun["elevation_deg"] <= 0:
        return []

    buildings = await fetch_buildings(min_lat, min_lng, max_lat, max_lng)

    result = []
    for b in buildings:
        if b["height_m"] < 6:
            continue
        poly = building_shadow_polygon(b, sun)
        # 높은 건물일수록 불투명
        opacity = min(0.45, 0.15 + b["height_m"] / 100)
        result.append({
            "points":  [[p[0], p[1]] for p in poly],
            "opacity": round(opacity, 2),
            "height":  round(b["height_m"]),
        })

    return result


# =============================================================================
#  유틸
# =============================================================================

def haversine_m(lat1, lng1, lat2, lng2) -> float:
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def _parse_int(val: str, default: int) -> int:
    try:
        return max(1, int(float(str(val).split(";")[0].strip())))
    except (ValueError, TypeError):
        return default


def _parse_float(val: str, default: float) -> float:
    try:
        return float(str(val).split(";")[0].strip())
    except (ValueError, TypeError):
        return default
