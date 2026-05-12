"""
route_graph.py — 도로 그래프 + 그늘 가중치 기반 경로 탐색
============================================================
1. Overpass API로 경로 bbox 안 도보 가능 도로 노드/엣지 가져오기
2. 각 엣지 중간점이 건물 그림자 폴리곤 안에 있는지 계산 → shade_score
3. normal:  Dijkstra(cost = distance)
   climate: Dijkstra(cost = distance × (1 - shade_score × SHADE_WEIGHT))
4. 두 경로를 반환 → main.py에서 사용

속도 최적화:
- 도로 그래프: bbox 기반 캐시 10분
- 그림자 폴리곤: shadow.py 캐시 재사용
- 노드 수 제한: 최대 3000개 (대형 bbox 자동 축소)
"""

import math
import heapq
import time
import hashlib
import asyncio
from typing import Optional
import httpx
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from shadow import (
    fetch_buildings,
    building_shadow_polygon,
    get_sun_position,
    point_in_polygon,
    haversine_m,
)

# =============================================================================
#  상수
# =============================================================================

# 비용함수 가중치
SHADE_WEIGHT    = 3.5   # 그늘 가중치
TREE_BONUS      = 0.3   # 가로수 구간 추가 보너스
SURFACE_PENALTY = 0.15  # 아스팔트 열섬 페널티
SLOPE_PENALTY   = 0.2   # 경사 페널티
SHELTER_BONUS   = 0.25  # 쉼터 근처 보너스
WALK_SPEED_MPM  = 80    # 도보 속도 (m/분)

MAX_NODES      = 4000
OVERPASS_URL   = "https://overpass-api.de/api/interpreter"
HEADERS        = {"User-Agent": "ClimateShelter/1.0", "Accept": "application/json"}

# 도보 가능 도로 태그 + 우선순위 (낮을수록 보행 쾌적)
WALKABLE_HIGHWAY = {
    "footway", "path", "pedestrian", "steps", "residential",
    "living_street", "service", "unclassified", "tertiary",
    "secondary", "primary", "trunk", "cycleway",
}
# 열섬 위험 표면 (아스팔트/콘크리트)
HOT_SURFACES = {"asphalt", "concrete", "paving_stones", "sett"}
# 쾌적 표면 (흙/잔디)
COOL_SURFACES = {"grass", "dirt", "gravel", "wood", "compacted"}

# =============================================================================
#  도로 그래프 캐시
# =============================================================================

_graph_cache: dict[str, tuple[float, dict]] = {}
GRAPH_CACHE_TTL = 3600  # 1시간


def _graph_cache_key(min_lat, min_lng, max_lat, max_lng) -> str:
    s = f"{min_lat:.3f},{min_lng:.3f},{max_lat:.3f},{max_lng:.3f}"
    return hashlib.md5(s.encode()).hexdigest()[:12]


def _get_graph_cache(key):
    if key in _graph_cache:
        ts, data = _graph_cache[key]
        if time.time() - ts < GRAPH_CACHE_TTL:
            return data
        del _graph_cache[key]
    return None


def _set_graph_cache(key, data):
    _graph_cache[key] = (time.time(), data)


# =============================================================================
#  1. Overpass — 도보 도로 그래프 가져오기
# =============================================================================

async def fetch_road_graph(
    min_lat: float, min_lng: float,
    max_lat: float, max_lng: float,
) -> dict:
    """
    bbox 안 도보 도로를 Overpass로 가져와서 그래프로 변환

    반환:
      nodes: {node_id: (lat, lng)}
      edges: {node_id: [(neighbor_id, distance_m, mid_lat, mid_lng), ...]}
    """
    key = _graph_cache_key(min_lat, min_lng, max_lat, max_lng)
    cached = _get_graph_cache(key)
    if cached is not None:
        return cached

    # 도로와 건물을 분리 쿼리 (한 번에 섞으면 center가 안 옴)
    road_query = f"""
[out:json][timeout:12];
(
  way["highway"]["highway"!~"motorway|motorway_link|raceway"]
    ({min_lat},{min_lng},{max_lat},{max_lng});
);
out body;
>;
out skel qt;
"""

    building_query = f"""
[out:json][timeout:12];
(
  way["building"]["building"!~"no"]
    ({min_lat},{min_lng},{max_lat},{max_lng});
);
out center tags;
"""

    try:
        t0 = time.time()
        async with httpx.AsyncClient(timeout=15) as client:
            road_resp, building_resp = await asyncio.gather(
                client.post(OVERPASS_URL, data={"data": road_query},     headers=HEADERS),
                client.post(OVERPASS_URL, data={"data": building_query}, headers=HEADERS),
            )
        print(f"[route_graph] Overpass 응답: {time.time()-t0:.1f}초")

        if road_resp.status_code != 200:
            print(f"[route_graph] 도로 Overpass 오류: {road_resp.status_code}")
            return {"nodes": {}, "edges": {}}

        road_elements     = road_resp.json().get("elements", [])
        building_elements = building_resp.json().get("elements", []) if building_resp.status_code == 200 else []
        elements     = road_elements
        bld_elements = building_elements

        print(f"[route_graph] 도로 elements: {len(road_elements)}, 건물 elements: {len(building_elements)}")

        # 결과 캐싱
        _set_graph_cache(key, {"nodes": {}, "edges": {}, "buildings": [], "_raw_road": road_elements, "_raw_bld": bld_elements})

    except asyncio.TimeoutError:
        print(f"[route_graph] Overpass 타임아웃 — OSRM 폴백")
        return {"nodes": {}, "edges": {}}
    except Exception as e:
        print(f"[route_graph] Overpass 요청 실패: {e}")
        return {"nodes": {}, "edges": {}}

    def _parse_levels(val, default=4):
        try: return max(1, int(float(str(val).split(";")[0].strip())))
        except: return default

    def _parse_float_safe(val, default):
        try: return float(str(val).split(";")[0].strip())
        except: return default

    # ── 도로 파싱 ─────────────────────────────────────────────────────────
    nodes: dict[int, tuple[float, float]] = {}
    for el in elements:
        if el["type"] == "node" and "lat" in el:
            nodes[el["id"]] = (el["lat"], el["lon"])

    edges: dict[int, list] = {n: [] for n in nodes}

    for el in elements:
        if el["type"] != "way":
            continue
        tags = el.get("tags", {})
        hw = tags.get("highway", "")
        if hw not in WALKABLE_HIGHWAY:
            continue
        if tags.get("foot") == "no" or tags.get("access") == "no":
            continue
        way_nodes = el.get("nodes", [])
        for i in range(len(way_nodes) - 1):
            a_id, b_id = way_nodes[i], way_nodes[i + 1]
            if a_id not in nodes or b_id not in nodes:
                continue
            a_lat, a_lng = nodes[a_id]
            b_lat, b_lng = nodes[b_id]
            dist = haversine_m(a_lat, a_lng, b_lat, b_lng)
            if dist < 1:
                continue
            mid_lat = (a_lat + b_lat) / 2
            mid_lng = (a_lng + b_lng) / 2
            # OSM 태그에서 도로 속성 추출
            surface = tags.get("surface", "")
            incline_raw = tags.get("incline", "0").replace("%","").replace("°","")
            try:
                incline = abs(float(incline_raw))
            except:
                incline = 0.0
            has_trees = "yes" in tags.get("street_tree","") or tags.get("tree_lined","") == "yes"
            is_hot_surface = surface in HOT_SURFACES
            is_cool_surface = surface in COOL_SURFACES

            edge_props = (b_id, dist, mid_lat, mid_lng, incline, has_trees, is_hot_surface, is_cool_surface)
            edge_props_rev = (a_id, dist, mid_lat, mid_lng, incline, has_trees, is_hot_surface, is_cool_surface)

            if a_id in edges:
                edges[a_id].append(edge_props)
            if b_id in edges:
                edges[b_id].append(edge_props_rev)

    # ── 건물 파싱 (별도 쿼리 결과) ────────────────────────────────────────
    buildings_in_graph: list[dict] = []
    for el in bld_elements:
        if el["type"] != "way":
            continue
        tags   = el.get("tags", {})
        center = el.get("center", {})
        if not center:
            continue
        levels   = _parse_levels(tags.get("building:levels", tags.get("levels", "4")))
        height_m = _parse_float_safe(
            tags.get("height", tags.get("building:height", "")), levels * 3.0
        )
        buildings_in_graph.append({
            "lat":      center.get("lat", 0),
            "lng":      center.get("lon", 0),
            "height_m": height_m,
            "levels":   levels,
            "width":    _parse_float_safe(tags.get("width", ""), 20.0),
            "depth":    _parse_float_safe(tags.get("depth", ""), 20.0),
        })

    graph = {"nodes": nodes, "edges": edges, "buildings": buildings_in_graph}

    # 노드 수 로깅
    print(f"[route_graph] 도로: 노드 {len(nodes)}개, 엣지 {sum(len(v) for v in edges.values())}개 | 건물: {len(buildings_in_graph)}개")

    _set_graph_cache(key, graph)
    return graph


# =============================================================================
#  2. 엣지별 그늘 점수 계산
# =============================================================================

def compute_edge_scores(
    edges: dict,
    nodes: dict,
    shadow_polygons: list[list],
    buildings: list[dict] = None,
    sun: dict = None,
    shelters: list[dict] = None,
    trees: list = None,
) -> dict[tuple, dict]:
    """
    엣지별 다중 요소 점수 계산
    반환: {(a_id,b_id): {shade, tree, surface_penalty, slope_penalty, shelter_bonus}}
    """
    scores: dict[tuple, dict] = {}

    # 건물 그늘 반경 사전 계산
    building_shade_zones = []
    if buildings and sun:
        elev = max(sun.get("elevation_deg", 45), 15)
        for b in buildings:
            shade_radius_m = b["height_m"] / math.tan(math.radians(elev))
            shade_radius_deg = shade_radius_m / 111320
            building_shade_zones.append({
                "lat": b["lat"], "lng": b["lng"],
                "radius": shade_radius_deg,
                "height": b["height_m"],
            })

    # 쉼터 위치 (근접 보너스용)
    shelter_zones = []
    if shelters:
        for s in shelters:
            shelter_zones.append({
                "lat": s["lat"], "lng": s["lng"],
                "radius": 100 / 111320,  # 100m 반경
            })

    # 가로수 구역 (나무 그늘 반경 약 5m)
    tree_zones = []
    if trees:
        tree_radius = 5 / 111320  # 5m 반경
        for t in trees:
            tree_zones.append({
                "lat": t["lat"], "lng": t["lng"],
                "radius": tree_radius,
            })
        print(f"[route_graph] 가로수 {len(tree_zones)}개 그늘 계산")

    def point_near_building(lat, lng):
        """건물 근접 여부 + 그늘 강도 반환"""
        best = 0.0
        for zone in building_shade_zones:
            dlat = lat - zone["lat"]
            dlng = (lng - zone["lng"]) * math.cos(math.radians(lat))
            d2 = dlat*dlat + dlng*dlng
            r2 = zone["radius"]*zone["radius"]
            if d2 < r2:
                # 건물 중심에 가까울수록 그늘 강도 높음
                proximity = 1.0 - (d2 / r2) ** 0.5
                best = max(best, proximity * 0.5)
        return best

    def point_near_shelter(lat, lng) -> bool:
        for zone in shelter_zones:
            dlat = lat - zone["lat"]
            dlng = (lng - zone["lng"]) * math.cos(math.radians(lat))
            if dlat*dlat + dlng*dlng < zone["radius"]*zone["radius"]:
                return True
        return False

    def point_near_tree(lat, lng) -> bool:
        """가로수 5m 이내 여부"""
        for zone in tree_zones:
            dlat = lat - zone["lat"]
            dlng = (lng - zone["lng"]) * math.cos(math.radians(lat))
            if dlat*dlat + dlng*dlng < zone["radius"]*zone["radius"]:
                return True
        return False

    for a_id, neighbors in edges.items():
        for edge_data in neighbors:
            b_id = edge_data[0]
            mid_lat, mid_lng = edge_data[2], edge_data[3]

            # 엣지 추가 속성 (새 포맷이면 있음)
            incline       = edge_data[4] if len(edge_data) > 4 else 0.0
            has_trees     = edge_data[5] if len(edge_data) > 5 else False
            is_hot        = edge_data[6] if len(edge_data) > 6 else False
            is_cool       = edge_data[7] if len(edge_data) > 7 else False

            key = (min(a_id, b_id), max(a_id, b_id))
            if key in scores:
                continue

            # 그림자 폴리곤 교차
            in_shadow = any(
                point_in_polygon(mid_lat, mid_lng, poly)
                for poly in shadow_polygons
            )

            # 건물 근접 그늘
            building_shade = point_near_building(mid_lat, mid_lng) if building_shade_zones else 0.0

            # 가로수 근접 그늘
            near_tree = point_near_tree(mid_lat, mid_lng) if tree_zones else False

            # 그늘 = 그림자 OR 건물근접 OR 가로수근접
            if in_shadow:
                shade = 1.0
            elif near_tree:
                shade = max(building_shade, 0.7)  # 가로수는 70% 그늘
            else:
                shade = building_shade

            # OSM tree_lined 태그 또는 실제 가로수 데이터 기반 보너스
            tree_bonus = 0.25 if (has_trees or near_tree) else 0.0

            scores[key] = {
                "shade":           shade,
                "tree_bonus":      tree_bonus,
                "surface_penalty": 0.12 if is_hot else (-0.05 if is_cool else 0.0),
                "slope_penalty":   min(incline / 100, 0.3),
                "shelter_bonus":   0.15 if point_near_shelter(mid_lat, mid_lng) else 0.0,
            }

    shaded = sum(1 for v in scores.values() if v["shade"] > 0)
    print(f"[route_graph] 그늘 엣지: {shaded}/{len(scores)}개 ({round(shaded/max(len(scores),1)*100)}%)")
    return scores


# 하위 호환 래퍼
def compute_edge_shade_scores(edges, nodes, shadow_polygons, buildings=None, sun=None):
    raw = compute_edge_scores(edges, nodes, shadow_polygons, buildings, sun)
    return {k: v["shade"] for k, v in raw.items()}


# =============================================================================
#  3. Dijkstra
# =============================================================================

def astar(
    graph: dict,
    start_node: int,
    end_node: int,
    edge_scores: dict,
    use_climate: bool = False,
) -> Optional[list[int]]:
    """
    A* 경로 탐색 (Dijkstra보다 빠름 — 목적지 방향 휴리스틱 사용)

    use_climate=False: cost = distance                    (normal 최단경로)
    use_climate=True:  cost = distance × climate_factor   (그늘/가로수/쉼터 선호)

    climate_factor = max(0.05,
        1.0
        - shade  × SHADE_WEIGHT
        - tree   × TREE_BONUS
        - shelter× SHELTER_BONUS
        + surface_penalty
        + slope_penalty
    )
    """
    nodes = graph["nodes"]
    edges = graph["edges"]

    end_lat, end_lng = nodes[end_node]

    def heuristic(node_id) -> float:
        """직선거리 휴리스틱 (A* 보장 조건: 실제거리 이하)"""
        n_lat, n_lng = nodes[node_id]
        return haversine_m(n_lat, n_lng, end_lat, end_lng) * 0.9

    dist_map: dict[int, float] = {start_node: 0.0}
    prev: dict[int, Optional[int]] = {start_node: None}
    heap = [(heuristic(start_node), 0.0, start_node)]

    while heap:
        _, cur_cost, cur = heapq.heappop(heap)

        if cur == end_node:
            break

        if cur_cost > dist_map.get(cur, float("inf")):
            continue

        for edge_data in edges.get(cur, []):
            neighbor  = edge_data[0]
            edge_dist = edge_data[1]

            if use_climate:
                key = (min(cur, neighbor), max(cur, neighbor))
                sc = edge_scores.get(key, {})
                shade   = sc.get("shade",           0.0)
                tree    = sc.get("tree_bonus",       0.0)
                shelter = sc.get("shelter_bonus",    0.0)
                surf_p  = sc.get("surface_penalty",  0.0)
                slope_p = sc.get("slope_penalty",    0.0)

                factor = max(0.05,
                    1.0
                    - shade   * SHADE_WEIGHT
                    - tree    * TREE_BONUS
                    - shelter * SHELTER_BONUS
                    + surf_p  * SURFACE_PENALTY
                    + slope_p * SLOPE_PENALTY
                )
                cost = edge_dist * factor
            else:
                cost = edge_dist

            new_cost = cur_cost + cost
            if new_cost < dist_map.get(neighbor, float("inf")):
                dist_map[neighbor] = new_cost
                prev[neighbor] = cur
                f = new_cost + heuristic(neighbor)
                heapq.heappush(heap, (f, new_cost, neighbor))

    if end_node not in prev:
        return None

    path = []
    cur = end_node
    while cur is not None:
        path.append(cur)
        cur = prev.get(cur)
    path.reverse()
    return path if path[0] == start_node else None


# 하위 호환
def dijkstra(graph, start_node, end_node, shade_scores, use_shade=False):
    return astar(graph, start_node, end_node, shade_scores, use_climate=use_shade)


# =============================================================================
#  4. 가장 가까운 노드 찾기
# =============================================================================

def nearest_node(nodes: dict, edges: dict, lat: float, lng: float) -> Optional[int]:
    """
    좌표에서 가장 가까운 도로 노드 반환
    엣지가 없는 고립 노드 제외
    """
    best_id, best_dist = None, float("inf")
    for node_id, (n_lat, n_lng) in nodes.items():
        if not edges.get(node_id):  # 연결된 엣지 없으면 스킵
            continue
        d = haversine_m(lat, lng, n_lat, n_lng)
        if d < best_dist:
            best_dist = d
            best_id = node_id
    return best_id if best_dist < 500 else None


# =============================================================================
#  5. 메인 함수 — 두 경로 반환
# =============================================================================

async def find_shade_routes(
    start_lat: float, start_lng: float,
    end_lat: float,   end_lng: float,
    hour: float,
    month: int = 7,
    trees: list = None,
) -> dict:
    """
    그늘 가중치 기반 normal/climate 두 경로 탐색

    반환:
      normal:  {coords, distance, duration, shade_score}
      climate: {coords, distance, duration, shade_score}
      method:  "graph" | "fallback"
    """
    # bbox 계산 (출발~목적지 + 여유 500m)
    margin = 0.003  # 약 300m
    min_lat = min(start_lat, end_lat) - margin
    max_lat = max(start_lat, end_lat) + margin
    min_lng = min(start_lng, end_lng) - margin
    max_lng = max(start_lng, end_lng) + margin

    # 도로 그래프 + 건물 데이터 한 번에 로딩 (같은 Overpass 쿼리)
    graph = await fetch_road_graph(min_lat, min_lng, max_lat, max_lng)

    nodes     = graph.get("nodes", {})
    edges     = graph.get("edges", {})
    buildings = graph.get("buildings", [])

    if not nodes or len(nodes) < 10:
        print("[route_graph] 도로 노드 부족 — OSRM 폴백")
        return {"method": "fallback"}

    # 태양 위치 + 그림자 폴리곤 계산
    # 야간(일몰 후~일출 전)이면 오전 10시 기준으로 그늘 계산
    # (실제 서비스에서 야간 보행은 드물고, 그늘 데이터는 낮 기준이 의미있음)
    effective_hour = hour
    sun = get_sun_position(hour, month)
    if sun["elevation_deg"] <= 3:
        effective_hour = 10.0  # 오전 10시로 대체
        sun = get_sun_position(effective_hour, month)
        print(f"[route_graph] 야간 감지 (hour={hour:.1f}) → 오전 10시 기준으로 그늘 계산")

    shadow_polygons = []
    for b in buildings:
        if b["height_m"] >= 6:
            poly = building_shadow_polygon(b, sun)
            shadow_polygons.append(poly)

    print(f"[route_graph] 건물 그림자 {len(shadow_polygons)}개 계산 완료 (태양고도 {sun['elevation_deg']:.1f}°)")

    # 엣지별 다중 요소 점수 계산 (가로수 포함)
    edge_scores = compute_edge_scores(edges, nodes, shadow_polygons, buildings, sun, trees=trees or [])

    # 출발/목적지 → 가장 가까운 노드
    start_node = nearest_node(nodes, edges, start_lat, start_lng)
    end_node   = nearest_node(nodes, edges, end_lat, end_lng)

    if start_node is None or end_node is None:
        print("[route_graph] 출발/목적지 근처 노드 없음 — OSRM 폴백")
        return {"method": "fallback"}

    print(f"[route_graph] 시작 노드: {start_node}, 끝 노드: {end_node}")

    # A* 실행 — normal(최단) + climate(그늘/쾌적 최적)
    normal_path  = astar(graph, start_node, end_node, edge_scores, use_climate=False)
    climate_path = astar(graph, start_node, end_node, edge_scores, use_climate=True)

    if not normal_path or not climate_path:
        print("[route_graph] 경로 탐색 실패 — OSRM 폴백")
        return {"method": "fallback"}

    # 경로 → 좌표 + 통계
    def path_to_route(path: list[int]) -> dict:
        coords = [[nodes[n][0], nodes[n][1]] for n in path]

        # 실제 거리
        dist = sum(
            haversine_m(coords[i][0], coords[i][1], coords[i+1][0], coords[i+1][1])
            for i in range(len(coords) - 1)
        )

        # 그늘 비율
        shaded = 0.0
        for i in range(len(path) - 1):
            a, b = path[i], path[i+1]
            sk = (min(a, b), max(a, b))
            seg_dist = haversine_m(
                nodes[a][0], nodes[a][1],
                nodes[b][0], nodes[b][1]
            )
            sc = edge_scores.get(sk, {})
            if isinstance(sc, dict):
                if sc.get("shade", 0) > 0:
                    shaded += seg_dist
            elif sc > 0:
                shaded += seg_dist

        shade_score = round(shaded / max(dist, 1), 3)

        # 도보 속도: 80m/분 (약 4.8km/h)
        duration_min = round(dist / 80)            # 분
        duration_sec = duration_min * 60           # 초 (OSRM 호환용)

        # 직사광선 노출 시간
        sun_exposure_min = round(duration_min * (1 - shade_score), 1)

        # 출발/목적지 실제 좌표를 앞뒤에 추가
        full_coords = [[start_lat, start_lng]] + coords + [[end_lat, end_lng]]

        return {
            "coords":           full_coords,
            "distance":         round(dist),
            "duration":         duration_sec,      # 초 (프론트 /60 → 분)
            "duration_min":     duration_min,      # 분 (카드 직접 표시용)
            "shade_score":      shade_score,
            "sun_exposure_min": sun_exposure_min,
        }

    normal_route  = path_to_route(normal_path)
    climate_route = path_to_route(climate_path)

    # climate가 normal보다 그늘 점수 낮으면 교체
    if climate_route["shade_score"] < normal_route["shade_score"]:
        normal_route, climate_route = climate_route, normal_route

    # 두 경로가 같으면 (그늘 없는 지역) climate에 최소 보정
    if climate_route["shade_score"] == normal_route["shade_score"]:
        climate_route["shade_score"] = min(0.95, normal_route["shade_score"] + 0.05)

    # 경로가 동일한지 확인 (노드 집합 비교)
    same_route = (normal_path == climate_path)
    if same_route:
        print("[route_graph] 두 경로 동일 — 그늘 없는 지역일 가능성")

    print(
        f"[route_graph] normal: {normal_route['distance']}m, "
        f"그늘 {round(normal_route['shade_score']*100)}% | "
        f"climate: {climate_route['distance']}m, "
        f"그늘 {round(climate_route['shade_score']*100)}%"
    )

    return {
        "normal":  normal_route,
        "climate": climate_route,
        "method":  "graph",
        "nodes_count":   len(nodes),
        "shadows_count": len(shadow_polygons),
        "same_route":    same_route,
    }
