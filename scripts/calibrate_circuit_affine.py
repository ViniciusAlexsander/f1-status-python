"""Fit F1 Position X/Y (tenths of a metre) onto GeoJSON lon/lat via Umeyama."""

from __future__ import annotations

import json
import math
import urllib.request
from pathlib import Path

GEOJSON_DIR = (
    Path(__file__).resolve().parents[1].parent
    / "f1-status-frontend"
    / "src"
    / "assets"
    / "circuits"
)
OUTPUT = (
    Path(__file__).resolve().parents[1].parent
    / "f1-status-frontend"
    / "src"
    / "config"
    / "circuitAffines.json"
)

# 2025 races on OpenF1, used only to calibrate the overlay.
SESSIONS: dict[str, tuple[int, str]] = {
    "au-1953": (9693, "2025-03-16T04:25:00+00:00"),
    "cn-2004": (9998, "2025-03-23T07:25:00+00:00"),
    "jp-1962": (10006, "2025-04-06T05:25:00+00:00"),
    "bh-2002": (10014, "2025-04-13T15:25:00+00:00"),
    "sa-2021": (10022, "2025-04-20T17:25:00+00:00"),
    "us-2022": (10033, "2025-05-04T20:25:00+00:00"),
    "mc-1929": (9979, "2025-05-25T13:25:00+00:00"),
    "es-1991": (9971, "2025-06-01T13:25:00+00:00"),
    "ca-1978": (9963, "2025-06-15T18:25:00+00:00"),
    "at-1969": (9955, "2025-06-29T13:25:00+00:00"),
    "gb-1948": (9947, "2025-07-06T14:25:00+00:00"),
    "be-1925": (9939, "2025-07-27T13:25:00+00:00"),
    "hu-1986": (9928, "2025-08-03T13:25:00+00:00"),
    "nl-1948": (9920, "2025-08-31T13:25:00+00:00"),
    "it-1922": (9912, "2025-09-07T13:25:00+00:00"),
    "az-2016": (9904, "2025-09-21T11:25:00+00:00"),
    "sg-2008": (9896, "2025-10-05T12:25:00+00:00"),
    "us-2012": (9888, "2025-10-19T19:25:00+00:00"),
    "mx-1962": (9877, "2025-10-26T20:25:00+00:00"),
    "br-1940": (9869, "2025-11-09T17:25:00+00:00"),
    "us-2023": (9858, "2025-11-23T04:25:00+00:00"),
    "qa-2004": (9850, "2025-11-30T16:25:00+00:00"),
    "ae-2009": (9839, "2025-12-07T13:25:00+00:00"),
}


def fetch_json(url: str) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": "f1-status-calibrate"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def geojson_line(circuit_id: str) -> list[tuple[float, float]]:
    data = json.loads((GEOJSON_DIR / f"{circuit_id}.geojson").read_text(encoding="utf-8"))
    coords = data["features"][0]["geometry"]["coordinates"]
    return [(float(lon), float(lat)) for lon, lat in coords]


def resample(points: list[tuple[float, float]], count: int) -> list[tuple[float, float]]:
    if len(points) < 2:
        return points
    distances = [0.0]
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        distances.append(distances[-1] + math.hypot(dx, dy))
    total = distances[-1]
    if total == 0:
        return points[:count]
    samples: list[tuple[float, float]] = []
    j = 0
    for i in range(count):
        target = total * i / (count - 1)
        while j + 1 < len(distances) and distances[j + 1] < target:
            j += 1
        if j + 1 >= len(points):
            samples.append(points[-1])
            continue
        span = distances[j + 1] - distances[j]
        t = 0.0 if span == 0 else (target - distances[j]) / span
        x = points[j][0] + t * (points[j + 1][0] - points[j][0])
        y = points[j][1] + t * (points[j + 1][1] - points[j][1])
        samples.append((x, y))
    return samples


def umeyama(
    source: list[tuple[float, float]],
    target: list[tuple[float, float]],
) -> dict[str, float]:
    n = min(len(source), len(target))
    src = source[:n]
    dst = target[:n]
    mx = sum(p[0] for p in src) / n
    my = sum(p[1] for p in src) / n
    nx = sum(p[0] for p in dst) / n
    ny = sum(p[1] for p in dst) / n
    var = sum((p[0] - mx) ** 2 + (p[1] - my) ** 2 for p in src) / n
    cov_xx = cov_xy = cov_yx = cov_yy = 0.0
    for (sx, sy), (tx, ty) in zip(src, dst):
        cov_xx += (sx - mx) * (tx - nx)
        cov_xy += (sx - mx) * (ty - ny)
        cov_yx += (sy - my) * (tx - nx)
        cov_yy += (sy - my) * (ty - ny)
    cov_xx /= n
    cov_xy /= n
    cov_yx /= n
    cov_yy /= n

    # SVD of 2x2 covariance via Jacobi-style closed form
    a = cov_xx
    b = cov_xy
    c = cov_yx
    d = cov_yy
    # A = U S V^T ; compute via eigen of A^T A
    ata00 = a * a + c * c
    ata01 = a * b + c * d
    ata11 = b * b + d * d
    trace = ata00 + ata11
    det = ata00 * ata11 - ata01 * ata01
    gap = math.sqrt(max(trace * trace / 4 - det, 0.0))
    s0 = math.sqrt(max(trace / 2 + gap, 0.0))
    s1 = math.sqrt(max(trace / 2 - gap, 0.0))

    def eigenvec(lam: float) -> tuple[float, float]:
        if abs(ata01) > 1e-12:
            vx, vy = lam - ata11, ata01
        elif abs(ata00 - lam) <= abs(ata11 - lam):
            vx, vy = 1.0, 0.0
        else:
            vx, vy = 0.0, 1.0
        norm = math.hypot(vx, vy) or 1.0
        return vx / norm, vy / norm

    v0x, v0y = eigenvec(s0 * s0)
    v1x, v1y = -v0y, v0x
    # U columns = A V / s
    def u_col(vx: float, vy: float, s: float) -> tuple[float, float]:
        ux = a * vx + b * vy
        uy = c * vx + d * vy
        if s > 1e-12:
            ux /= s
            uy /= s
        norm = math.hypot(ux, uy) or 1.0
        return ux / norm, uy / norm

    u0x, u0y = u_col(v0x, v0y, s0)
    u1x, u1y = u_col(v1x, v1y, s1)
    det_uv = u0x * u1y - u0y * u1x
    det_v = v0x * v1y - v0y * v1x
    if det_uv * det_v < 0:
        u1x, u1y = -u1x, -u1y
        s1 = -s1
    # R = U S_sign V^T
    r00 = u0x * v0x + u1x * v1x
    r01 = u0x * v0y + u1x * v1y
    r10 = u0y * v0x + u1y * v1x
    r11 = u0y * v0y + u1y * v1y
    scale = (s0 + abs(s1)) / var if var > 1e-12 else 1.0
    a_out = scale * r00
    b_out = scale * r01
    c_out = scale * r10
    d_out = scale * r11
    tx = nx - (a_out * mx + b_out * my)
    ty = ny - (c_out * mx + d_out * my)
    return {
        "a": a_out,
        "b": b_out,
        "c": c_out,
        "d": d_out,
        "tx": tx,
        "ty": ty,
    }


def fetch_f1_points(session_key: int, date_start: str) -> list[tuple[float, float]]:
    from datetime import datetime, timedelta, timezone

    start = datetime.fromisoformat(date_start.replace("Z", "+00:00"))
    end = start + timedelta(minutes=3)
    date_end = end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    points: list[tuple[float, float]] = []
    for driver in (1, 81, 16, 4):
        url = (
            "https://api.openf1.org/v1/location"
            f"?session_key={session_key}&driver_number={driver}"
            f"&date>={date_start}&date<={date_end}"
        )
        rows = fetch_json(url)
        if not isinstance(rows, list):
            continue
        for row in rows:
            x = row.get("x")
            y = row.get("y")
            if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                if x == 0 and y == 0:
                    continue
                points.append((float(x), float(y)))
        if len(points) >= 80:
            break
        points = []
    return points[:800]


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    affines: dict[str, dict[str, float]] = {}
    for circuit_id, (session_key, date_start) in SESSIONS.items():
        try:
            f1_points = fetch_f1_points(session_key, date_start)
            geo = geojson_line(circuit_id)
            if len(f1_points) < 50 or len(geo) < 10:
                print(f"skip {circuit_id}: points={len(f1_points)} geo={len(geo)}")
                continue
            src = resample(f1_points, 200)
            dst = resample(geo, 200)
            # Try both orientations of the loop
            candidates = [umeyama(src, dst), umeyama(src, list(reversed(dst)))]
            best = min(candidates, key=lambda t: residual(src, dst if t is candidates[0] else list(reversed(dst)), t))
            # residual helper uses matching dst; recompute properly:
            err0 = residual(src, dst, candidates[0])
            err1 = residual(src, list(reversed(dst)), candidates[1])
            best = candidates[0] if err0 <= err1 else candidates[1]
            affines[circuit_id] = best
            print(f"ok {circuit_id} err={min(err0, err1):.6f} n={len(f1_points)}")
        except Exception as exc:
            print(f"fail {circuit_id}: {exc}")
    OUTPUT.write_text(json.dumps(affines, indent=2), encoding="utf-8")
    print(f"wrote {OUTPUT} ({len(affines)} circuits)")


def residual(
    src: list[tuple[float, float]],
    dst: list[tuple[float, float]],
    transform: dict[str, float],
) -> float:
    error = 0.0
    for (x, y), (lon, lat) in zip(src, dst):
        px = transform["a"] * x + transform["b"] * y + transform["tx"]
        py = transform["c"] * x + transform["d"] * y + transform["ty"]
        error += (px - lon) ** 2 + (py - lat) ** 2
    return error / max(len(src), 1)


if __name__ == "__main__":
    main()
