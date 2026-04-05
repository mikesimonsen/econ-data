from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def load(path: Path = CONFIG_PATH) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def all_series(cfg: dict) -> list:
    """Return all (series_id, name) pairs across standalone series and groups."""
    result = [(s["id"], s["name"]) for s in cfg.get("series", [])]
    for group in cfg.get("groups", {}).values():
        result.extend((s["id"], s["name"]) for s in group["series"])
    return result


def percent_series(cfg: dict) -> set:
    """Return set of series IDs that have units: percent."""
    result = set()
    for s in cfg.get("series", []):
        if s.get("units") == "percent":
            result.add(s["id"])
    for group in cfg.get("groups", {}).values():
        for s in group["series"]:
            if s.get("units") == "percent":
                result.add(s["id"])
    return result


def inverted_series(cfg: dict) -> set:
    """Return set of series IDs where rising = economic weakness."""
    result = set()
    for s in cfg.get("series", []):
        if s.get("inverted"):
            result.add(s["id"])
    for group in cfg.get("groups", {}).values():
        for s in group["series"]:
            if s.get("inverted"):
                result.add(s["id"])
    return result


def minimal_signal_series(cfg: dict) -> set:
    """Return set of series IDs with signals: minimal.

    These series suppress YoY-based signals and unusual-move detection,
    keeping only beat/miss, trend reversal, and directional trend.
    """
    result = set()
    for group in cfg.get("groups", {}).values():
        if group.get("signals") == "minimal":
            for s in group["series"]:
                result.add(s["id"])
    for s in cfg.get("series", []):
        if s.get("signals") == "minimal":
            result.add(s["id"])
    return result


def seasonal_series(cfg: dict) -> set:
    """Return set of series IDs that need seasonal adjustment."""
    result = set()
    for group in cfg.get("groups", {}).values():
        if group.get("seasonal"):
            for s in group["series"]:
                result.add(s["id"])
    for s in cfg.get("series", []):
        if s.get("seasonal"):
            result.add(s["id"])
    return result


def fred_series(cfg: dict) -> list:
    """Return (series_id, name) pairs for FRED-sourced series only."""
    result = [(s["id"], s["name"]) for s in cfg.get("series", [])
              if not s.get("bls_id")]
    for group in cfg.get("groups", {}).values():
        if group.get("source"):
            continue  # skip non-FRED groups (e.g. source: mnd)
        result.extend((s["id"], s["name"]) for s in group["series"]
                       if not s.get("bls_id"))
    return result
