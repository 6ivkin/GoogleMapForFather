from __future__ import annotations

import argparse
import json
import textwrap
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from branca.element import MacroElement, Template

import pandas as pd
import requests
import folium
import hashlib

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
YANDEX_API_KEY = "be475cb4-744a-4095-bbbb-61246c795b3d"
DEFAULT_API_KEY = YANDEX_API_KEY
DEFAULT_EXCEL = "data/map.xlsx"
DEFAULT_CACHE = "cache_geocode.json"
DEFAULT_HTML = "index.html"
DEFAULT_SHEET: str | int = 0
DEFAULT_REGIONS = ["Саратовская", "Пензенская"]

POPUP_TEMPLATE = textwrap.dedent(
    """
    <b>Наименование:</b> {Наименование}<br>
    <b>Населенный пункт:</b> {Населенный пункт}<br>
    <b>Адрес:</b> {Адрес}<br>
    <b>Филиал:</b> {Филиал}<br>
    <b>Наличие аптеки на ТТ:</b> {Наличие аптеки на ТТ}<br>
    <b>Инженер по эксплуатации:</b> {Инженер по эксплуатации}<br>
    <b>Инженер по ХиТО:</b> {Инженер по ХиТО}<br>
    <b>Инженер-энергетик:</b> {Инженер-энергетик}<br>
    <b>Инженер-теплотехник:</b> {Инженер-теплотехник}<br>
    <b>Механик КТО:</b> {Механик КТО}<br>
    <b>Механик ХО:</b> {Механик ХО}<br>
    <b>Электрик:</b> {Электрик}<br>
    """
).strip()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_cache(path: Path) -> Dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text("utf-8"))
        except json.JSONDecodeError:
            print("[warn] broken cache, start new")
    return {}


def save_cache(cache: Dict[str, Any], path: Path) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(path)


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    def clean(c: str) -> str:
        c = c.replace("-", "-")
        c = " ".join(c.replace("\u00A0", " ").split())
        return c.strip()

    return df.rename(columns={c: clean(c) for c in df.columns})


def add_mechanics_legend(fmap, mechanic_colors):
    legend_html = """
    {% macro html(this, kwargs) %}
    <div style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        z-index:9999;
        background: white;
        padding: 12px 18px;
        border-radius: 12px;
        border: 1px solid #bbb;
        font-size: 14px;">
    <b>Механики КТО:</b><br>
    """ + "".join(
        f'<div style="margin-bottom:4px;"><span style="color:{color};font-size:18px;">●</span> {name}</div>'
        for name, color in mechanic_colors.items() if name.strip()
    ) + """
    </div>
    {% endmacro %}
    """
    macro = MacroElement()
    macro._template = Template(legend_html)
    fmap.get_root().add_child(macro)


def get_color(name: str) -> str:
    # Цвет по хэшу строки — всегда одинаковый для одного механика
    palette = [
        "red", "blue", "green", "orange", "purple", "darkred", "lightred",
        "beige", "darkblue", "darkgreen", "cadetblue", "darkpurple", "white",
        "pink", "lightblue", "lightgreen", "gray", "black", "lightgray"
    ]
    # Преобразуем имя в индекс палитры
    idx = int(hashlib.md5(name.encode()).hexdigest(), 16) % len(palette)
    return palette[idx]


def normalize_address(addr: str) -> str:
    # Нормализация адресных сокращений
    return (
        addr.replace('д.', 'дом')
        .replace('д ', 'дом ')
        .replace('ул.', 'улица')
        .replace('ул ', 'улица ')
        .replace('пр-т', 'проспект')
        .replace('пр.', 'проезд')
        .replace('пер.', 'переулок')
        .replace('пл.', 'площадь')
        .replace('г.', '')
        .replace(' г ', ' ')
        .replace(',', ' ')
        .replace('  ', ' ')
        .strip()
    )


# ---------------------------------------------------------------------------
# Yandex Geocoding
# ---------------------------------------------------------------------------

def geocode(addr: str, place: str, key: str, regions: List[str]) -> Optional[Dict[str, Any]]:
    url = "https://geocode-maps.yandex.ru/1.x/"
    # Пробуем скомбинировать адрес и населенный пункт
    address_variants = []
    addr_norm = normalize_address(addr)
    if place and place not in addr_norm:
        address_variants.append(f"{place}, {addr_norm}")
    address_variants.append(addr_norm)
    address_variants.append(addr)  # вдруг оригинал прокатит

    # Разные шаблоны с регионом и страной
    tries = []
    for base in address_variants:
        for region in regions + ["Россия"]:
            tries.append(f"{base}, {region} область")
            tries.append(f"{base}, {region}")
        tries.append(base)

    tried = set()
    for q in tries:
        q = q.strip()
        if not q or q in tried:
            continue
        tried.add(q)
        params = {
            "apikey": key,
            "geocode": q,
            "format": "json",
            "lang": "ru_RU"
        }
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            resp = r.json()
            features = resp["response"]["GeoObjectCollection"]["featureMember"]
            if not features:
                continue
            obj = features[0]["GeoObject"]
            pos = obj["Point"]["pos"].split()
            full_name = obj["metaDataProperty"]["GeocoderMetaData"]["text"]
            # Можно ослабить фильтр, чтобы максимум точек получить:
            # if not any(r.lower() in full_name.lower() for r in regions):
            #     continue
            return {
                "lat": float(pos[1]),
                "lon": float(pos[0]),
                "full_name": full_name
            }
        except Exception as e:
            print(f"[err] Yandex Geocoder: {e} [{q}]")
    return None


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

def make_popup(row: pd.Series) -> folium.Popup:
    return folium.Popup(POPUP_TEMPLATE.format_map(defaultdict(str, row.to_dict())), max_width=450)


def build_map(df: pd.DataFrame, *, key: str, regions: List[str], cache: Dict[str, Any], flush: bool) -> Tuple[
    folium.Map, List[str]]:
    df = normalize_cols(df)
    misses: List[str] = []
    marks: List[Tuple[float, float, pd.Series]] = []

    # Собрать всех механиков для легенды
    kto_to_color = {}
    for _, row in df.iterrows():
        kto = str(row.get("Механик КТО", "")).strip()
        if kto and kto not in kto_to_color:
            kto_to_color[kto] = get_color(kto or "default")

    for _, row in df.iterrows():
        addr = str(row.get("Адрес", "")).strip()
        place = str(row.get("Населенный пункт", "")).strip()
        if not addr:
            misses.append("<пустой адрес>")
            continue

        cached = cache.get(addr)
        if flush and cached is None:
            cached = None  # force retry

        resolved = cached if cached else geocode(addr, place, key, regions)
        cache[addr] = resolved  # may be None

        if resolved is None:
            misses.append(addr if not place else f"{place}, {addr}")
            continue
        marks.append((resolved["lat"], resolved["lon"], row))

        print(f"Found {len(marks)} valid points, {len(misses)} misses")

    if not marks:
        raise RuntimeError("Nothing resolved – check API key or regions")

    avg_lat = sum(m[0] for m in marks) / len(marks)
    avg_lon = sum(m[1] for m in marks) / len(marks)
    fmap = folium.Map([avg_lat, avg_lon], zoom_start=9)
    for lat, lon, row in marks:
        kto = str(row.get("Механик КТО", "")).strip()
        color = kto_to_color.get(kto or "default", "blue")
        folium.Marker(
            [lat, lon],
            popup=make_popup(row),
            icon=folium.Icon(color=color)
        ).add_to(fmap)

    # Добавить легенду на карту!
    add_mechanics_legend(fmap, kto_to_color)
    return fmap, misses


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse() -> argparse.Namespace:
    p = argparse.ArgumentParser("Excel → Yandex Map (Saratov & Penza)")
    p.add_argument("--excel", default=DEFAULT_EXCEL)
    p.add_argument("--sheet", default=DEFAULT_SHEET)
    p.add_argument("--cache", default=DEFAULT_CACHE)
    p.add_argument("--output", default=DEFAULT_HTML)
    p.add_argument("--key", default=DEFAULT_API_KEY)
    p.add_argument("--regions", default=",".join(DEFAULT_REGIONS))
    p.add_argument("--flush", action="store_true", help="requery addresses cached as None")
    return p.parse_args()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse()
    regions = [r.strip() for r in args.regions.split(",") if r.strip()]

    df = pd.read_excel(args.excel, sheet_name=args.sheet)
    cache_file = Path(args.cache)
    cache = load_cache(cache_file)

    try:
        fmap, bad = build_map(df, key=args.key, regions=regions, cache=cache, flush=args.flush)
    except RuntimeError as e:
        print("[err]", e)
        return

    save_cache(cache, cache_file)
    fmap.save(args.output)

    if bad:
        print(f"\n⚠ Не распознаны ({len(bad)}):")
        for a in bad:
            print("  •", a)
        # сохраняем для ручной правки
        pd.DataFrame({"Адрес": bad}).to_excel("missed_addresses.xlsx", index=False)
        print("Сохранил список нераспознанных в missed_addresses.xlsx")
    else:
        print("Все адреса найдены!")
    print(f"✔ Карту сохранил в '{args.output}'.")


if __name__ == "__main__":
    main()
