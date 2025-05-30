import json
import re
from pathlib import Path

import folium
import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import ArcGIS, Nominatim

ABBR_REPLACE = {
    r"\bг\.\s*": "",  # убираем «г.»
    r"\bрп\s+": "",  # «рп» (раб. посёлок)
    r"\bр[- ]?н\b\.?": "",  # «р-н»
    r"\bобл\.?": "",  # «обл»
    r"\bд\.\s*": "",  # «д.»
    r"\bдом\s*№?": "",  # «дом»
    r"\bул\.?": "улица",  # «ул.» → «улица»
    r"\bпр-т\.?": "проспект",  # «пр-т»
    r"\bпр-кт\.?": "проспект",
    r"\bпл\.?": "площадь",  # «пл.»
}

POST_CODE = re.compile(r"^\s*\d{6},?\s*")

STOP_WORDS = (
    "район",
    "р-н",
    "рн",
    "область",
    "обл",
    "Респ.",
    "респ",
    "посёлок",
    "п.",
    "рабочий посёлок",
    "рабочий",
    "поселок",
    "здание",
    "строение",
    "корпус",
    "к.",
    "литера",
    "лит.",
    "им.",
    "академика",
    "имени",
)

def try_progressive(geocoders, query: str):
    """
    Возвращает (lat, lon, level):
        level = "house" | "street" | "city" | None
    """
    # 1) полный адрес (дом)
    for g in geocoders:
        if (loc := g(query, exactly_one=True)):
            return loc.latitude, loc.longitude, "house"

    # 2) без номера дома (улица)
    no_house = re.sub(r"\s\d+[А-Яа-яA-Za-z/-]*\s*$", "", query)
    if no_house != query:
        for g in geocoders:
            if (loc := g(no_house, exactly_one=True)):
                return loc.latitude, loc.longitude, "street"

    # 3) только город
    parts = query.split(",")
    if len(parts) > 2:
        city_only = parts[1].strip()
    else:
        return None, None, None
    for g in geocoders:
        if (loc := g(f"Россия, {city_only}", exactly_one=True)):
            return loc.latitude, loc.longitude, "city"

    return None, None, None

def strip_stopwords(addr: str) -> str:
    """Удаляет всё после первого стоп-слова, чтобы оставить «Город, улица, дом»."""
    parts = [p.strip() for p in re.split(r",", addr)]
    clean = []
    for p in parts:
        if any(sw.lower() in p.lower() for sw in STOP_WORDS):
            break
        clean.append(p)
    return ", ".join(clean)


def normalize(city: str, raw: str) -> str:
    """Возвращает строку формата 'Россия, <город>, <очищенный адрес>'"""
    addr = POST_CODE.sub("", raw)  # убираем индекс
    for pattern, repl in ABBR_REPLACE.items():  # заменяем сокращения
        addr = re.sub(pattern, repl, addr, flags=re.IGNORECASE)
    addr = re.sub(r"\s{2,}", " ", addr)  # лишние пробелы
    addr = addr.strip(", ")
    addr = strip_stopwords(addr)
    return f"Россия, {city}, {addr}"


# ---------- Кэш ----------
class GeocodeCache:
    def __init__(self, path: str = "geocode_cache.json"):
        self.path = Path(path)
        self.data: dict[str, tuple[float, float] | None] = (
            json.loads(self.path.read_text("utf-8")) if self.path.exists() else {}
        )

    def get(self, key: str):
        return self.data.get(key)

    def set(self, key: str, coords):
        self.data[key] = coords
        self.path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2), "utf-8"
        )


# ---------- Геокодер ----------
class GeoCoder:
    def __init__(self, cache: GeocodeCache):
        self.cache = cache
        self.nom = RateLimiter(
            Nominatim(user_agent="map_app", timeout=10).geocode,
            min_delay_seconds=2.0,
            max_retries=3,
            error_wait_seconds=7,
        )
        self.arc = RateLimiter(
            ArcGIS(timeout=10).geocode,
            min_delay_seconds=2.0,
            max_retries=3,
            error_wait_seconds=7,
        )

    def get(self, city: str, raw_addr: str):
        query = normalize(city, raw_addr)

        if (cached := self.cache.get(query)) is not None:
            return cached

        lat, lon, lvl = try_progressive((self.nom, self.arc), query)
        res = (lat, lon, lvl) if lvl == "house" else None  # сохраняем в кэш только «дом»
        self.cache.set(query, res)
        return res


# ---------- Загрузка Excel ----------
class ExcelDataLoader:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def load(self):
        return pd.read_excel(self.file_path)


# ---------- Генератор карты ----------
class MapGenerator:
    def __init__(self, center_lat: float, center_lon: float, zoom: int = 11):
        self._map = folium.Map(location=[center_lat, center_lon], zoom_start=zoom)

    def add_marker(self, lat: float, lon: float, tooltip: str, popup_info: dict):
        html = "<br>".join(f"<b>{k}:</b> {v}" for k, v in popup_info.items())
        folium.Marker([lat, lon], tooltip=tooltip, popup=html).add_to(self._map)

    def save(self, path: str = "index.html"):
        self._map.save(path)


# ---------- Склейка ----------
def main():
    data = ExcelDataLoader("map.xlsx").load()
    data.columns = data.columns.str.strip().str.replace(r"\s+", " ", regex=True)
    cache = GeocodeCache()
    geocoder = GeoCoder(cache)
    m = MapGenerator(51.533557, 46.034257)  # центр Саратова
    
    not_found_rows = []

    total = found = 0
    for _, row in data.iterrows():
        total += 1
        coords = geocoder.get(row["Населенный пункт"], row["Адрес"])
        
        if coords is None:
            not_found_rows.append({
                "№ п/п": row.get("№ п/п", ""),
                "Адрес": row["Адрес"],
                "Насел. пункт": row["Населенный пункт"],
            })
            continue

        found += 1
        lat, lon, _ = coords

        popup_info = {
            "Филиал": row.get("Филиал", ""),
            "Аптека": row.get("Наличие аптеки на ТТ", "Нет"),
            "Инж. эксплуатации": row.get("Инженер по эксплуатации", ""),
            "Инж. ХиТО": row.get("Инженер по ХиТО", ""),
            "Инж.-энергетик": row.get("Инженер-энергетик", ""),
            "Инж.-теплотехник": row.get("Инженер-теплотехник", ""),
            "Механик КТО": row.get("Механик  КТО", ""),
            "Механик ХО": row.get("Механик ХО", ""),
            "Электрик": row.get("Электрик", ""),
        }
        m.add_marker(lat, lon, tooltip=row["Наименование"], popup_info=popup_info)

    m.save()

    if not_found_rows:
        pd.DataFrame(not_found_rows).to_excel("not_found.xlsx", index=False)
        print(f"⚠️  Не найден точный дом для {len(not_found_rows)} записей. "f"Список → not_found.xlsx")
    else:
        print("✅ Все записи имеют точный адрес.")

    print(f"Маркеров на карте: {found}/{total}")


if __name__ == "__main__":
    main()
