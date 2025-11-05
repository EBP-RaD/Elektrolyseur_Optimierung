
import os # Datei-/Pfadoperationen
import requests # HTTP-Requests zur API
import json #JSON-Parsing/Serialisierung
import pandas as pd # Dataframe-Verarbeitung

from io import StringIO #Wandelt json-strig in ein file-like object, das pd.read_json lesen kann
from time import sleep # Pause zwischen Retry-Versuchen (bei fehlerhaften API-Aufrufen)
from typing import Optional
from datetime import datetime
from Quellcode.get_data.ppa_config import PV_PARAMS, WIND_PARAMS

# Hilfsfunktion gegen Schaltjahrfehler

def safe_replace_year(dt, new_year):
    try:
        return dt.replace(year=new_year)
    except ValueError:
        # Falls 29.Februar --> 28. Februar setzen
        return dt.replace(year=new_year, day=28)

# 1. Wetterjahr-Zuordnung (fest definiert)
# Mapping zunächst im Code, später variabel durch Excel möglich

WEATHER_MAPPING = {
    2026: 2007,
    2027: 2014,
    2028: 2007,
    2029: 2008,
    2030: 2011,
    2031: 2017,
    2032: 2016,
    2033: 2014,
    2034: 2005,
    2035: 2016,
    2036: 2012,
    2037: 2016,
    2038: 2011,
    2039: 2005,
    2040: 2017,
    2041: 2008,
    2042: 2012,
    2043: 2011,
    2044: 2007,
    2045: 2017,
    2046: 2017,
    2047: 2017,
    2048: 2012,
    2049: 2017,
    2050: 2012,
}

# 2. Helper-Funktion: JSON mit Retry abrufen
# Funktion versucht JSON-Daten von einer API abzurufen, wiederholt fehlgeschlagene Requests automatisch bis
# ..zu retries-Mal und wirft einen Fehler, falls alles fehlschlägt
def get_json_with_retries(session: requests.Session, url: str, params: dict, retries: int = 2, wait: float = 2)-> dict:

    """
    Ruft JSON-Dateien von einer API ab und wiederholt fehlgeschlagene Requests.

    Args:
        session: requests.Session mit gesetztem Headern
        url: API-Endpunkt
        params: Query-Parameter für den Request
        retries: Anzahl Wiederholungen bei Fehler
        wait: Wartezeit (Sekunden) zwischen Versuchen
    """
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=30)
        except requests.RequestException as e:
            if attempt == retries:
                raise RuntimeError(f"RequestException nach {retries} Versuchen für {url}, params={params}: {e}")
            sleep(wait)
            continue

        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                raise RuntimeError(f"Ungültige JSOn von {url}, params={params}: {e}")
        else:
            if attempt == retries:
                raise RuntimeError(f"API error {r.status_code} für {url} mit params={params}")
            sleep(wait)

    # sollte nicht bis hierhin kommen
    raise RuntimeError("Unerwarteter Fehler in get_json_with_retries")

# Hauptfunktion: PPA-Daten abrufen
def get_ppa_data(
        token: str,                  
        start_year: int,              
        end_year: int,               
        lat: float,                    
        lon: float,                    
        mode: str = "mix",             
        mixes: Optional[list[int]] = None,            
        tz: str = "Europe/Berlin",
        flatten: bool = False,                      # if true --> return single time time series (datetime, P_PPA_avail)
        pv_share: Optional[int] = None              # Wenn flatten = True und mode=="mix"
) -> pd.DataFrame: # Ergebnis der Rückgabe ist ein DataFrame
    
    """
    Holt stündliche PPA-Erzeugungsdaten von renewable.ninja und liefere entweder:
        - vollständige Tabelle mit Mix-Varianten (default), oder
        - eine einzelne Zeitreihe (flatten=True) für den gewählten pv_share.

    Rückgabe (wenn flatten=False):
        DataFrame mit Spalten ["year", "hour", "mix", "datetime", "G_PPA_avail"]

    Rückgabe (wenn flatten=True)
        DataFrame mit Spalten ["datetime", "G_PPA_avail"] für das ausgewählte Profil

    """

    if mixes is None:
        mixes = list(range(0,101,10))

    if mode not in ("mix", "pv", "wind"):
        raise ValueError("mode must be one of 'mix', 'pv', 'wind'")
    
    # API-Setup
    session = requests.Session()
    session.headers = {"Authorization": f"Token {token}"}

    base = "https://www.renewables.ninja/api/"
    pv_url = base + "data/pv"
    wind_url = base + "data/wind"

    pv_params = PV_PARAMS.copy()
    wind_params = WIND_PARAMS.copy()

    all_years = []

    for year in range (start_year, end_year + 1):
        pv_args = pv_params | {
            "lat": lat, "lon":lon,
            "date_from": f"{year}-01-01", "date_to": f"{year}-12-31"
        }
        wind_args = wind_params | {
            "lat": lat, "lon":lon,
            "date_from": f"{year}-01-01", "date_to": f"{year}-12-31"
        }
    
        # Daten abrufen je nach Modus
        
        pv_df = wind_df = None
        
        # PV
        if mode in ("pv", "mix"):
            pv_json = get_json_with_retries(session, pv_url, pv_args)
            if "data" not in pv_json or not pv_json["data"]:
                raise RuntimeError(f"Keine PV-Daten für das Jahr {year}")
            pv_df = pd.read_json(StringIO(json.dumps(pv_json["data"])), orient="index")
            pv_df.index = pd.to_datetime(pv_df.index, utc=True)
            # Änderung für Zeitstempel
            
        
        # Wind
        if mode in ("wind", "mix"):
            wind_json = get_json_with_retries(session, wind_url, wind_args)
            if "data" not in wind_json or not wind_json["data"]:
                raise RuntimeError(f"Keine Wind-Daten für das Jahr {year}")
            wind_df = pd.read_json(StringIO(json.dumps(wind_json["data"])), orient="index")
            
            wind_df.index = pd.to_datetime(wind_df.index, utc=True)
            # Änderung für Zeitstempel
            

        frames= []

        if mode == "pv":
            tmp = pd.DataFrame({"G_PPA_avail": pv_df["electricity"]})
            tmp["year"] = year
            tmp["mix"] = "pv100_wind0"
            tmp["hour"] = tmp.index.hour
            tmp["datetime"] = tmp.index
            frames.append(tmp[["year", "hour", "mix", "datetime", "G_PPA_avail"]])

        elif mode == "wind":
            tmp = pd.DataFrame({"G_PPA_avail": wind_df["electricity"]})
            tmp["year"] = year
            tmp["mix"] = "pv0_wind100"
            tmp["hour"] = tmp.index.hour
            tmp["datetime"] = tmp.index
            frames.append(tmp[["year", "hour", "mix", "datetime", "G_PPA_avail"]])

        else: # mix
            for pv_pct in mixes:
                share_pv = pv_pct / 100.0
                share_wind = 1 - share_pv
                mix_series = pv_df["electricity"] * share_pv + wind_df["electricity"] * share_wind
                tmp = pd.DataFrame({"G_PPA_avail":mix_series})
                tmp["year"] = year
                tmp["mix"] = f"pv{pv_pct}_wind{100 - pv_pct}"
                tmp["hour"] = tmp.index.hour
                tmp["datetime"] = tmp.index
                frames.append(tmp[["year", "hour", "mix", "datetime", "G_PPA_avail"]])

        all_years.append(pd.concat(frames).reset_index(drop=True))
        sleep(0.3)
            
        
    result = pd.concat(all_years).reset_index(drop=True)
    result = result.sort_values("datetime").reset_index(drop=True)

    result = result.pivot_table(
        index=["datetime", "year", "hour"],
        columns="mix",
        values="G_PPA_avail"
    ).reset_index()
    result.columns.name = None # Spaltenname entfernen

    result.loc[:, result.columns.str.startswith("pv") | result.columns.str.startswith("wind")] /= 1000.0
    return result

# 4. Beispiel: Matching Optimierungsjahr -> Wetterjahr
if __name__ == "__main__":
    TOKEN = "556c605e18c957326de4152532b694c483986f64"

    # Optimierungsjahr für Test
    requested_years = [2030]

    all_profiles = []

    # Mix-Anteile definieren (0, 25, 50, 75, 100)
    mixes = [0, 25, 50, 75, 100]

    for opt_year in requested_years:
        weather_year = WEATHER_MAPPING[opt_year]

        # PPA-Daten abrufen, Wide-Form automatisch
        df_weather = get_ppa_data(
            token=TOKEN,
            start_year=weather_year,
            end_year=weather_year,
            lat=52.52,
            lon=13.405,
            mode="wind",
            mixes=mixes
        )

        # Jahreszahlen auf Optimierungsjahr ändern
        df_weather["year"] = opt_year
        df_weather["datetime"] = df_weather["datetime"].apply(lambda dt: safe_replace_year(dt, opt_year))

        all_profiles.append(df_weather)
        print(f"Wetterjahr {weather_year} → Optimierungsjahr {opt_year}")

    df_all = pd.concat(all_profiles).reset_index(drop=True)

    # Ausgabe der ersten 5 Zeilen
    print(df_all.head(10))

