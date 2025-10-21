import os
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
from io import StringIO
from time import sleep
from typing import Optional

def safe_replace_year(dt, new_year):
    try:
        return dt.replace(year=new_year)
    except ValueError:
        return dt.replace(year=new_year, day=28)
    
def expand_h2_prices_hourly(df_monthly):
    hourly_records = []
    for _, row in df_monthly.iterrows():
        year, month, price = int(row["year"]), int(row["month"]), row["h2_price"]
        start = datetime(year, month, 1,0)
        if month == 12:
            end = datetime(year + 1, 1, 1, 0)
        else:
            end = datetime(year, month + 1, 1, 0)
        current = start
        while current < end:
            hourly_records.append({
                "datetime": current,
                "h2_price": price,
                "year": year,
                "month": month
            })
            current += timedelta (hours=1)
    return pd.DataFrame(hourly_records)

def get_json_with_retries(session: requests.Session, url: str, params: dict, retries: int = 2, wait: float = 2) -> dict:
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
                print(f"⚠️ API error {r.status_code} für {url} mit params={params}")
                return None

            sleep(wait)

    raise RuntimeError(f"Unerwarteter Fehler in get_json_with_retries")
# Weather_Mapping

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

# Funktionen

def read_h2_prices(start_year, end_year):
    file_path = r"C:\RaD\GitHuB\Elektrolyseur_Optimierung\data\H2_prices_de.xlsx" 
    try:
        df = pd.read_excel(file_path, sheet_name="€_per_MWh")
    except FileNotFoundError:
        print(f"Datei {file_path} nicht gefunden.")
        return pd.DataFrame(columns=["year", "month", "h2_price"])
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["h2_price"] = pd.to_numeric(df["h2_price"], errors="coerce")
    df = df[(df["year"] >= start_year) & (df["year"] <= end_year)]
    return df.reset_index(drop=True)

def get_da_prices(file_path, start_year, end_year, weather_mapping):
    requested_years = list(range(start_year, end_year + 1))
    all_prices = []
    for opt_year in requested_years:
        weather_year = weather_mapping[opt_year]
        sheet_name = f"WY_{weather_year}"
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        df["UTC"] = pd.to_datetime(df["UTC"], errors="coerce").dt.round("h")
        df_year = df[df["UTC"].dt.year == opt_year].copy()
        df_year["datetime"] = df_year["UTC"].apply(lambda dt: safe_replace_year(dt, opt_year))
        df_year["year"] = opt_year
        df_year = df_year[["datetime", "DA_price", "year"]]
        all_prices.append(df_year)
    return pd.concat(all_prices).sort_values("datetime").reset_index(drop=True)

def get_ppa_data(token, start_year, end_year, lat, lon, mode="mix", mixes=None, tz="Europe/Berlin"):
    if mixes is None:
        mixes = list(range(0,101,10))
    session = requests.Session()
    session.headers = {"Authorization": f"Token {token}"}
    base = "https://www.renewables.ninja/api/"
    pv_url, wind_url = base + "data/pv", base + "data/wind"
    all_years = []
    for year in range(start_year, end_year + 1):
        pv_params = {"capacity": 20.0, "system_loss":0.1, "tracking":0, "tilt":18, "azim":180, "dataset":"merra2", "format":"json", "lat":lat, "lon":lon, "date_from":f"{year}-01-01", "date_to":f"{year}-12-31"}
        wind_params = {"capacity": 20.0, "height":100, "turbine":"Vestas V90 2000", "format":"json", "lat":lat, "lon":lon, "date_from":f"{year}-01-01", "date_to":f"{year}-12-31"}
        frames=[]
        pv_df = wind_df = None
        if mode in ("pv","mix"):
            pv_json = get_json_with_retries(session, pv_url, pv_params)
            pv_df = pd.read_json(StringIO(json.dumps(pv_json["data"])), orient="index")
            pv_df.index = pd.to_datetime(pv_df.index, utc=True).tz_convert(tz)
        if mode in ("wind","mix"):
            wind_json = get_json_with_retries(session, wind_url, wind_params)
            wind_df = pd.read_json(StringIO(json.dumps(wind_json["data"])), orient="index")
            wind_df.index = pd.to_datetime(wind_df.index, utc=True).tz_convert(tz)
        if mode=="pv":
            tmp=pd.DataFrame({"G_PPA_avail":pv_df["electricity"]})
            tmp["year"]=year; tmp["mix"]="pv100_wind0"; tmp["hour"]=tmp.index.hour; tmp["datetime"]=tmp.index
            frames.append(tmp[["year","hour","mix","datetime","G_PPA_avail"]])
        elif mode=="wind":
            tmp=pd.DataFrame({"G_PPA_avail":wind_df["electricity"]})
            tmp["year"]=year; tmp["mix"]="pv0_wind100"; tmp["hour"]=tmp.index.hour; tmp["datetime"]=tmp.index
            frames.append(tmp[["year","hour","mix","datetime","G_PPA_avail"]])
        else:
            for pv_pct in mixes:
                share_pv=pv_pct/100.0; share_wind=1-share_pv
                mix_series = pv_df["electricity"]*share_pv + wind_df["electricity"]*share_wind
                tmp=pd.DataFrame({"G_PPA_avail":mix_series})
                tmp["year"]=year; tmp["mix"]=f"pv{pv_pct}_wind{100-pv_pct}"; tmp["hour"]=tmp.index.hour; tmp["datetime"]=tmp.index
                frames.append(tmp[["year","hour","mix","datetime","G_PPA_avail"]])
        all_years.append(pd.concat(frames).reset_index(drop=True))
        sleep(0.3)
    result=pd.concat(all_years).reset_index(drop=True)
    result=result.sort_values("datetime").reset_index(drop=True)
    result=result.pivot_table(index=["year","hour","datetime"], columns="mix", values="G_PPA_avail").reset_index()
    result.columns.name=None
    return result

def merge_hourly_data(df_da, df_h2_hourly, df_ppa, ppa_mix="pv50_wind50"):
    df_ppa_sel = df_ppa[['datetime', ppa_mix]].rename(columns={ppa_mix:"G_PPA_avail"})
    df_merge = pd.merge(df_da, df_h2_hourly, on='datetime', how='outer')
    df_merge = pd.merge(df_merge, df_ppa_sel, on='datetime', how='outer')
    return df_merge.sort_values('datetime').reset_index(drop=True)

#Beispiel

# -------------------------
# Beispiel Nutzung
# -------------------------
if __name__=="__main__":
    start_year, end_year = 2030, 2030

    # === DA-Preise ===
    da_file = r"C:\RaD\GitHuB\Elektrolyseur_Optimierung\data\DA_prices_de.xlsx" 
    df_da = get_da_prices(da_file, start_year, end_year, WEATHER_MAPPING)

    # === H₂-Preise ===
    df_h2_monthly = read_h2_prices(start_year, end_year)
    df_h2_hourly = expand_h2_prices_hourly(df_h2_monthly)

    # === PPA-Profile ===
    TOKEN = "556c605e18c957326de4152532b694c483986f64"
    requested_years = list(range(start_year, end_year + 1))
    all_profiles = []

    for opt_year in requested_years:
        weather_year = WEATHER_MAPPING[opt_year]

        print(f"Lade Wetterjahr {weather_year} für Optimierungsjahr {opt_year}...")
        print(f"→ Hole Daten von Renewables.ninja für {weather_year} ...")


        df_weather = get_ppa_data(
            token=TOKEN,
            start_year=weather_year,
            end_year=weather_year,
            lat=52.52,
            lon=13.405,
            mode="pv",
            mixes=[0, 25, 50, 75, 100]
        )

        # Jahreszahlen auf Optimierungsjahr anpassen
        df_weather["year"] = opt_year
        df_weather["datetime"] = df_weather["datetime"].apply(lambda dt: safe_replace_year(dt, opt_year))
        all_profiles.append(df_weather)
    
    # Alle PPA-Daten zusammenführen
    df_ppa = pd.concat(all_profiles).reset_index(drop=True)

    # Merge aller Datenquellen
    df_all = merge_hourly_data(df_da, df_h2_hourly, df_ppa, ppa_mix="pv100_wind0")

    print("\n Daten erfolgreich geladen und zusammengeführt!")
    print(df_all.head())
    print(f"\nGesamtanzahl Stunden: {len(df_all)}")


        

    








