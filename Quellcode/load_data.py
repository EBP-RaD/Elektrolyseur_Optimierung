import pandas as pd

from get_data.da_prices import get_da_prices, WEATHER_MAPPING
from get_data.h2_prices import read_h2_prices, expand_h2_prices_hourly
from get_data.ppa_profiles import get_ppa_data

def load_all_data(start_year, end_year, da_file, h2_file, ppa_token, ppa_lat, ppa_lon, ppa_mode, tz="UTC"):
    """
    Lädt DA-Preise, H2-Preise und PPA-Profile für einen einzelnen PPA-Mix und führt sie stündlich zusammen.
    Args:
        start_year, end_year: Optimierungsjahre
        da_file: Pfad zu Excel-Datei DA-Preise
        h2_file: Pfad zu Excel-Datei H2-Preise
        ppa_token: API-Token
        ppa_lat, ppa_lon: Standort für PPA
        ppa_mode: 'wind' oder 'pv' - wählt, welche Erzeugung genutzt wird
        tz: Zeitzone

    Returns:
        pd.DataFrame: Stündliche Daten mit Spalten ["datetime", "DA_price", "h2_price", "G_PPA_avail", "year"]    
    """

    # 1. Day-Ahead-Preise laden 
    df_da = get_da_prices(da_file, start_year, end_year, WEATHER_MAPPING)
    # Entferne potenziell fehlerhafte Zeilen aus der Excel
    df_da.dropna(subset=['datetime'], inplace=True)
    df_da["datetime"] = pd.to_datetime(df_da["datetime"], utc=True)

    # 2. H2-Preise laden 
    df_h2_monthly = read_h2_prices(h2_file, start_year, end_year)
    df_h2 = expand_h2_prices_hourly(df_h2_monthly, tz_name=tz)
    df_h2["datetime"] = pd.to_datetime(df_h2["datetime"], utc=True)

    # 3. PPA-Profile laden und für die Optimierungsjahre anpassen
    all_ppa = []
    for opt_year in range(start_year, end_year+1):
        weather_year = WEATHER_MAPPING[opt_year]

        df_ppa_weather = get_ppa_data(
            token=ppa_token,
            start_year=weather_year,
            end_year=weather_year,
            lat=ppa_lat,
            lon=ppa_lon,
            mode=ppa_mode,
            mixes=None, # erstmal kein Mix, nur pv0_wind100 oder pv100_wind0
            tz=tz,
            flatten=True # nur eine Spalte G_PPA_avail
        )
        # Jahreszahlen auf Optimierungsjahr ändern
        
        df_ppa_weather["datetime"] = df_ppa_weather["datetime"].apply(lambda dt: dt.replace(year=opt_year))
        df_ppa_weather["datetime"] = pd.to_datetime(df_ppa_weather["datetime"], utc=True)

        all_ppa.append(df_ppa_weather)
    
    df_ppa_all = pd.concat(all_ppa, ignore_index=True)
    
    # 4. Merge aller Daten auf stündlicher Basis
    df_merged = pd.merge(df_da, df_h2, on="datetime", how="inner")
    df_merged = pd.merge(df_merged, df_ppa_all, on="datetime", how="inner")

    # 5. Finale Spalten auswählen für eine saubere Ausgabe
    ppa_mapping = {
        "wind": "pv0_wind100",
        "pv": "pv100_wind0",
        "mix": "pv50_wind50"
    }
    
    ppa_col = ppa_mapping.get(ppa_mode, "pv0_wind100")

    # 6. Finale Struktur
    df_final = df_merged[
        ["datetime",
         "DA_price",
         "h2_price",
         ppa_col]].rename(
             columns={ppa_col: "G_PPA_avail"}
         )

    df_final["year"] = df_final["datetime"].dt.year
    
    return df_final

# Test

if __name__ == "__main__":
    # === Testparameter ===
    start_year = 2030
    end_year = 2030

    # Pfade zu den Eingabedateien
    da_file = r"C:\RaD\GitHuB\Elektrolyseur_Optimierung\data\DA_prices_de.xlsx"
    h2_file = r"C:\RaD\GitHuB\Elektrolyseur_Optimierung\data\H2_prices_de.xlsx"

    # PPA API-Infos
    ppa_token = "556c605e18c957326de4152532b694c483986f64"
    ppa_lat = 52.52  
    ppa_lon = 13.405
    ppa_mode = "mix"

    # === Funktion ausführen ===
    df_all = load_all_data(start_year, end_year, da_file, h2_file, ppa_token, ppa_lat, ppa_lon, ppa_mode)

    # === Ausgabe prüfen ===
    print(df_all.head(10))
    print(f"\nAnzahl Stunden insgesamt: {len(df_all)}")
    print(f"\nSpalten: {list(df_all.columns)}")
