import pandas as pd
import os
from datetime import datetime, timedelta
import pytz # Zeitzonen-Unterstützung für Merge 

def read_h2_prices(file_path, start_year, end_year):
    """
    Liest H2-Preise aus Excel ein und filtert nach Jahr.

    Args:
        file_path (str): Pfad zur Excel-Datei
        start_year (int): Startjahr
        end_year (int): Endjahr

    Returns:
        pd.DataFrame: DataFrame mit Spalten ["year", "month", "h2_price"]
    """
    
    try:
        df = pd.read_excel(file_path, sheet_name="€_per_MWh")
    except FileNotFoundError:
        print(f"Datei {file_path} nicht gefunden.")
        return pd.DataFrame(columns=["year", "month", "h2_price"])

    # Prüfen, ob alle Spalten vorhanden sind
    required_cols = ["year", "month", "h2_price"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError (f"Excel-Tabelle muss die Spalten {required_cols} enthalten.")
    
    # Typen sicherstellen
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["h2_price"] = pd.to_numeric(df["h2_price"], errors="coerce")

    # Filtere nach Jahr
    df = df[(df["year"] >= start_year) & (df["year"] <= end_year)]
    if df.empty:
        print(f"Keine H2-Preise für den Zeitraum {start_year}-{end_year} gefunden.")

    return df.reset_index(drop=True)

def expand_h2_prices_hourly(df_monthly, tz_name="UTC"):
    """
    Wandelt monatliche H2-Preise in stündliche Werte um (jede Stunde im Monat im Monat erhält den selben Preis)
    Args: 
        df_monthly (pd.DataFrame): DataFrame mit Spalten ["year", "month", "h2_price"]
    Returns:
        pd.DataFrame: stündlicher DataFrame mit ["datetime", "h2_price", "year", "month"]
    """
    hourly_records = []
    
    for _, row in df_monthly.iterrows():
        year, month, price = int(row["year"]), int(row["month"]), row["h2_price"]
        start = pd.Timestamp(year=year, month=month, day=1, tz=tz_name)
        # Ende des Monats
        if month == 12:
            end = pd.Timestamp(year=year+1, month=1, day=1, tz=tz_name)
        else:
            end = pd.Timestamp(year=year, month=month+1, day=1, tz=tz_name)
        
        # Alle Stunden auf einmal erzeugen
        dates = pd.date_range(start=start, end=end - pd.Timedelta(hours=1), freq="h" )
        df_hourly = pd.DataFrame({
            "datetime": dates,
            "h2_price": price,
            "year": year,
            "month": month
        })
        hourly_records.append(df_hourly)
            
    # Alle Monate zusammenfügen
    result = pd.concat(hourly_records).reset_index(drop=True)
    return result

# Beispiel
if __name__ == "__main__":
    start_year, end_year = 2030, 2030
    file_path = r"C:\RaD\GitHuB\Elektrolyseur_Optimierung\data\H2_prices_de.xlsx"
    df_monthly = read_h2_prices(file_path, start_year, end_year)
    df_hourly = expand_h2_prices_hourly(df_monthly)
    print(df_hourly.head(10))
    print(f"Anzahl Stunden: {len(df_hourly)}")
