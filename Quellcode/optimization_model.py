import pandas as pd
from ortools.linear_solver import pywraplp

def run_optimization(df_all: pd.DataFrame, params: dict):
    """
    Führt das Optimierungsmodell für den Elektrolyseur aus.
    
    Args:
        df_all (pd.DataFrame): Stündlicher DataFrame aus load_data mit Spalten:
            ["datetime", "DA_price", "h2_price", "G_PPA_avail", "year", "v"]
        params (dict): Dictionary mit allen Modellparametern, z.B.
            - P_max, P_min
            - delta_t
            - eta_ely
            - p_ppa
            - penalty
    
    Returns:
        result_df: Optimale Stundenwerte für E_ely, P_ppa_used, B_grid, S_sell, H_prod, Z_penalty

    """
    print ("Optimierungsmodell wird gestartet...")

    # - 1. Prüfung -
    required_cols = ["datetime", "DA_price", "h2_price", "G_PPA_avail", "v"]
    if not all (c in df_all.columns for c in required_cols):
        raise ValueError(f"Fehlende Spalten in df_all. Erwartet: {required_cols}")
    
    # - 2. Solver initialisieren -
    solver = pywraplp.Solver.CreateSolver("SCIP_MIXED_INTEGER_PROGRAMMING") # "SCIP" ist der MILP-Solver von Google
    if not solver:
        raise RuntimeError("Solver konnte nicht erstellt werden.")
    print(f"Solver '{solver.SolverVersion()}' erfolgreich geladen.")

    # - 3. Zeitpunkte vorbereiten -
    df_all = df_all.sort_values("datetime").reset_index(drop=True)
    n = len(df_all)  
    df_all["year"] = df_all["datetime"].dt.year
    df_all["month"] = df_all["datetime"].dt.month

    print(f"{n} Stunden für die Optimierung geladen.")

    # - 4. Variablen definieren - 
    E_ely = [solver.NumVar(0, params['P_max'] * params['delta_t'], f"E_ely_{i}") for i in range(n)]
    G_ppa_used = [solver.NumVar(0, solver.infinity(), f"G_ppa_used_{i}") for i in range(n)]
    B_grid = [solver.NumVar(0, solver.infinity(), f"B_grid_{i}") for i in range(n)]
    S_sell = [solver.NumVar(0, solver.infinity(), f"S_sell_{i}") for i in range(n)]
    H_prod = [solver.NumVar(0, solver.infinity(), f"H_prod_{i}") for i in range(n)]
    u = [solver.BoolVar(f"u_{i}") for i in range(n)]        # Ely an/aus

    print(f"{6 * n} Variablen erstellt ({n} Stunden x 6 Variablen).")

    # - 5. Constraints - 
    # --------------------------
    # (1) Energiebilanz
    # Für jede Stunde gilt: genutzter PPA-Strom + Zukauf aus Netz = Stromverbrauch des Elektrolyseurs
    # keine Speicherung oder Verluste Berücksichtigt

    for i in range(n):
        solver.Add(
            E_ely[i] == G_ppa_used[i] + B_grid[i],
        ) #--> Abwägen, ob <= besser/genauer ist

    # (2) PPA-Verfügbarkeit
    
    # Ab 2030: stündliche Beschränkung
    # Vor 2030: monatliche Bilanzierung (Summe genutzt + verkauft <= Summe verfügbar)

    for i in range(n):
        if df_all.loc[i, "year"] >= 2030:
            solver.Add(G_ppa_used[i] <= df_all.loc[i, "G_PPA_avail"])
            solver.Add(S_sell[i] <= df_all.loc[i, "G_PPA_avail"] - G_ppa_used[i])
            
    # Vor 2030: Monatsbilanz
    for (y,m), group in df_all[df_all["year"] < 2030].groupby(["year", "month"]):
        idx = group.index.tolist()
        solver.Add(
            solver.Sum(G_ppa_used[i] + S_sell[i] for i in idx)
            <= group["G_PPA_avail"].sum(),
        )

    # (3) Umwandlung Strom -> Wasserstoff
    # Für jede Stunde: Erzeugter Wasserstoff = Stromverbrauch * Wirkungsgrad
    # etwa_ely in Dezimalform, z.B. 0.7 -> später ggf. lastabhängig erweiterbar

    for i in range(n):
        solver.Add(
            H_prod[i] == E_ely[i] * params["eta_ely"],
        )

    # (4) Mindestlast und Maximallast
    
    Pmin_dt = params["P_min"] * params["delta_t"]
    Pmax_dt = params["P_max"] * params["delta_t"]
    for i in range(n):
        solver.Add(E_ely[i] >= Pmin_dt * u[i])
        solver.Add(E_ely[i] <= Pmax_dt * u[i])

    # (5) Zukaufbeschränkung
    # Netzbezug (B_grid) ist nur erlaubt, wenn v = 1
    # Wenn v = 0 -> B_grid muss 0 sein
    # Wenn v = 1 -> B_grid kann bis zur Ely-Maximalleistung reichen

    for i in range(n):
        solver.Add(B_grid[i] <= params["P_max"] * params["delta_t"] * df_all.loc[i, "v"]) # --> Einfach erweiterbar, eventuell CO2-Preis-Kriterium
        
    # - 6. Zielfunktion: Erlösmaximierung -
    # Ziel: Maximiere Gewinn = (H2-Erlöse + Überschussverkauf) - (Zukaufskosten + Strafkosten)

    objective = solver.Objective()

    for i in range(n):
        # Einnahmen
        objective.SetCoefficient(H_prod[i], df_all.loc[i, "h2_price"])
        objective.SetCoefficient(S_sell[i], df_all.loc[i, "DA_price"])

        # Kosten 
        objective.SetCoefficient(B_grid[i], -df_all.loc[i, "DA_price"] - 0.001) # tierbreaker

    # FESTE PPA-Kosten (pay-as-produced)
    total_ppa_cost = params["p_ppa"] * float(df_all["G_PPA_avail"].sum())
    objective.SetOffset(-total_ppa_cost)

    # Zielfunktion maximieren
    objective.SetMaximization()

    # 6. Solver starten
    print("Löse Optimierungsproblem...")
    status = solver.Solve()

    if status != pywraplp.Solver.OPTIMAL:
        print("Warnung: Kein optimales Ergebnis gefunden. Status:", status)
    else:
        print("Optimale Lösung gefunden!")
    
    # Ergebnisse extrahieren
    results = pd.DataFrame({
        "datetime": df_all["datetime"],
        "DA_price": df_all["DA_price"],
        "h2_price": df_all["h2_price"],
        "G_PPA_avail": df_all["G_PPA_avail"],
        "v": df_all["v"],
        "E_ely": [E_ely[i].solution_value() for i in range(n)],
        "G_ppa_used": [G_ppa_used[i].solution_value() for i in range(n)],
        "B_grid": [B_grid[i].solution_value() for i in range(n)],
        "S_sell": [S_sell[i].solution_value() for i in range(n)],
        "H_prod": [H_prod[i].solution_value() for i in range(n)],
        "u": [u[i].solution_value() for i in range(n)],
    })
    # Kleine numerische Rundungsfehler korrigieren:
    results = results.round(10)
    results["S_sell"] = results["S_sell"].clip(lower=0)


    # Optimimalen Wert speichern (inkl. Offset)
    results.attrs["objective_value"] = solver.Objective().Value()
    print(f"Zielfunktionswert: {results.attrs['objective_value']:.2f} €")

    return results  
    




    

    