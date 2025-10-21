import pandas as pd
from ortools.linear_solver import pywraplp

def run_optimization(h2_prices: pd.DataFrame, ppa_profiles: pd.DataFrame, da_prices: pd.DataFrame, params: dict):
    """
    Führt das Optimierungsmodell für den Elektrolyseur aus.
    ...
    """
    print ("Optimierungsmodell wird gestartet...")

    # 1. Solver initialisieren 
    solver = pywraplp.Solver.CreateSolver("GLOP") # 'GLOP' ist der LP-Solver von Google
    if not solver:
        print("Solver konnte nicht geladen werden.")
        return None
    print(f"Solver '{solver.Version()}' erfolgreich erstellt.") #oder solver.SolverVersioN()

    # 2. Daten aufbereiten und in Dictionaries umwandeln
    # Für eine bessere Performance wandel ich die Zeitreihen aus den Dataframes
    # in Dicitionaries um. Das macht den Zugriff in den Schleifen viel schneller

    # Ich erstelle eine zentrale DataFrame für alle stündlichen Daten 
    # und setzte den Dataframe-Index für die einfache Verknüpfung
    df = ppa_profiles.set_index('datetime')

    # Die monatlichen H2-Preise müssen jeder Stunde zugeordnet werden.
    # Ich extrahiere Jahr und Monat aus dem Index von df.
    df['year'] = df.index.year
    df['month'] = df.index.month

    # Jetzt kann ich die H2-Preise an den Haupt-DataFrame anfügen (mergen).
    df = pd.merge(df, h2_prices, on=['year', 'month'], how='left')

    # Platzhalter für Day-Ahead-Preise
    # Sobald ich die da_prices habe, werden diese auf ähnliche Weise gemerged.
    # Annahme: df_da hat eine 'datetime' und eine 'da_price' Spalte
    if da_prices is not None:
        df = pd.merge(df, da_prices.set_index('datetime'), left_index=True, right_index=True, how='left')
    else:
        # Wenn keine DA-Preise da sind, erstelle ich eine Platzhalter-Spalte mit Nullen
        df['da_price'] = 0.0
    #--------------------------------------

    # Erstelle eine Liste aller Zeitpunkte (Stunden) der Optimierung
    time_steps = df.index

    # Jetzt die finalen Dicionaries erstellen
    ppa_avail = df['G_PPA_avail'].to_dict()
    h2_price = df['h2_price'].to_dict()
    da_price = df['da_price'].to_dict()

    print(f"Daten für {len(time_steps)} Zeitpunkte aufbereitet.")

    # 3. Entscheidungsvariablen deklarieren
    # Ich erstelle für jede Variable ein Dictionary, das die Variablen-Objekte für jeden Zeitraum (t) speichert.

    E_ely = {}
    G_ppa_used = {}
    B_grid = {}
    S_sell = {}
    H_prod = {}
    Z_penalty = {}

    # Ich loope durch jeden einzelnen Zeutpunkt (jede Stunde) der Optimierung
    for t in time_steps:
        # solver.NumVar(untere_Schranke, obere-Schranke. "name")
        # ist diw OR-Tools Funktion, um eine kontinuierliche Variable zu erstellen.

        # Gesamtverbrauch Elektrolyseur [MWh]
        # Untere Schranke: 0, Obere Schranke: Die Maximale Leistung P_max
        E_ely[t] = solver.NumVar(0, params['P_max'] * params['delta_t'], f'E_ely_{t}')

        # Genutzter PPA-Strom [MWh]
        G_ppa_used[t] = solver.NumVar(0, solver.infinity(), f'G_ppa_used_{t}')

        # Zukauf aus dem Netz [MWh]
        B_grid[t] = solver.NumVar(0, solver.infinity(), f'B_grid_{t}')

        # Verkauf von PPA-Strom ins Netz [MWh]
        S_sell[t] = solver.NumVar(0, solver.infinity(), f'S_sell_{t}')

        # Produzierter Wasserstoff [MWh]
        H_prod[t] = solver.NumVar(0, solver.infinity(), f'H_prod_{t}')

        # Hilfsvariable für Mindestlast-Unterschreitung [MWh]
        Z_penalty[t] = solver.NumVar(0, solver.infinity(), f'Z_penalty_{t}')

    # Zähle die Dictionaries, die meine Variablentypen repräsentieren
    num_variable_types = 6 # (E_ely, G_ppa_used, B_grid, S_sell, H_prod, Z_penalty)

    print(f"{num_variable_types} Variablentypen für {len(time_steps)} Zeitpunkte erstellt.")

    # Constraints definieren
    # Ich loopen wieder durch jeden Zeitpunk, um die stündlichen Regeln festzulegen.
    for t in time_steps:
        # (1) Energie-Bilanz
        solver.Add(E_ely[t] == G_ppa_used[t] + B_grid[t], f'Energiebilanz_{t}')

        # (2) Umwandlung in Wasserstoff
        solver.Add(H_prod[t] == E_ely[t] * params['eta_ely'], f'Umwandlung_{t}')

        # (3) Maximal-Leistung:
        # Dieser Constraint ist bereits durch die Definition der Variable E_ely[t] (mit P_max als oberer Schranke abgedeckt)

        # (4) Mindestlast-Hilfsconstraint:
        solver.Add(Z_penalty[t] >= params['P_min'] * params['delta_t'] - E_ely[t], f'Mindestlast_Hilfe_{t}')

        # (5) Zukauf-Beschränkung
        # Hier benötige ich die externen daten für v_y,h
        # Da diese noch nicht vorliegen, lasse ich diesen Constraint vorerst raus
        # solver.Add(B_grid[t] <= params['P_max'] * params['delta_t'] * v[t], f'Zukauf_{t}')

        # 6a. Zeitraum-spezifische stündliche PPA-Constraints
        if t.year < 2030:
            # Stündlicher PPA-Verkauf: Verkauf ist nur bis zur Höhe der stündlich verfügbaren PPA-Menge möglich.
            solver.Add(S_sell[t] <= ppa_avail[t], f'PPA-Verkauf_{t}')
        else: # Jahre >= 2030
            # Stündliche PPA-Verfügbarkeit (Nutzung + Verkauf):
            # Genutzer und verkaufter Strom dürfen zusammen die verfügbare PPA-Menge
            # in dieser Stunde nicht überschreiten
            solver.Add(G_ppa_used[t] + S_sell[t] <= ppa_avail[t], f'PPA_Verfügbarkeit_{t}')
    
    print("Allgemeingültige und stündliche PPA-Constraints wurden erstellt.") 

    # 6b. Zeitraum-spezifischer monatlicher PPA-Constraint
    # Ich ermittle alle einzigartigen Jahr-Monat-Konbinationen
    unique_months = df[['year', 'month']].drop_duplicates().to_records(index=False)

    for year, month in unique_months:
        # DIeser Constraint gilt nur für die Jahre vor 2030
        if year < 2030:
            # Finde alle Stunden, die zu diesem spezifischen Jahr und Monat gehören
            hours_in_month = [t for t in time_steps if t.year == year and t.month == month]

            if not hours_in_month:
                continue    #überspringe diesen Monat, keine Daten vorhanden

            # Die Summe des genutzen PPA-Stroms in diesem Monat...
            monthy_g_used = solver.Sum([G_ppa_used[t] for t in hours_in_month])

            # ... darf nicht die Summe des verfügbaren PPA-Stroms in diesem Monat überschreiten
            monthy_ppa_avail = sum(ppa_avail[t] for t in hours_in_month)

            solver.Add(monthy_g_used <= monthy_ppa_avail, f'PPA-Monatsbudget_{year}-{month}')

    print("Monatliche PPA-Constraints wurden erstellt.")

    # 7. Zielfunktion definieren
    # Ich erstelle eine Liste, die alle stündlichen Gewinn-und Verlustterme enthält.
    objective_terms = []

    for t in time_steps:
        # Erlöse
        revenue_h2 = h2_price[t] * H_prod[t]
        revenue_sell = da_price[t] * S_sell[t]

        # Kosten
        cost_ppa = params['p_ppa'] * G_ppa_used[t]
        cost_grid = da_price[t] * B_grid[t]
        cost_penalty = params['strafe'] * Z_penalty[t]

        # Füge den stündlichen Deckungsbeitrag zur Liste hnzu
        objective_terms.append(revenue_h2 + revenue_sell - cost_ppa - cost_grid - cost_penalty)
    
    # Ich sage dem Solver, dass er die Summe aller Terme in dieser Liste maximieren soll
    solver.Maximize(solver.Sum(objective_terms))

    print("Zielfunktion wurde definiert.")

    # 8. Solver starten und Ergebnisse auslesen
    print("Starte den Solver...")
    status = solver.Solve()

    # 9. Ergebnisse verarbeiten und zurückgeben
    if status == pywraplp.Solver.OPTIMAL:
        print(f"Lösung gefunden in {solver.wall_time()} Millisekunden")
        print(f"Optimaler Gesamtgewinn {solver.Objective().Value:.2f} €")

        # Erstelle inen leeren DataFrame, um die Ergebnisse zu speichern
        results_df = pd.DataFrame(index=time_steps)
        df.index = pd.to_datetime(df.index).tz_convert("Europe/Berlin")

        # Lies die optimalen Werte für jede Variable aus und füge sie zum DataFrame hinzu
        for t in time_steps:
            results_df.loc[t, 'E_ely'] = E_ely[t].solution_value()
            results_df.loc[t, 'G_ppa_used'] = G_ppa_used[t].solution_value()
            results_df.loc[t, 'B_grid'] = B_grid[t].solution_value()
            results_df.loc[t, 'S_sell'] = S_sell[t].solution_value()
            results_df.loc[t, 'H_prod'] = H_prod[t].solution_value()
            results_df.loc[t, 'Z_penalty'] = Z_penalty[t].solution_value()

        return results_df

    elif status ==pywraplp.Solver.FEASIBLE:
        print("Eine möglihe, aber nicht garantiert optimale Lösung wurde gefunden.")
        # Ich könnte hier trotzdem die Ergebnisse auslesen, wenn notwendig.
        return None
    else: 
        print("Der Solver konnte keine Lösung finden.")
        return None





    

    