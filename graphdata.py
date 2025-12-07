#!/usr/bin/env python3
import sys
import pandas as pd
import matplotlib.pyplot as plt

# === CONFIGURABLE DEFAULTS ===
PURPLEAIR_DEFAULT = "purpleairCLEAN.csv"
AIRWISE_DEFAULT = "airwisedataclean.csv"

# NODES
NODE_1 = 2102560288
NODE_2 = 2102560276


def load_purpleair(path: str) -> pd.DataFrame:
    """Load PurpleAir CSV and prepare columns."""
    print(f"Reading PurpleAir data from {path}...")
    df = pd.read_csv(path)

    df["PacificTime"] = pd.to_datetime(df["PacificTime"], errors="coerce")
    df = df.dropna(subset=["PacificTime"]).sort_values("PacificTime")

    # Convert F to C for temperature
    df["temp_c"] = (df["current_temp_f"] - 32.0) * 5.0 / 9.0

    # Ensure numeric types
    numeric_cols = [
        "temp_c",
        "current_humidity",
        "pressure",
        "pm1_0_atm",
        "pm2_5_atm",
        "pm10_0_atm",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def load_airwise(path: str) -> pd.DataFrame:
    """Load AIRWISE CSV and prepare columns, including PacificTime."""
    print(f"Reading AIRWISE data from {path}...")
    df = pd.read_csv(path)

    if "pst_time" not in df.columns:
        raise KeyError("Expected a 'pst_time' column in the AIRWISE CSV.")

    df["PacificTime"] = pd.to_datetime(
        df["pst_time"].str.replace(" Pacific Standard Time", "", regex=False),
        errors="coerce",
    )
    df = df.dropna(subset=["PacificTime"]).sort_values("PacificTime")

    # Ensure numeric
    numeric_cols = [
        "temperature",
        "humidity",
        "pressure",
        "pm1_0",
        "pm2_5",
        "pm10",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def main(argv):
    # --- Handle command-line arguments ---
    if len(argv) >= 3:
        purpleair_path = argv[1]
        airwise_path = argv[2]
    else:
        purpleair_path = PURPLEAIR_DEFAULT
        airwise_path = AIRWISE_DEFAULT
        print(
            f"Usage: {argv[0]} purpleair.csv airwise.csv\n"
            f"No paths given, using defaults: {purpleair_path}, {airwise_path}"
        )

    # --- Load data ---
    pa = load_purpleair(purpleair_path)
    aw = load_airwise(airwise_path)

    # Split AIRWISE by node
    aw_1 = aw[aw["node"] == NODE_1].copy()
    aw_2 = aw[aw["node"] == NODE_2].copy()

    if aw_1.empty:
        print(f"Warning: no AIRWISE data found for node {NODE_1}")
    if aw_2.empty:
        print(f"Warning: no AIRWISE data found for node {NODE_2}")

    # === 1) TEMPERATURE (°C) ===
    plt.figure(figsize=(12, 6))
    plt.plot(pa["PacificTime"], pa["temp_c"], label="PurpleAir temp (°C)")
    if not aw_1.empty:
        plt.plot(aw_1["PacificTime"], aw_1["temperature"], label=f"Node {NODE_1} temp (°C)")
    if not aw_2.empty:
        plt.plot(aw_2["PacificTime"], aw_2["temperature"], label=f"Node {NODE_2} temp (°C)")
    plt.xlabel("Time (Pacific)")
    plt.ylabel("Temperature (°C)")
    plt.title("Temperature Comparison")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # === 2) HUMIDITY ===
    plt.figure(figsize=(12, 6))
    plt.plot(pa["PacificTime"], pa["current_humidity"], label="PurpleAir humidity (%)")
    if not aw_1.empty:
        plt.plot(aw_1["PacificTime"], aw_1["humidity"], label=f"Node {NODE_1} humidity (%)")
    if not aw_2.empty:
        plt.plot(aw_2["PacificTime"], aw_2["humidity"], label=f"Node {NODE_2} humidity (%)")
    plt.xlabel("Time (Pacific)")
    plt.ylabel("Relative Humidity (%)")
    plt.title("Humidity Comparison")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # === 3) PRESSURE ===
    plt.figure(figsize=(12, 6))
    plt.plot(pa["PacificTime"], pa["pressure"], label="PurpleAir pressure")
    if not aw_1.empty:
        plt.plot(aw_1["PacificTime"], aw_1["pressure"], label=f"Node {NODE_1} pressure")
    if not aw_2.empty:
        plt.plot(aw_2["PacificTime"], aw_2["pressure"], label=f"Node {NODE_2} pressure")
    plt.xlabel("Time (Pacific)")
    plt.ylabel("Pressure")
    plt.title("Pressure Comparison")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # === 4) PM1 ===
    plt.figure(figsize=(12, 6))
    plt.plot(pa["PacificTime"], pa["pm1_0_atm"], label="PurpleAir PM1 (atm)")
    if not aw_1.empty:
        plt.plot(aw_1["PacificTime"], aw_1["pm1_0"], label=f"Node {NODE_1} PM1")
    if not aw_2.empty:
        plt.plot(aw_2["PacificTime"], aw_2["pm1_0"], label=f"Node {NODE_2} PM1")
    plt.xlabel("Time (Pacific)")
    plt.ylabel("PM1")
    plt.title("PM1 Comparison")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.ylim(0, 15)   

    plt.tight_layout()

    # === 5) PM2.5 ===
    plt.figure(figsize=(12, 6))
    plt.plot(pa["PacificTime"], pa["pm2_5_atm"], label="PurpleAir PM2.5 (atm)")
    if not aw_1.empty:
        plt.plot(aw_1["PacificTime"], aw_1["pm2_5"], label=f"Node {NODE_1} PM2.5")
    if not aw_2.empty:
        plt.plot(aw_2["PacificTime"], aw_2["pm2_5"], label=f"Node {NODE_2} PM2.5")
    plt.xlabel("Time (Pacific)")
    plt.ylabel("PM2.5")
    plt.title("PM2.5 Comparison")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.ylim(0, 15)   

    plt.tight_layout()

    # === 6) PM10 ===
    plt.figure(figsize=(12, 6))
    plt.plot(pa["PacificTime"], pa["pm10_0_atm"], label="PurpleAir PM10 (atm)")
    if not aw_1.empty:
        plt.plot(aw_1["PacificTime"], aw_1["pm10"], label=f"Node {NODE_1} PM10")
    if not aw_2.empty:
        plt.plot(aw_2["PacificTime"], aw_2["pm10"], label=f"Node {NODE_2} PM10")
    plt.xlabel("Time (Pacific)")
    plt.ylabel("PM10")
    plt.title("PM10 Comparison")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.ylim(0, 15)   

    plt.tight_layout()

    # Show all six figures
    plt.show()


if __name__ == "__main__":
    main(sys.argv)
