#!/usr/bin/env python3
import sys
import pandas as pd


def main(argv):
    if len(argv) < 2:
        print(f"Usage: {argv[0]} file1.csv [file2.csv ...]")
        sys.exit(1)

    input_files = argv[1:]
    frames = []

    for path in input_files:
        print(f"Reading {path}...")
        df = pd.read_csv(path)
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)

    # --- Time handling: UTC -> Pacific ---
    df["UTCDateTime"] = pd.to_datetime(df["UTCDateTime"], utc=True, errors="coerce")

    df = df.dropna(subset=["UTCDateTime"])

    # Convert to pacific time
    df["PacificTime"] = df["UTCDateTime"].dt.tz_convert("America/Los_Angeles")

    df = df.sort_values("PacificTime")

    # CHANGE COLUMNS YOU WANT
    cols = [
        "PacificTime",
        "current_temp_f",
        "current_humidity",
        "pressure",
        "pm1_0_atm",
        "pm2_5_atm",
        "pm10_0_atm",
    ]

    out = df[cols].copy()

    out[["pm1_0_atm", "pm2_5_atm", "pm10_0_atm", "pressure"]] = out[
        ["pm1_0_atm", "pm2_5_atm", "pm10_0_atm", "pressure"]
    ].round(3)

    out["PacificTime"] = out["PacificTime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Write to CSV
    output_name = "purpleairCLEAN.csv"
    out.to_csv(output_name, index=False)
    print(f"Wrote {len(out)} rows to {output_name}")


if __name__ == "__main__":
    main(sys.argv)

