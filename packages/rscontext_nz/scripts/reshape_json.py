import json

# Load the original JSON file
with open("/Users/philipbailey/GISData/nz/2025_09_25_projectS_with_taudem.json", "r") as f:
    data = json.load(f)

reshaped = {}

for entry in data:
    huc = entry["huc"]

    # If duplicate HUCs exist, give them a unique suffix
    if huc in reshaped:
        suffix = 1
        new_huc = f"{huc}_{suffix}"
        while new_huc in reshaped:
            suffix += 1
            new_huc = f"{huc}_{suffix}"
        huc = new_huc

    reshaped[huc] = {
        "RSCONTEXT_ID": entry["RSCONTEXT_ID"],
        "CHANNEL_AREA_ID": entry["CHANNEL_AREA_ID"],
        "TAUDEM_ID": entry["TAUDEM_ID"]
    }

# Save reshaped JSON
with open("/Users/philipbailey/GISData/nz/2025_09_25_projectS_with_taudem_reshaped.json", "w") as f:
    json.dump(reshaped, f, indent=2)

print("Reshaped JSON written to output.json")
