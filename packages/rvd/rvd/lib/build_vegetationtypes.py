import csv
import sys


def main(existing_vegetation_csv, historic_vegetation_csv, out_csv):

    with open(out_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["VegetationID", "EpochID", "Name", "Physiognomy", "LandUseGroup"])
        with open(historic_vegetation_csv, 'r') as in_csv:
            reader = csv.DictReader(in_csv)
            for row in reader:
                writer.writerow([row["VALUE"], 2, row["BPS_NAME"], row["GROUPVEG"], ""])

        with open(existing_vegetation_csv, 'r') as in_csv:
            reader = csv.DictReader(in_csv)
            for row in reader:
                writer.writerow([row["VALUE"], 1, row["EVT_Name"], row['EVT_PHYS'], row["EVT_GP_N"]])


if __name__ == "__main__":

    main(sys.argv[1], sys.argv[2], sys.argv[3])
