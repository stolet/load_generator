import os
import pandas as pd
import argparse
import seaborn as sns
import matplotlib.pyplot as plt

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--directory", required=True, 
        help="Path to the directory containing log files.")
    args = parser.parse_args()
    return args


def parse_log_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    file_name = os.path.basename(file_path)
    system, rate = file_name.split('_')[0], file_name.split('_')[1].split('.')[0]
    
    p50_latency, p99_latency, p99_9_latency = None, None, None
    for line in lines:
        if line.startswith("Totals"):
            parts = line.split()
            p50_latency = float(parts[5])
            p99_latency = float(parts[6])
            p99_9_latency = float(parts[7])
            break
    
    return system, int(rate), p50_latency, p99_latency, p99_9_latency


def parse_logs(directory):
    data = {
        "System": [],
        "Rate": [],
        "p50": [],
        "p99": [],
        "p99.9": []
    }

    for file_name in os.listdir(directory):
        if file_name.endswith(".log"):  
            file_path = os.path.join(directory, file_name)
            system, rate, p50, p99, p99_9 = parse_log_file(file_path)
            
            data["System"].append(system)
            data["Rate"].append(rate)
            data["p50"].append(p50)
            data["p99"].append(p99)
            data["p99.9"].append(p99_9)

    df = pd.DataFrame(data)
    df = df.sort_values(by=["System", "Rate"])
    df = df.groupby(["System", "Rate"]).mean().reset_index()
    #df = df[df["Rate"] <= 20000]
    df["p99.9"] = df["p99.9"] * 1000
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)

    return df


def plot_data(df):
    sns.set()
    palette = sns.color_palette()
    custom_palette = {
        'shim': palette[1],   # Second color in the palette
        'linux': palette[2]   # First color in the palette
    }
    plt.figure(figsize=(10, 6))
    lineplot = sns.lineplot(data=df, x="Rate", y="p99.9", hue="System", marker="o", palette=custom_palette)
    lineplot.set(xlabel="Throughput [req/s]", ylabel="99,9p Latency [µs]")
    plt.savefig("lat_redis.png", dpi=2000)


def main():
    args = parse_args()
    df = parse_logs(args.directory)
    plot_data(df)


if __name__ == "__main__":
    main()
