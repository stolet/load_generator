import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--directory", required=True,
        help="path to directory containing log files")
    args = parser.parse_args()
    return args

def plot_data(df):
    # Print data
    names = df['Name'].unique()
    for name in names:
        sorted_df = df[df['Name'] == name].sort_values(by="Rate")

        print(sorted_df)

    # Convert from nanoseconds to microseconds
    df["99.9p"] = df.apply(lambda row: row["99.9p"] / 1000, axis=1)

    sns.set_theme()
    lineplot = sns.lineplot(x="Rate", y="99.9p", hue="Name", data=df, marker="o")
    lineplot.set(xlabel="Throughput [req/s]", ylabel="99.9p Latency [µs]")

    plt.ylim(0, 500)
    plt.xlim(0, 150000)
    plt.savefig("lat_echo.png", dpi=2000)

def parse_log_file(filepath):
    with open(filepath, 'r') as file:
        lines = file.readlines()
        data = {}
        for line in lines:
            if '50p=' in line:
                data['50p'] = float(line.split('=')[1].strip())
            elif '99p=' in line:
                data['99p'] = float(line.split('=')[1].strip())
            elif '99.9p=' in line:
                data['99.9p'] = float(line.split('=')[1].strip())
        return data

def parse_logs(directory):
    results = {}

    for filename in os.listdir(directory):
        if filename.endswith('.log'):
            name, rate, run = filename.split('_')
            rate = int(rate)
            run = int(run.split('.')[0])

            filepath = os.path.join(directory, filename)
            data = parse_log_file(filepath)

            key = (name, rate)
            if key not in results:
                results[key] = {'50p': [], '99p': [], '99.9p': []}

            print(filename)
            results[key]['50p'].append(data['50p'])
            results[key]['99p'].append(data['99p'])
            results[key]['99.9p'].append(data['99.9p'])

    # Calculate averages and prepare data for DataFrame
    averaged_results = []
    for (name, rate), values in results.items():
        avg_50p = int(sum(values['50p']) / len(values['50p']))
        avg_99p = int(sum(values['99p']) / len(values['99p']))
        avg_99_9p = int(sum(values['99.9p']) / len(values['99.9p']))
        averaged_results.append({'Name': name, 'Rate': rate, '50p': avg_50p, '99p': avg_99p, '99.9p': avg_99_9p})

    # Create DataFrame
    df = pd.DataFrame(averaged_results)
    return df

def main():
    args = parse_args()

    # Directory containing the log files
    log_directory = args.directory

    # Process the logs and create the DataFrame
    df = parse_logs(log_directory)

    plot_data(df)


if __name__ == "__main__":
    main()

