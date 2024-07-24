import os
import pandas as pd
import matplotlib.pyplot as plt


def plot_data(df):
    shim_df = df[df['Name'] == 'shim'].sort_values(by="Rate")
    demi_df = df[df['Name'] == 'demi'].sort_values(by="Rate")

    throughput = shim_df["Rate"]
    latency_shim = shim_df["99.9p"] / 1000
    latency_demi = demi_df["99.9p"] / 1000
    
    # Plotting the data
    plt.figure(figsize=(10, 6))
    plt.plot(throughput, latency_shim, label='shim', marker="o")
    plt.plot(throughput, latency_demi, label='demikernel', marker="x")
    
    # Set limit on y-axis
    plt.ylim(0, 300)
    
    # Labeling the axes
    plt.xlabel('Throughput [req/s]')
    plt.ylabel('99.9p Latency [µs]')
    
    # Adding a legend
    plt.legend()
    
    # Display the plot
    plt.savefig("lat.png")

def parse_log_file(filepath):
    with open(filepath, 'r') as file:
        lines = file.readlines()
        data = {}
        for line in lines:
            if '50p=' in line:
                data['50p'] = float(line.split('=')[1].strip())
            elif '99.9p=' in line:
                data['99.9p'] = float(line.split('=')[1].strip())
        return data

def process_logs(directory):
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
                results[key] = {'50p': [], '99.9p': []}
            
            results[key]['50p'].append(data['50p'])
            results[key]['99.9p'].append(data['99.9p'])
    
    # Calculate averages and prepare data for DataFrame
    averaged_results = []
    for (name, rate), values in results.items():
        avg_50p = int(sum(values['50p']) / len(values['50p']))
        avg_99_9p = int(sum(values['99.9p']) / len(values['99.9p']))
        averaged_results.append({'Name': name, 'Rate': rate, '50p': avg_50p, '99.9p': avg_99_9p})
    
    # Create DataFrame
    df = pd.DataFrame(averaged_results)
    return df

def main():
    # Directory containing the log files
    log_directory = 'logs'

    # Process the logs and create the DataFrame
    df = process_logs(log_directory)

    plot_data(df)


if __name__ == "__main__":
    main()

