import os
import json
import csv
import glob

def process_iperf_data(folder_path, output_csv):
    with open(output_csv, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['uplink_throughput_Mbps', 'downlink_throughput_Mbps', 'wall_time_ns'])
        for filename in glob.glob(os.path.join(folder_path, '*.json')):
            print(f'Processing file: {filename}')
            with open(filename, 'r') as json_file:
                try:
                    data = json.load(json_file)
                except json.JSONDecodeError as e:
                    print(f'Error decoding JSON in file {filename}: {e}')
                    continue
                try:
                    timesecs = data['start']['timestamp']['timesecs']
                except KeyError as e:
                    print(f'Missing key {e} in file {filename}')
                    continue
                timesecs_ns = int(timesecs * 1e9)

                for interval in data.get('intervals', []):
                    uplink_throughput_Mbps = None
                    downlink_throughput_Mbps = None
                    wall_time_ns = None

                    sender_stream = None
                    receiver_stream = None

                    streams = interval.get('streams', [])
                    for stream in streams:
                        if stream.get('sender') == True:
                            sender_stream = stream
                        elif stream.get('sender') == False:
                            receiver_stream = stream

                    if sender_stream is not None and receiver_stream is not None:
                        bits_per_second_uplink = sender_stream.get('bits_per_second', 0)
                        uplink_throughput_Mbps = bits_per_second_uplink / 1e6

                        bits_per_second_downlink = receiver_stream.get('bits_per_second', 0)
                        downlink_throughput_Mbps = bits_per_second_downlink / 1e6

                        start_time = sender_stream.get('start', 0)
                        start_time_ns = int(start_time * 1e9)
                        wall_time_ns = timesecs_ns + start_time_ns

                        csv_writer.writerow([
                            f'{uplink_throughput_Mbps:.6f}',
                            f'{downlink_throughput_Mbps:.6f}',
                            wall_time_ns
                        ])
                    else:
                        print(f'Incomplete data in interval in file {filename}')
                        continue

    fill_zeros_in_csv(output_csv, output_csv)

def fill_zeros_in_csv(csv_input, csv_output):
    data = []
    with open(csv_input, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)
        for row in reader:
            try:
                uplink = float(row[0])
            except ValueError:
                uplink = 0.0
            try:
                downlink = float(row[1])
            except ValueError:
                downlink = 0.0
            wall_time = row[2]
            data.append({'uplink': uplink, 'downlink': downlink, 'wall_time': wall_time})

    def fill_zeros(column_name):
        i = 0
        while i < len(data):
            if data[i][column_name] != 0:
                start_index = i
                valid_value = data[i][column_name]
                zeros_count = 0
                j = i + 1
                while j < len(data) and data[j][column_name] == 0:
                    zeros_count += 1
                    j += 1
                total_entries = zeros_count + 1
                divided_value = valid_value / total_entries
                for k in range(start_index, start_index + total_entries):
                    data[k][column_name] = divided_value
                i = j
            else:
                i += 1

    fill_zeros('uplink')

    fill_zeros('downlink')

    with open(csv_output, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for entry in data:
            writer.writerow([
                f'{entry["uplink"]:.6f}',
                f'{entry["downlink"]:.6f}',
                entry['wall_time']
            ])

if __name__ == '__main__':
    folder_path = 'path/to/iperf3/directory'
    output_csv = 'path/you/want/to/save/iperf3.csv'

    process_iperf_data(folder_path, output_csv)
    print('iperf3 processing complete.')
