import os
import json
import csv
import glob

def process_irtt_data(folder_path, output_csv):
    interval_data = {}
    for filename in glob.glob(os.path.join(folder_path, '*.json')):
        print(f'Processing file: {filename}')
        with open(filename, 'r') as json_file:
            try:
                data = json.load(json_file)
            except json.JSONDecodeError as e:
                print(f'Error decoding JSON in file {filename}: {e}')
                continue

            if 'round_trips' not in data:
                print(f"'round_trips' key not found in file {filename}")
                continue

            for entry in data['round_trips']:
                try:
                    wall_time_ns = entry['timestamps']['client']['send']['wall']
                    uplink_delay_ns = entry['delay']['send']
                    downlink_delay_ns = entry['delay']['receive']
                except KeyError as e:
                    print(f'Missing key {e} in entry: {entry}')
                    continue

                ns_within_sec = wall_time_ns % 1_000_000_000
                ms_within_sec = ns_within_sec / 1_000_000
                interval_index = int(ms_within_sec // 100)

                target_ms = interval_index * 100
                distance_to_target = abs(ms_within_sec - target_ms)

                key = (wall_time_ns // 1_000_000_000, interval_index)

                if key not in interval_data or distance_to_target < interval_data[key]['distance']:
                    interval_data[key] = {
                        'uplink_delay_ms': uplink_delay_ns / 1_000_000,
                        'downlink_delay_ms': downlink_delay_ns / 1_000_000,
                        'wall_time_ns': wall_time_ns,
                        'distance': distance_to_target
                    }

    with open(output_csv, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['uplink_delay_ms', 'downlink_delay_ms', 'wall_time_ns'])
        sorted_data = sorted(interval_data.values(), key=lambda x: x['wall_time_ns'])

        for data_point in sorted_data:
            csv_writer.writerow([
                f'{data_point["uplink_delay_ms"]:.6f}',
                f'{data_point["downlink_delay_ms"]:.6f}',
                data_point['wall_time_ns']
            ])

    print('Data processing complete.')

if __name__ == '__main__':
    folder_path = 'path/to/irtt/directory'
    output_csv = 'path/you/want/to/save/irtt.csv'
    process_irtt_data(folder_path, output_csv)
