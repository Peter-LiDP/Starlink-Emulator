import os
import json
import csv
import glob

def process_irtt_data(folder_path, output_csv):
    interval_data = {}
    INTERVAL_MS = 100

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
                    wall_time_ns = entry.get('timestamps', {}).get('client', {}).get('send', {}).get('wall', None)
                    if wall_time_ns is None:
                        continue
                    uplink_delay_ns = None
                    downlink_delay_ns = None

                lost = entry.get('lost', 'false').lower()

                is_lost_up = lost == 'true_up'
                is_lost_down = lost in ('true_down', 'true')

                seconds = wall_time_ns // 1_000_000_000
                ns_within_sec = wall_time_ns % 1_000_000_000
                ms_within_sec = ns_within_sec / 1_000_000
                interval_index = int(ms_within_sec // INTERVAL_MS)
                key = (seconds, interval_index)

                if key not in interval_data:
                    interval_data[key] = {
                        'uplink_delay_ms': None,
                        'downlink_delay_ms': None,
                        'wall_time_ns': None,
                        'distance': float('inf'),
                        'total_packets': 0,
                        'uplink_lost_packets': 0,
                        'downlink_lost_packets': 0,
                        'lost_wall_times_ns': []
                    }

                interval_data[key]['total_packets'] += 1

                if is_lost_up or is_lost_down:
                    if wall_time_ns is not None:
                        interval_data[key]['lost_wall_times_ns'].append(wall_time_ns)

                if is_lost_up:
                    interval_data[key]['uplink_lost_packets'] += 1
                if is_lost_down:
                    interval_data[key]['downlink_lost_packets'] += 1

                if not (is_lost_up or is_lost_down):
                    target_ms = interval_index * INTERVAL_MS
                    distance_to_target = abs(ms_within_sec - target_ms)

                    if distance_to_target < interval_data[key]['distance']:
                        interval_data[key]['uplink_delay_ms'] = uplink_delay_ns / 1_000_000 if uplink_delay_ns is not None else None
                        interval_data[key]['downlink_delay_ms'] = downlink_delay_ns / 1_000_000 if downlink_delay_ns is not None else None
                        interval_data[key]['wall_time_ns'] = wall_time_ns
                        interval_data[key]['distance'] = distance_to_target

    processed_data = []
    for key, data_point in interval_data.items():
        total = data_point['total_packets']
        uplink_lost = data_point['uplink_lost_packets']
        downlink_lost = data_point['downlink_lost_packets']

        uplink_packet_loss = uplink_lost / total if total > 0 else 0
        downlink_packet_loss = downlink_lost / total if total > 0 else 0

        all_packets_lost = (uplink_lost + downlink_lost) >= total

        if all_packets_lost:
            uplink_delay_ms = 200.0
            downlink_delay_ms = 200.0
            uplink_packet_loss = 1.0
            downlink_packet_loss = 1.0

            target_wall_time_ns = (key[0] * 1_000_000_000) + (key[1] * INTERVAL_MS * 1_000_000)

            closest_wall_time_ns = None
            min_distance = float('inf')
            for lost_wall_time in data_point['lost_wall_times_ns']:
                distance = abs(lost_wall_time - target_wall_time_ns)
                if distance < min_distance:
                    min_distance = distance
                    closest_wall_time_ns = lost_wall_time

            if closest_wall_time_ns is not None:
                wall_time_ns = closest_wall_time_ns
            else:
                wall_time_ns = target_wall_time_ns
        else:
            uplink_delay_ms = data_point['uplink_delay_ms'] if data_point['uplink_delay_ms'] is not None else 0.0
            downlink_delay_ms = data_point['downlink_delay_ms'] if data_point['downlink_delay_ms'] is not None else 0.0
            wall_time_ns = data_point['wall_time_ns'] if data_point['wall_time_ns'] is not None else (
                (key[0] * 1_000_000_000) + (key[1] * INTERVAL_MS * 1_000_000)
            )

        processed_data.append({
            'uplink_delay_ms': uplink_delay_ms,
            'downlink_delay_ms': downlink_delay_ms,
            'uplink_packet_loss': uplink_packet_loss,
            'downlink_packet_loss': downlink_packet_loss,
            'wall_time_ns': wall_time_ns
        })

    sorted_data = sorted(processed_data, key=lambda x: x['wall_time_ns'])

    with open(output_csv, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['uplink_delay_ms', 'downlink_delay_ms', 'uplink_packet_loss', 'downlink_packet_loss', 'wall_time_ns'])

        for data_point in sorted_data:
            csv_writer.writerow([
                f'{data_point["uplink_delay_ms"]:.6f}',
                f'{data_point["downlink_delay_ms"]:.6f}',
                f'{data_point["uplink_packet_loss"]:.6f}',
                f'{data_point["downlink_packet_loss"]:.6f}',
                data_point['wall_time_ns']
            ])

    print('Data processing complete.')

if __name__ == '__main__':
    folder_path = 'path/to/irtt/directory'
    output_csv = 'path/you/want/to/save/irtt.csv'
    process_irtt_data(folder_path, output_csv)
