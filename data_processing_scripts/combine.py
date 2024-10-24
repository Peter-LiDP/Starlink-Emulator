import csv

def read_irtt_data(irtt_csv_file):
    irtt_data = []
    with open(irtt_csv_file, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            try:
                uplink_delay_ms = float(row['uplink_delay_ms'])
                downlink_delay_ms = float(row['downlink_delay_ms'])
                uplink_packet_loss = float(row['uplink_packet_loss'])
                downlink_packet_loss = float(row['downlink_packet_loss'])
                wall_time_ns = int(float(row['wall_time_ns']))
                irtt_data.append({
                    'uplink_delay_ms': uplink_delay_ms,
                    'downlink_delay_ms': downlink_delay_ms,
                    'uplink_packet_loss': uplink_packet_loss,
                    'downlink_packet_loss': downlink_packet_loss,
                    'wall_time_ns': wall_time_ns
                })
            except ValueError:
                continue
    irtt_data.sort(key=lambda x: x['wall_time_ns'])
    return irtt_data

def read_iperf_data(iperf_csv_file):
    iperf_data = []
    with open(iperf_csv_file, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            try:
                uplink_throughput_Mbps = float(row['uplink_throughput_Mbps'])
                downlink_throughput_Mbps = float(row['downlink_throughput_Mbps'])
                wall_time_ns = int(float(row['wall_time_ns']))
                iperf_data.append({
                    'uplink_throughput_Mbps': uplink_throughput_Mbps,
                    'downlink_throughput_Mbps': downlink_throughput_Mbps,
                    'wall_time_ns': wall_time_ns
                })
            except ValueError:
                continue
    iperf_data.sort(key=lambda x: x['wall_time_ns'])
    return iperf_data

def find_closest_iperf_entry(iperf_data, irtt_wall_time_ns, tolerance_ns, start_index):
    min_diff = tolerance_ns + 1
    closest_entry = None
    index = start_index
    n = len(iperf_data)
    while index < n and iperf_data[index]['wall_time_ns'] < irtt_wall_time_ns - tolerance_ns:
        index += 1
    search_index = index
    while search_index < n and iperf_data[search_index]['wall_time_ns'] <= irtt_wall_time_ns + tolerance_ns:
        diff = abs(iperf_data[search_index]['wall_time_ns'] - irtt_wall_time_ns)
        if diff < min_diff:
            min_diff = diff
            closest_entry = iperf_data[search_index]
        search_index += 1
    return closest_entry, index

def combine_data(irtt_data, iperf_data, output_csv_file, tolerance_ns=40_000_000):
    combined_dict = {}
    iperf_index = 0
    for irtt_entry in irtt_data:
        irtt_wall_time_ns = irtt_entry['wall_time_ns']
        interval_wall_time_ns = (irtt_wall_time_ns // 100_000_000) * 100_000_000
        if interval_wall_time_ns in combined_dict:
            existing_entry = combined_dict[interval_wall_time_ns]
            current_diff = abs(irtt_wall_time_ns - interval_wall_time_ns)
            if 'iperf_wall_time_ns' in existing_entry:
                existing_diff = abs(existing_entry['iperf_wall_time_ns'] - interval_wall_time_ns)
                if current_diff < existing_diff:
                    combined_dict[interval_wall_time_ns] = {
                        'uplink_throughput_Mbps': existing_entry['uplink_throughput_Mbps'],
                        'downlink_throughput_Mbps': existing_entry['downlink_throughput_Mbps'],
                        'uplink_delay_ms': irtt_entry['uplink_delay_ms'],
                        'downlink_delay_ms': irtt_entry['downlink_delay_ms'],
                        'uplink_packet_loss': irtt_entry['uplink_packet_loss'],
                        'downlink_packet_loss': irtt_entry['downlink_packet_loss'],
                        'wall_time_ns': interval_wall_time_ns,
                        'iperf_wall_time_ns': irtt_wall_time_ns
                    }
            else:
                combined_dict[interval_wall_time_ns] = {
                    'uplink_throughput_Mbps': existing_entry['uplink_throughput_Mbps'],
                    'downlink_throughput_Mbps': existing_entry['downlink_throughput_Mbps'],
                    'uplink_delay_ms': irtt_entry['uplink_delay_ms'],
                    'downlink_delay_ms': irtt_entry['downlink_delay_ms'],
                    'uplink_packet_loss': irtt_entry['uplink_packet_loss'],
                    'downlink_packet_loss': irtt_entry['downlink_packet_loss'],
                    'wall_time_ns': interval_wall_time_ns,
                    'iperf_wall_time_ns': irtt_wall_time_ns
                }
        else:
            closest_iperf_entry, iperf_index = find_closest_iperf_entry(
                iperf_data, irtt_wall_time_ns, tolerance_ns, iperf_index
            )
            if closest_iperf_entry is not None:
                combined_dict[interval_wall_time_ns] = {
                    'uplink_throughput_Mbps': closest_iperf_entry['uplink_throughput_Mbps'],
                    'downlink_throughput_Mbps': closest_iperf_entry['downlink_throughput_Mbps'],
                    'uplink_delay_ms': irtt_entry['uplink_delay_ms'],
                    'downlink_delay_ms': irtt_entry['downlink_delay_ms'],
                    'uplink_packet_loss': irtt_entry['uplink_packet_loss'],
                    'downlink_packet_loss': irtt_entry['downlink_packet_loss'],
                    'wall_time_ns': interval_wall_time_ns,
                    'iperf_wall_time_ns': closest_iperf_entry['wall_time_ns']
                }
    combined_data = list(combined_dict.values())
    combined_data.sort(key=lambda x: x['wall_time_ns'])
    filled_data = []
    n = len(combined_data)
    for i in range(n):
        filled_data.append(combined_data[i])
    for i in range(n - 1):
        current_time = combined_data[i]['wall_time_ns']
        next_time = combined_data[i + 1]['wall_time_ns']
        gap = next_time - current_time
        if gap > 5 * 60 * 1_000_000_000:
            continue
        expected_time = current_time + 100_000_000
        while expected_time < next_time:
            prev_entry = combined_dict.get(current_time)
            next_entry = combined_dict.get(next_time)
            if prev_entry and next_entry:
                averaged_entry = {
                    'uplink_throughput_Mbps': (prev_entry['uplink_throughput_Mbps'] + next_entry['uplink_throughput_Mbps']) / 2,
                    'downlink_throughput_Mbps': (prev_entry['downlink_throughput_Mbps'] + next_entry['downlink_throughput_Mbps']) / 2,
                    'uplink_delay_ms': (prev_entry['uplink_delay_ms'] + next_entry['uplink_delay_ms']) / 2,
                    'downlink_delay_ms': (prev_entry['downlink_delay_ms'] + next_entry['downlink_delay_ms']) / 2,
                    'uplink_packet_loss': (prev_entry['uplink_packet_loss'] + next_entry['uplink_packet_loss']) / 2,
                    'downlink_packet_loss': (prev_entry['downlink_packet_loss'] + next_entry['downlink_packet_loss']) / 2,
                    'wall_time_ns': expected_time
                }
                filled_data.append(averaged_entry)
            expected_time += 100_000_000
    filled_data.sort(key=lambda x: x['wall_time_ns'])
    unique_data = {}
    for entry in filled_data:
        interval_time = (entry['wall_time_ns'] // 100_000_000) * 100_000_000
        if interval_time in unique_data:
            existing_entry = unique_data[interval_time]
            current_diff = abs(entry['wall_time_ns'] - interval_time)
            existing_diff = abs(existing_entry['wall_time_ns'] - interval_time)
            if current_diff < existing_diff:
                unique_data[interval_time] = entry
        else:
            unique_data[interval_time] = entry
    final_data = list(unique_data.values())
    final_data.sort(key=lambda x: x['wall_time_ns'])
    with open(output_csv_file, 'w', newline='') as csvfile:
        fieldnames = ['uplink_throughput_Mbps', 'downlink_throughput_Mbps',
                      'uplink_delay_ms', 'downlink_delay_ms',
                      'uplink_packet_loss', 'downlink_packet_loss', 'wall_time_ns']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for entry in final_data:
            writer.writerow({
                'uplink_throughput_Mbps': f'{entry["uplink_throughput_Mbps"]:.6f}',
                'downlink_throughput_Mbps': f'{entry["downlink_throughput_Mbps"]:.6f}',
                'uplink_delay_ms': f'{entry["uplink_delay_ms"]:.6f}',
                'downlink_delay_ms': f'{entry["downlink_delay_ms"]:.6f}',
                'uplink_packet_loss': f'{entry["uplink_packet_loss"]:.6f}',
                'downlink_packet_loss': f'{entry["downlink_packet_loss"]:.6f}',
                'wall_time_ns': entry['wall_time_ns']
            })
    print('Data combining complete.')

if __name__ == '__main__':
    irtt_csv_file = 'path/to/irtt.csv'
    iperf_csv_file = 'path/to/iperf3.csv'
    output_csv_file = 'path/you/want/to/save/combined_output_data.csv'

    irtt_data = read_irtt_data(irtt_csv_file)
    iperf_data = read_iperf_data(iperf_csv_file)

    combine_data(irtt_data, iperf_data, output_csv_file)
