import csv

def read_irtt_data(irtt_csv_file):
    irtt_data = []
    with open(irtt_csv_file, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            try:
                uplink_delay_ms = float(row['uplink_delay_ms'])
                downlink_delay_ms = float(row['downlink_delay_ms'])
                wall_time_ns = int(float(row['wall_time_ns']))
                irtt_data.append({
                    'uplink_delay_ms': uplink_delay_ms,
                    'downlink_delay_ms': downlink_delay_ms,
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
    with open(output_csv_file, 'w', newline='') as csvfile:
        fieldnames = ['uplink_throughput_Mbps', 'downlink_throughput_Mbps',
                      'uplink_delay_ms', 'downlink_delay_ms', 'wall_time_ns']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        iperf_index = 0
        written_intervals = set()
        for irtt_entry in irtt_data:
            irtt_wall_time_ns = irtt_entry['wall_time_ns']
            interval_wall_time_ns = (irtt_wall_time_ns // 100_000_000) * 100_000_000
            if interval_wall_time_ns in written_intervals:
                continue
            closest_iperf_entry, iperf_index = find_closest_iperf_entry(
                iperf_data, irtt_wall_time_ns, tolerance_ns, iperf_index
            )
            if closest_iperf_entry is not None:
                writer.writerow({
                    'uplink_throughput_Mbps': closest_iperf_entry['uplink_throughput_Mbps'],
                    'downlink_throughput_Mbps': closest_iperf_entry['downlink_throughput_Mbps'],
                    'uplink_delay_ms': irtt_entry['uplink_delay_ms'],
                    'downlink_delay_ms': irtt_entry['downlink_delay_ms'],
                    'wall_time_ns': interval_wall_time_ns
                })
                written_intervals.add(interval_wall_time_ns)
            else:
                continue

if __name__ == '__main__':
    irtt_csv_file = 'path/to/irtt.csv'
    iperf_csv_file = 'path/to/iperf3.csv'
    output_csv_file = 'path/you/want/to/save/combined_output_data.csv'

    irtt_data = read_irtt_data(irtt_csv_file)
    iperf_data = read_iperf_data(iperf_csv_file)

    combine_data(irtt_data, iperf_data, output_csv_file)
    print('Data combining complete.')
