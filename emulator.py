import time
import csv
import threading
import argparse
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Mininet
from multiprocessing import Process, Value
from mininet.cli import CLI

iface1 = "r2-eth0"
init_flags = {
    'r3-eth1': False,
    'r5-eth0': False,
    'r2-eth1': False,
    'r4-eth0': False
}

barrier = threading.Barrier(4)
update_event = threading.Event()
start_event = threading.Event()
timestamp_5g = Value('i', 0)
timestamp_starlink = Value('i', 0)

start_time_option = None
start_time_offset = 0

def auto_test():
    # your test code
    pass

class NetworkConfigThread(threading.Thread):
    def __init__(self, net, host_name, dev, column, barrier, timestamp, update_event, data_file):
        super().__init__()
        self.net = net
        self.host_name = host_name
        self.column = column
        self.dev = dev
        self.barrier = barrier
        self.timestamp = timestamp
        self.update_event = update_event
        self.data_file = data_file
        self.latency_lines = self.read_csv_data()
        self.timestamp_to_line = self.create_timestamp_index()
        self.timestamps = sorted([int(line[-1]) for line in self.latency_lines])
        self.total_duration = self.timestamps[-1] - self.timestamps[0] + 100

    def read_csv_data(self):
        with open(self.data_file, 'r') as file:
            reader = csv.reader(file)
            latency_lines = [line for line in reader if line]
        return latency_lines

    def create_timestamp_index(self):
        timestamp_index = {}
        for idx, line in enumerate(self.latency_lines):
            timestamp = int(line[-1])
            timestamp_index[timestamp] = idx
        return timestamp_index

    def run(self):
        configureNetworkConditions(self)

def configureNetworkConditions(thread_obj):
    global init_flags
    global start_time_offset
    global start_event
    net = thread_obj.net
    host_name = thread_obj.host_name
    dev = thread_obj.dev
    column = thread_obj.column
    barrier = thread_obj.barrier
    update_event = thread_obj.update_event
    latency_lines = thread_obj.latency_lines
    timestamp_to_line = thread_obj.timestamp_to_line
    data_file = thread_obj.data_file
    timestamp = thread_obj.timestamp

    host = net.get(host_name)
    
    if not init_flags[dev]:
        init_flags[dev] = True
        check_and_start_test()

    start_event.wait()

    current_timestamp = get_current_virtual_timestamp(start_time_offset)
    line_num = find_line_number(timestamp_to_line, current_timestamp)

    if line_num is None:
        line_num = synchronize_timestamp(timestamp_to_line, current_timestamp)
        if line_num is None:
            print(f"No matching timestamp found in {data_file}. Exiting thread.")
            return

    if '5G' in data_file:
        loss_rate = '1%'
    else:
        loss_rate = '2%'

    initialBW = float(latency_lines[line_num][column - 2])
    cmd_bw = f'tc qdisc replace dev {dev} root handle 1: tbf rate {initialBW}mbit burst 15k latency 50ms'
    host.cmd(cmd_bw)

    initialDelay = float(latency_lines[line_num][column])
    cmd_jitter = f'tc qdisc add dev {dev} parent 1:1 handle 10: netem delay {initialDelay}ms loss {loss_rate}'
    host.cmd(cmd_jitter)
    
    barrier.wait()

    while True:
        update_event.wait()
        update_event.clear()
        
        virtual_timestamp = timestamp.value
        
        current_wall_time_ms = int(time.time() * 1000)
        wall_time_in_minute = current_wall_time_ms % (60 * 1000)
        virtual_timestamp_in_minute = virtual_timestamp % (60 * 1000)
        expected_wall_time = (virtual_timestamp_in_minute - start_time_offset) % (60 * 1000)
        deviation = (wall_time_in_minute - expected_wall_time) % (60 * 1000)
        if deviation > 30 * 1000:
            deviation -= 60 * 1000

        tolerance = 30
        if abs(deviation) > tolerance:
            print(f"[{data_file}] Deviation {deviation} ms exceeds tolerance at wall time {wall_time_in_minute} ms.")
            second_correction = (((wall_time_in_minute + start_time_offset) % (60 * 1000)) // 100) * 100
            timestamp.value = (virtual_timestamp // (60 * 1000)) * 60 * 1000 + second_correction
            if timestamp.value < virtual_timestamp and abs(timestamp.value - virtual_timestamp) >= 30000:
                timestamp.value = (virtual_timestamp // (60 * 1000) + 1) * 60 * 1000 + second_correction
            
            virtual_timestamp = timestamp.value

        effective_timestamp = ((virtual_timestamp - thread_obj.timestamps[0]) % thread_obj.total_duration) + thread_obj.timestamps[0]
        line_num = find_line_number(thread_obj.timestamp_to_line, effective_timestamp)
        if line_num is None:
            print(f"[{data_file}] Virtual timestamp {virtual_timestamp} ms not found in data file.")
            continue

        currentBW = float(latency_lines[line_num][column - 2])
        update_cmd_bw = f'tc qdisc change dev {dev} root handle 1: tbf rate {currentBW}mbit burst 15k latency 50ms'
        host.cmd(update_cmd_bw)

        currentDelay = float(latency_lines[line_num][column])
        update_cmd = f'tc qdisc change dev {dev} parent 1:1 handle 10: netem delay {currentDelay}ms loss {loss_rate}'
        host.cmd(update_cmd)

        barrier.wait()

def get_current_virtual_timestamp(start_time_offset):
    current_wall_time_ms = int(time.time() * 1000)
    current_time_in_minute = current_wall_time_ms % (60 * 1000)
    virtual_timestamp = (current_time_in_minute + start_time_offset) % (60 * 1000)
    virtual_timestamp = (virtual_timestamp // 100) * 100
    return virtual_timestamp

def find_line_number(timestamp_to_line, timestamp):
    if timestamp in timestamp_to_line:
        return timestamp_to_line[timestamp]
    else:
        return None

def synchronize_timestamp(timestamp_to_line, current_timestamp):
    timestamps = sorted(timestamp_to_line.keys())
    timestamp_idx = min(range(len(timestamps)), key=lambda i: abs(timestamps[i] - current_timestamp))
    closest_timestamp = timestamps[timestamp_idx]
    return timestamp_to_line[closest_timestamp]

def check_and_start_test():
    global start_time_offset
    global start_event
    global args
    if all(init_flags.values()):
        if args.start_time is not None:
            current_wall_time_ms = int(time.time() * 1000)
            current_time_in_minute = current_wall_time_ms % (60 * 1000)
            start_time_offset = (args.start_time - current_time_in_minute) % (60 * 1000)
            print(f"Start time offset recalculated: {start_time_offset} ms")
        else:
            start_time_offset = 0
            print("No start_time_option specified, synchronizing with wall time.")

        start_event.set()

        test_process = Process(target=auto_test)
        test_process.start()

def update_lines_based_on_wall_time(update_event):
    global start_time_offset
    global start_event
    
    start_event.wait()
    next_update_time = ((int(time.time() * 1000) // 100) + 1) * 100
    last_update_time = None

    while True:
        current_time_ms = int(time.time() * 1000)
        if current_time_ms >= next_update_time:
            if last_update_time is None or current_time_ms - last_update_time >= 100:
                timestamp_5g.value += 100
                timestamp_starlink.value += 100
                update_event.set()
                last_update_time = current_time_ms
                next_update_time = ((current_time_ms // 100) + 1) * 100
            else:
                missed_intervals = (current_time_ms - next_update_time) // 100
                next_update_time += (missed_intervals + 1) * 100
        else:
            time.sleep(0.001)

if '__main__' == __name__:
    parser = argparse.ArgumentParser(description='Network Emulator')
    parser.add_argument('--start_time', type=int, default=None, help='Starting timestamp in milliseconds (e.g., --start_time=23100)')
    args = parser.parse_args()

    data_files = {
        '5G': './5G.csv',
        'Starlink': './lagos.csv'
    }

    with open(data_files['5G'], 'r') as file:
        reader = csv.reader(file)
        latency_lines_5g = [line for line in reader if line]
    timestamp_index_5g = {int(line[-1]) for line in latency_lines_5g}

    with open(data_files['Starlink'], 'r') as file:
        reader = csv.reader(file)
        latency_lines_starlink = [line for line in reader if line]
    timestamp_index_starlink = {int(line[-1]) for line in latency_lines_starlink}

    if args.start_time is not None:
        start_time_option = args.start_time
        if start_time_option % 100 != 0:
            print(f"Error: The start timestamp {start_time_option} must be a multiple of 100 ms.")
            exit(1)
        timestamp_5g.value += start_time_option
        timestamp_starlink.value += start_time_option
        
    else:
        start_time_option = None

    setLogLevel('info')
    net = Mininet(link=TCLink)

    h1 = net.addHost('h1')
    h2 = net.addHost('h2')
    r1 = net.addHost('r1')
    r2 = net.addHost('r2')
    r3 = net.addHost('r3')
    r4 = net.addHost('r4')
    r5 = net.addHost('r5')

    net.addLink(r1, h1, cls=TCLink)
    net.addLink(r1, r4, cls=TCLink)
    net.addLink(r1, r5, cls=TCLink)
    net.addLink(r4, r2, cls=TCLink)
    net.addLink(r5, r3, cls=TCLink)
    net.addLink(r2, h2, cls=TCLink)
    net.addLink(r3, h2, cls=TCLink)
    net.build()

    r1.cmd("ifconfig r1-eth0 0")
    r1.cmd("ifconfig r1-eth1 0")
    r1.cmd("ifconfig r1-eth2 0")

    r1.cmd("echo 1 > /proc/sys/net/ipv4/ip_forward")
    r1.cmd("echo 1 > /proc/sys/net/ipv4/conf/all/proxy_arp")

    r1.cmd("ifconfig r1-eth0 10.0.1.1 netmask 255.255.255.0")
    r1.cmd("ifconfig r1-eth1 10.0.2.1 netmask 255.255.255.0")
    r1.cmd("ifconfig r1-eth2 10.0.3.1 netmask 255.255.255.0")

    r4.cmd("ifconfig r4-eth0 0")
    r4.cmd("ifconfig r4-eth1 0")

    r4.cmd("echo 1 > /proc/sys/net/ipv4/ip_forward")
    r4.cmd("echo 1 > /proc/sys/net/ipv4/conf/all/proxy_arp")

    r4.cmd("ifconfig r4-eth0 10.0.2.4 netmask 255.255.255.0")
    r4.cmd("ifconfig r4-eth1 10.0.6.4 netmask 255.255.255.0")

    r5.cmd("ifconfig r5-eth0 0")
    r5.cmd("ifconfig r5-eth1 0")

    r5.cmd("echo 1 > /proc/sys/net/ipv4/ip_forward")
    r5.cmd("echo 1 > /proc/sys/net/ipv4/conf/all/proxy_arp")

    r5.cmd("ifconfig r5-eth0 10.0.3.4 netmask 255.255.255.0")
    r5.cmd("ifconfig r5-eth1 10.0.7.4 netmask 255.255.255.0")

    r2.cmd("ifconfig r2-eth0 0")
    r2.cmd("ifconfig r2-eth1 0")

    r2.cmd("echo 1 > /proc/sys/net/ipv4/ip_forward")
    r2.cmd("echo 1 > /proc/sys/net/ipv4/conf/all/proxy_arp")

    r2.cmd("ifconfig r2-eth0 10.0.6.2 netmask 255.255.255.0")
    r2.cmd("ifconfig r2-eth1 10.0.4.2 netmask 255.255.255.0")

    r3.cmd("ifconfig r3-eth0 0")
    r3.cmd("ifconfig r3-eth1 0")

    r3.cmd("echo 1 > /proc/sys/net/ipv4/ip_forward")
    r3.cmd("echo 1 > /proc/sys/net/ipv4/conf/all/proxy_arp")

    r3.cmd("ifconfig r3-eth0 10.0.7.2 netmask 255.255.255.0")
    r3.cmd("ifconfig r3-eth1 10.0.5.2 netmask 255.255.255.0")

    r2.cmd("ip route add 10.0.1.0/24 via 10.0.6.4")
    r3.cmd("ip route add 10.0.1.0/24 via 10.0.7.4")

    r1.cmd("ip route add 10.0.4.0/24 via 10.0.2.4")
    r1.cmd("ip route add 10.0.5.0/24 via 10.0.3.4")

    r4.cmd("ip route add 10.0.1.0/24 via 10.0.2.1")
    r5.cmd("ip route add 10.0.1.0/24 via 10.0.3.1")
    r4.cmd("ip route add 10.0.4.0/24 via 10.0.6.2")
    r5.cmd("ip route add 10.0.5.0/24 via 10.0.7.2")

    h1.cmd("ifconfig h1-eth0 0")

    h2.cmd("ifconfig h2-eth0 0")
    h2.cmd("ifconfig h2-eth1 0")

    h1.cmd("ifconfig h1-eth0 10.0.1.2 netmask 255.255.255.0")

    h2.cmd("ifconfig h2-eth0 10.0.4.3 netmask 255.255.255.0")
    h2.cmd("ifconfig h2-eth1 10.0.5.3 netmask 255.255.255.0")

    h1.cmd("ip route add default scope global nexthop via 10.0.1.1 dev h1-eth0")

    h2.cmd("ip rule add from 10.0.4.3 table 1")
    h2.cmd("ip rule add from 10.0.5.3 table 2")

    h2.cmd("ip route add 10.0.6.0/24 dev h2-eth0 table 1")
    h2.cmd("ip route add 10.0.4.0/24 dev h2-eth0 table 1")
    h2.cmd("ip route add 10.0.2.0/24 dev h2-eth0 table 1")
    h2.cmd("ip route add 10.0.1.0/24 dev h2-eth0 table 1")

    h2.cmd("ip route add 10.0.7.0/24 dev h2-eth1 table 2")
    h2.cmd("ip route add 10.0.5.0/24 dev h2-eth1 table 2")
    h2.cmd("ip route add 10.0.3.0/24 dev h2-eth1 table 2")
    h2.cmd("ip route add 10.0.1.0/24 dev h2-eth1 table 2")

    h2.cmd("ip route add default scope global nexthop via 10.0.4.2 dev h2-eth0")

    network_thread1 = NetworkConfigThread(net, 'r3', 'r3-eth1', 3, barrier, timestamp_5g, update_event, data_files['5G'])
    network_thread3 = NetworkConfigThread(net, 'r5', 'r5-eth0', 2, barrier, timestamp_5g, update_event, data_files['5G'])

    network_thread2 = NetworkConfigThread(net, 'r2', 'r2-eth1', 3, barrier, timestamp_starlink, update_event, data_files['Starlink'])
    network_thread4 = NetworkConfigThread(net, 'r4', 'r4-eth0', 2, barrier, timestamp_starlink, update_event, data_files['Starlink'])

    network_thread1.start()
    network_thread3.start()
    network_thread2.start()
    network_thread4.start()

    update_thread = threading.Thread(
        target=update_lines_based_on_wall_time,
        args=(update_event,)
    )
    update_thread.start()
