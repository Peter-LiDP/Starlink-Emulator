import time
import csv
import threading
import sched
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Mininet
from multiprocessing import Process, Value, Lock
from mininet.cli import CLI

iface1 = "r2-eth0"
init_flags = {
    'r3-eth1': False,
    'r5-eth0': False,
    'r2-eth1': False,
    'r4-eth0': False
}

barrier = threading.Barrier(4)
line_number_5g = Value('i', 0)
line_number_starlink = Value('i', 0)
line_lock = Lock()
update_event = threading.Event()
file_counter = Value('i', 0)

def auto_test():
    for i in range(50):
        h1.sendCmd('xterm -title "node: h1 server" -hold -e "./picoquicdemo -M 1 -p 4434 -G bbr1 -q ./server_qlog -w ./sample/" &')
        h2.sendCmd('xterm -title "node: h2 client" -hold -e "./picoquicdemo -n test -M 1 -A 10.0.5.3/3 -G bbr1 -q ./client_qlog -o /usr 10.0.1.2 4434 /testfile_1" &')
        time.sleep(145)
        h1.sendInt()
        h2.sendInt()
        h1.waitOutput()
        h2.waitOutput()
        time.sleep(5)
        if i == 49:
            print(f"line_number_5g.value: {line_number_5g.value}")
            print(f"line_number_starlink.value: {line_number_starlink.value}")

class NetworkConfigThread(threading.Thread):
    def __init__(self, net, host_name, dev, column, barrier, line_number, line_lock, update_event):
        super().__init__()
        self.net = net
        self.host_name = host_name
        self.column = column
        self.dev = dev
        self.barrier = barrier
        self.line_number = line_number
        self.line_lock = line_lock
        self.update_event = update_event

    def run(self):
        configureNetworkConditions(self.net, self.host_name, self.dev, self.column, self.barrier, self.line_number, self.line_lock, self.update_event)

class NetworkConfigThread_Starlink(threading.Thread):
    def __init__(self, net, host_name, dev, column, barrier, line_number, line_lock, update_event):
        super().__init__()
        self.net = net
        self.host_name = host_name
        self.dev = dev
        self.column = column
        self.barrier = barrier
        self.line_number = line_number
        self.line_lock = line_lock
        self.update_event = update_event

    def run(self):
        configureNetworkConditions_starlink(self.net, self.host_name, self.dev, self.column, self.barrier, self.line_number, self.line_lock, self.update_event)

def configureNetworkConditions_starlink(net, host_name, dev, column, barrier, line_number, line_lock, update_event):
    global init_flags

    with open('./lagos.csv', 'r') as file:
        reader = csv.reader(file)
        latency_lines = list(reader)

    host = net.get(host_name)

    with line_lock:
        initialBW = float(latency_lines[line_number.value][column - 2])
        cmd_bw = 'tc qdisc replace dev {} root handle 1: tbf rate {}mbit burst 15k latency 50ms'.format(dev, initialBW)
        host.cmd(cmd_bw)

        initialDelay = float(latency_lines[line_number.value][column])
        cmd_jitter = 'tc qdisc add dev {} parent 1:1 handle 10: netem delay {}ms loss 2%'.format(dev, initialDelay)
        host.cmd(cmd_jitter)

        if not init_flags[dev]:
            init_flags[dev] = True
            check_and_start_test()

    barrier.wait()

    while True:
        update_event.wait()
        update_event.clear()

        with line_lock:
            current_line_number = line_number.value
            currentBW = float(latency_lines[current_line_number][column - 2])
            update_cmd_bw = 'tc qdisc change dev {} root handle 1: tbf rate {}mbit burst 15k latency 50ms'.format(dev, currentBW)
            host.cmd(update_cmd_bw)

            currentDelay = float(latency_lines[current_line_number][column])
            update_cmd = 'tc qdisc change dev {} parent 1:1 handle 10: netem delay {}ms loss 2%'.format(dev, currentDelay)
            host.cmd(update_cmd)

        barrier.wait()

def configureNetworkConditions(net, host_name, dev, column, barrier, line_number, line_lock, update_event):
    global init_flags

    with open('./5G.csv', 'r') as file:
        reader = csv.reader(file)
        latency_lines = list(reader)

    host = net.get(host_name)

    with line_lock:
        initialBW = float(latency_lines[line_number.value][column - 2])
        cmd_bw = 'tc qdisc replace dev {} root handle 1: tbf rate {}mbit burst 15k latency 50ms'.format(dev, initialBW)
        host.cmd(cmd_bw)

        initialDelay = float(latency_lines[line_number.value][column])
        cmd_jitter = 'tc qdisc add dev {} parent 1:1 handle 10: netem delay {}ms loss 1%'.format(dev, initialDelay)
        host.cmd(cmd_jitter)

        if not init_flags[dev]:
            init_flags[dev] = True
            check_and_start_test()

    barrier.wait()

    while True:
        update_event.wait()
        update_event.clear()

        with line_lock:
            current_line_number = line_number.value
            currentBW = float(latency_lines[current_line_number][column - 2])
            update_cmd_bw = 'tc qdisc change dev {} root handle 1: tbf rate {}mbit burst 15k latency 50ms'.format(dev, currentBW)
            host.cmd(update_cmd_bw)

            currentDelay = float(latency_lines[current_line_number][column])
            update_cmd = 'tc qdisc change dev {} parent 1:1 handle 10: netem delay {}ms loss 1%'.format(dev, currentDelay)
            host.cmd(update_cmd)
        
        barrier.wait()

def check_and_start_test():
    if all(init_flags.values()):
        test_process = Process(target=auto_test)
        test_process.start()

def update_lines_periodically(scheduler, step, start_time):
    global line_number_5g, line_number_starlink
    with line_lock:
        line_number_5g.value = (line_number_5g.value + 1) % len(open('./5G.csv').readlines())
        line_number_starlink.value = (line_number_starlink.value + 1) % len(open('./lagos.csv').readlines())
    update_event.set()

    next_time = start_time + step
    current_time = time.perf_counter()
    sleep_time = next_time - current_time

    if sleep_time > 0:
        scheduler.enter(sleep_time, 1, update_lines_periodically, (scheduler, step, next_time))
    else:
        scheduler.enter(0, 1, update_lines_periodically, (scheduler, step, time.perf_counter()))


if '__main__' == __name__:
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

    h2.cmd('xterm -title "node: h2 monitoring" -hold -e "sudo bwm-ng" &')

    network_thread1 = NetworkConfigThread(net, 'r3', 'r3-eth1', 2, barrier, line_number_5g, line_lock, update_event)
    network_thread3 = NetworkConfigThread(net, 'r5', 'r5-eth0', 3, barrier, line_number_5g, line_lock, update_event)

    network_thread2 = NetworkConfigThread_Starlink(net, 'r2', 'r2-eth1', 3, barrier, line_number_starlink, line_lock, update_event)
    network_thread4 = NetworkConfigThread_Starlink(net, 'r4', 'r4-eth0', 2, barrier, line_number_starlink, line_lock, update_event)

    network_thread1.start() 
    network_thread3.start()
    network_thread2.start()
    network_thread4.start()

    scheduler = sched.scheduler(time.time, time.sleep)
    start_time = time.perf_counter()
    scheduler.enter(0.1, 1, update_lines_periodically, (scheduler, 0.1, start_time))
    update_thread = threading.Thread(target=scheduler.run)
    update_thread.start()

    CLI(net)
    net.stop()
