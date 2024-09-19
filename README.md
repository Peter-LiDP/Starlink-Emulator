# Starlink-emulator

## Data Processing Steps:
1. Move iRTT JSON files into a dedicated folder. Then run the `process_irtt.py` script, adjusting the folder path accordingly:

    ```bash
    python process_irtt.py
    ```

2. Move iPerf3 JSON files into a dedicated folder. Then run the `process_iperf3.py` script, adjusting the folder path accordingly:

    ```bash
    python process_iperf3.py
    ```

3. After processing both datasets, combine the iRTT and iPerf3 CSV files into one file by running:

    ```bash
    python combine.py
    ```

4. Once the files are combined, run the `data_process.py` script to complete the data processing:

    ```bash
    python data_process.py
    ```

## Emulator Usage:

### Starlink and Custom Trace Emulation
The emulator includes two paths, with Starlink as the default path. You can also replace the Starlink trace (`./lagos.csv`) with any trace file you wish to emulate (after adjusting it to the correct format by finishing the data processing scripts).

### Wall Time Emulation
To emulate a handover pattern at specific seconds of the minute (e.g., at 12, 27, 42, and 57 seconds), run:

    ```bash
    sudo python emulator.py
    ```

### Customized Start Time Emulation
If you prefer to specify a start time for the emulation (in 100 milliseconds precision), use the `--start_time` flag. For example, to start the emulation at 23.1 seconds:

    ```bash
    sudo python emulator.py --start_time=23100
    ```

