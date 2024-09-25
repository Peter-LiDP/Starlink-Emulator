import pandas as pd
import numpy as np

df = pd.read_csv('path/to/combined_output_data.csv', sep=',')

df['timestamp'] = pd.to_datetime(df['wall_time_ns'], unit='ns')

def get_seconds_in_minute(dt_series):
    seconds_since_minute = (dt_series - dt_series.dt.floor('min')).dt.total_seconds()
    return seconds_since_minute

df['seconds_in_minute'] = get_seconds_in_minute(df['timestamp'])

condition = (df['seconds_in_minute'] >= 12.0) & (df['seconds_in_minute'] <= 56.9)
df_filtered = df[condition].reset_index(drop=True)

data_columns = ['uplink_throughput_Mbps', 'downlink_throughput_Mbps',
                'uplink_delay_ms', 'downlink_delay_ms']
df_filtered[data_columns] = df_filtered[data_columns].replace([np.inf, -np.inf], np.nan)
df_filtered['is_valid'] = df_filtered[data_columns].gt(0).all(axis=1) & df_filtered[data_columns].notnull().all(axis=1)

total_missing_before = 0
missing_timestamps_total_before = []

def process_chunk(df_chunk, expected_timestamps, data_columns, chunk_info):
    df_chunk_full = pd.DataFrame({'timestamp': expected_timestamps})
    df_chunk_full = df_chunk_full.merge(df_chunk, on='timestamp', how='left', suffixes=('', '_orig'))
    df_chunk_full['is_missing'] = ~df_chunk_full['is_valid'].fillna(False)

    num_missing = df_chunk_full['is_missing'].sum()
    total_missing_before_chunk[0] += num_missing
    if num_missing > 0:
        missing_timestamps = df_chunk_full[df_chunk_full['is_missing']]['timestamp']
        missing_timestamps_total_before.extend(missing_timestamps.tolist())
        print(f"{chunk_info} has {num_missing} missing data points before processing.")

    if num_missing > 10:
        print(f"{chunk_info} is removed because it has more than 10 missing data points ({num_missing} missing).")
        return pd.DataFrame()
    else:
        averages = df_chunk_full.loc[~df_chunk_full['is_missing'], data_columns].mean()
        for col in data_columns:
            df_chunk_full[col] = df_chunk_full[col].fillna(averages[col])
        df_chunk_full['is_valid'] = True
        chunk_averages = df_chunk_full[data_columns].mean()
        if (chunk_averages < 2).any():
            print(f"{chunk_info} is removed because average of one or more columns is less than 2.")
            return pd.DataFrame()
        else:
            return df_chunk_full[['timestamp'] + data_columns + ['is_valid']]

processed_chunks = []
unique_minutes = df_filtered['timestamp'].dt.floor('min').unique()

total_expected = 0
total_available = 0
total_missing_before_chunk = [0]

for minute in unique_minutes:
    minute_mask = df_filtered['timestamp'].dt.floor('min') == minute
    df_minute = df_filtered.loc[minute_mask].copy()
    df_minute['seconds_in_minute'] = get_seconds_in_minute(df_minute['timestamp'])

    chunks = [
        (12.0, 26.9, 'Chunk 1'),
        (27.0, 41.9, 'Chunk 2'),
        (42.0, 56.9, 'Chunk 3')
    ]

    for chunk_start, chunk_end, chunk_name in chunks:
        chunk_start_time = minute + np.timedelta64(int(chunk_start * 1e9), 'ns')
        chunk_end_time = minute + np.timedelta64(int(chunk_end * 1e9), 'ns')
        expected_timestamps = pd.date_range(start=chunk_start_time, end=chunk_end_time, freq='100ms')

        chunk_mask = (df_minute['seconds_in_minute'] >= chunk_start) & (df_minute['seconds_in_minute'] <= chunk_end)
        df_chunk = df_minute.loc[chunk_mask, ['timestamp'] + data_columns + ['is_valid']]

        total_expected += len(expected_timestamps)
        total_available += len(df_chunk)

        chunk_info = f"{chunk_name} of minute {pd.Timestamp(minute).strftime('%Y-%m-%d %H:%M')}"
        df_chunk_processed = process_chunk(df_chunk, expected_timestamps, data_columns, chunk_info)

        if not df_chunk_processed.empty:
            processed_chunks.append(df_chunk_processed)
        else:
            pass

total_missing_before = total_missing_before_chunk[0]
print(f"\nTotal expected data points before processing: {total_expected}")
print(f"Total available data points before processing: {total_available}")
print(f"Total missing data points before processing: {total_missing_before}")

if total_missing_before > 0:
    print("\nExamples of missing data points before processing:")
    for ts in missing_timestamps_total_before[:10]:
        print(ts)
else:
    print("\nNo missing data points were found before processing.")

if processed_chunks:
    df_final = pd.concat(processed_chunks, ignore_index=True)
else:
    df_final = pd.DataFrame(columns=['timestamp'] + data_columns + ['is_valid'])

df_final['seconds_in_minute'] = get_seconds_in_minute(df_final['timestamp'])
missing_final = df_final['is_valid'] == False
total_missing_after = missing_final.sum()
total_data_points = len(df_final)

print(f"\nTotal data points after processing: {total_data_points}")
print(f"Total missing data points after processing: {total_missing_after}")

if total_missing_after == 0:
    print("All missing data have been filled or problematic chunks removed.")
else:
    print(f"There are still {total_missing_after} missing data points after processing.")

filled_data = df_final[df_final['is_valid'] & ~df_final['timestamp'].isin(df_filtered['timestamp'])]
if not filled_data.empty:
    print("\nExamples of newly generated data (filled missing values):")
    print(filled_data.head())
else:
    print("\nNo new data was generated by filling missing values.")

df_final = df_final.iloc[30:-120].reset_index(drop=True)

df_final = df_final[['uplink_throughput_Mbps', 'downlink_throughput_Mbps',
                     'uplink_delay_ms', 'downlink_delay_ms', 'timestamp']]

num_rows = len(df_final)
df_final['wall_time'] = np.arange(0, num_rows * 100, 100)
df_final.drop(columns=['timestamp'], inplace=True)
df_final = df_final[['uplink_throughput_Mbps', 'downlink_throughput_Mbps',
                     'uplink_delay_ms', 'downlink_delay_ms', 'wall_time']]

df_final.to_csv('path/you/want/to/save/the/final/trace.csv', index=False, header=False)

print("\nExamples of the newly organized data:")
print(df_final.head())
