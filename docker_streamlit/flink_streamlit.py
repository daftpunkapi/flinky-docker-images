# from kafka import KafkaConsumer
# import streamlit as st
# import numpy as np
# import pandas as pd

# # # consumer = KafkaConsumer('stream1ws')
# # consumer = KafkaConsumer('enriched', bootstrap_servers=['localhost:9092'],
# # auto_offset_reset='earliest', enable_auto_commit=False,
# # auto_commit_interval_ms=1000)

# chart_data = pd.DataFrame(
#      np.random.randn(20, 3),
#      columns=['a', 'b', 'c'])

# st.line_chart(chart_data)


from kafka import KafkaConsumer
import streamlit as st
import pandas as pd
import json

consumer = KafkaConsumer(
    'enriched',
    bootstrap_servers=['broker:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    auto_commit_interval_ms=1000,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

chart_data = pd.DataFrame(columns=['window_start_ws', 'result'])
chart = st.line_chart(chart_data, x='window_start_ws', y='result', use_container_width=True)

for message in consumer:
    value = message.value
    window_start_ws = value['window_start_ws']
    result = value['result']
    chart_data = chart_data.append({'window_start_ws': window_start_ws, 'result': result}, ignore_index=True)
    latest_data = chart_data.tail(20)

    # Clear the previous chart and update it with the latest data
    chart.empty()
    chart = st.line_chart(latest_data, x='window_start_ws', y='result', use_container_width=True)

    if chart_data.shape[0] == 1:
        print(chart_data)
    else:
        print(chart_data.tail(1))  # print the last row added to the DataFrame
