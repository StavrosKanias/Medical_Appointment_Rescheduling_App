import plotly.graph_objects as go
import numpy as np
from math import log
requests = [200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800]

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=requests,
    y=[log(t) for t in [0.15, 0.14, 0.31, 0.4, 0.32, 2.47, 0.625, 0.96, 1.82, 0.89, 0.92, 0.84, 24.65, 1.03, 844.7,1254,3, 32]],
    marker=dict(color="red", size=12),
    mode="markers+lines",
    name="Split",
))

fig.add_trace(go.Scatter(
    x=requests,
    y=[log(t) for t in[0.07, 0.06 , 0.0625, 0.11, 0.18, 1.52, 0.156, 0.25, 1.09, 0.25, 0.28, 0.25, 3.06, 0.32, 936.2, 1463.7, 46.06]],
    marker=dict(color="blue", size=12),
    mode="markers+lines",
    name="Merged",
)) 

fig.update_layout(title="Rescheduling after cancelation",
                  xaxis_title="Requests",
                  yaxis_title="CPU time (log(s))")

fig.show()