import json
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import streamlit as st
from PIL import Image

ORIG_DIR = "original_data"
PROCESSED_DIR = "processed_data"

st.set_page_config(
    page_title="COVID-19 Malaysia",
    page_icon="🦠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Add title, descriptions and image
st.title('COVID-19 Malaysia')
st.markdown('''
**Description**

All the data is scraped from the news articles from https://kpkesihatan.com/
by using the `BeautifulSoup` web scraping library.
The data consists of the number of cases from 27 March, 2020 until 15 April, 2021.
This app is intended to display some **visualizations** for the COVID-19 cases in Malaysia.

- App built by [Anson](https://www.linkedin.com/in/ansonnn07/)
- Built with `Python`, using `streamlit`, `BeautifulSoup`, `pandas`, `numpy`, `plotly`

**Links to my profiles**: GitHub TO INSERT LINK, 
[LinkedIn](https://www.linkedin.com/in/ansonnn07/),
[Kaggle](https://www.kaggle.com/ansonnn/code)
''')
st.markdown("""
**Tips about the figures**:
All the figures are ***interactive***. You can zoom in by dragging in the figure, and reset the axis by double-clicking. The legends can be clicked to disable or enable specific legends.
""")

st.markdown('---')
image = Image.open('covid19.jpg')
st.image(image, use_column_width=True)
st.markdown("""
Image source: [COVID-19](https://www.ei-ie.org/en/detail/16723/covid-19-educators-call-for-global-solidarity-and-a-human-centred-approach-to-the-crisis)
""")


@st.cache
def read_all_csv():
    df = pd.read_csv(f"{PROCESSED_DIR}//cleaned_all.csv")
    df.Date = pd.to_datetime(df.Date, format='%Y-%m-%d')
    df.set_index('Date', inplace=True)

    df_m = pd.read_csv(f'{PROCESSED_DIR}//monthly_sum.csv')
    df_m.Date = pd.to_datetime(df_m.Date, format='%Y-%m-%d')
    df_m.set_index('Date', inplace=True)

    dfState = pd.read_csv(f'{PROCESSED_DIR}//state_all.csv')
    dfStateCumu = pd.read_csv(f'{PROCESSED_DIR}//state_cumu.csv')

    dfState.Date = pd.to_datetime(dfState.Date)
    dfStateCumu.Date = pd.to_datetime(dfStateCumu.Date)

    dfState.set_index('Date', inplace=True)
    dfStateCumu.set_index('Date', inplace=True)
    return df, df_m, dfState, dfStateCumu


def read_map():
    # https://www.igismap.com/download-malaysia-shapefile-area-map-free-country-boundary-state-polygon/
    with open('original_data//malaysia_state_province_boundary.geojson', 'r') as f:
        msia_geojson = json.load(f)
    return msia_geojson


def get_df_state():
    state_id_dict = {}
    for i, feature in enumerate(msia_geojson['features']):
        feature['id'] = i
        state_id_dict[feature['properties']['locname']] = i

    df_state_total = dfStateCumu.iloc[[-1]].T.reset_index()
    df_state_total.columns = ['State', 'Confirmed']
    df_state_total['State_spaced'] = df_state_total['State'] + '  '

    correct_state_id = {}
    for stateName in df_state_total.State:
        name_to_search = stateName.replace('WP ', '')
        for k, v in state_id_dict.items():
            if name_to_search.lower() in k.lower():
                correct_state_id[stateName] = int(v)

    df_state_total['id'] = df_state_total.State.map(correct_state_id)

    return df_state_total, correct_state_id


df, df_m, dfState, dfStateCumu = read_all_csv()
msia_geojson = read_map()
df_state_total, correct_state_id = get_df_state()
# with st.spinner("[INFO] Loading necessary files ..."):

max_row = df.loc[df['SMA_new'] == df['SMA_new'].max()]
last_row = df.iloc[-1]
last_date = last_row.name.strftime("%b %d, %Y")
pct_vs_peak = round(
    (last_row['SMA_new'] / max_row['SMA_new'].values[0] * 100), 1)

display_one = None
all_data_checkbox = None
monthly_checkbox = None
state_checkbox = None
map_checkbox = None
animated_checkbox = None

st.sidebar.header("Display Options:")
display_type = st.sidebar.radio("Select your display type:",
                                ("Display only one type",
                                 "Display multiple types"),
                                index=1)

if display_type == "Display only one type":
    display_one = st.sidebar.radio("Select figures to display:",
                                   ("Show by Daily Cases",
                                    "Show by Monthly Cases",
                                    "Show by State Cases",
                                    "Show Choropleth Map",
                                    "Show Animated Map!"))

else:
    st.sidebar.markdown("Select figures to display:")
    all_data_checkbox = st.sidebar.checkbox("Show by Daily Cases", value=True)
    monthly_checkbox = st.sidebar.checkbox("Show by Monthly Cases", value=True)
    state_checkbox = st.sidebar.checkbox("Show by State Cases", value=True)
    map_checkbox = st.sidebar.checkbox("Show Choropleth Map", value=False)
    st.sidebar.info("""The animated map takes awhile to load, 
    and will show on a new tab.""")
    animated_checkbox = st.sidebar.checkbox("Show Animated Map!", value=False)

if (display_one == "Show by Daily Cases") or all_data_checkbox:
    st.markdown("---")
    st.markdown("# Daily Cases")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df['New Case'],
                             line=dict(color='teal'),
                             name='Confirmed'))
    fig.add_trace(go.Scatter(x=df.index, y=df['Recovered'],
                             line=dict(color='royalblue'),
                             name='Recovered'))
    fig.add_trace(go.Scatter(x=df.index, y=df['Death'],
                             line=dict(color='coral'),
                             name='Death'))
    fig.update_layout(title='COVID-19 Malaysia: Daily Cases',
                      height=600,
                      # hovermode="x unified",
                      xaxis_title=None, yaxis_title=None,
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.01
                      ))
    fig.update_layout(
        hovermode="x",
        hoverdistance=100,  # Distance to show hover label of data point
        spikedistance=1000,  # Distance to show spike
        xaxis=dict(
            # linecolor="#BCCCDC",
            showspikes=True,  # Show spike line for X-axis
            # Format spike
            spikethickness=2,
            spikedash="dot",
            spikecolor="#000000",
            spikemode="across",
        )
    )
    fig.update_xaxes(rangeslider_visible=True)
    st.plotly_chart(fig, use_container_width=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df['Cumulative Case'],
                             line=dict(color='teal'),
                             name='Confirmed'))
    fig.add_trace(go.Scatter(x=df.index, y=df['Cumulative Recovered'],
                             line=dict(color='royalblue'),
                             name='Recovered'))
    fig.add_trace(go.Scatter(x=df.index, y=df['Cumulative Death'],
                             line=dict(color='coral'),
                             name='Death'))
    fig.update_layout(title='COVID-19 Malaysia: Cumulative Cases',
                      height=600,
                      #   hovermode="x unified",
                      xaxis_title=None, yaxis_title='Log Scale',
                      hovermode="x",
                      hoverdistance=100,  # Distance to show hover label of data point
                      spikedistance=1000,
                      xaxis=dict(
                          showspikes=True,  # Show spike line for X-axis
                          # Format spike
                          spikethickness=2,
                          spikedash="dot",
                          spikecolor="#000000",
                          spikemode="across",
                      ),
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.01
                      ))
    fig.update_yaxes(type='log')
    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False})

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df['New Case'],
                             # marker_color='royalblue',
                             line=dict(color='royalblue'),
                             name='New Case'))
    fig.add_trace(go.Scatter(x=df.index, y=df['SMA_new'],
                             line=dict(color='coral'),
                             name='Average'))
    fig.update_layout(title='COVID-19 Malaysia: New Case VS 7-day Moving Average',
                      xaxis_title=None, yaxis_title=None)
    fig.update_layout(hovermode="x unified",
                      height=600,
                      xaxis=dict(
                          # Format spike
                          spikethickness=2,
                          spikecolor="#000000",
                      ),
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.01
                      ))
    fig.add_annotation(x=str(max_row.index.values[0]), y=int(max_row['SMA_new'].values[0]),
                       text=f"Highest average on {max_row.index.date[0]}"
                       f": {int(max_row['SMA_new'].values[0])}",
                       xref="x",
                       yref="y",
                       showarrow=True,
                       font=dict(
        family="Courier New, monospace",
        size=16,
        color="#ffffff"
    ),
        align="center",
        xanchor='right',
        arrowhead=1,
        arrowsize=1,
        arrowwidth=2,
        arrowcolor="#636363",
        ax=-20,
        ay=15,
        bordercolor="#c7c7c7",
        borderwidth=2,
        borderpad=4,
        bgcolor="brown",
        standoff=2,
        opacity=0.8)
    fig.add_annotation(x=last_row.name, y=int(last_row['SMA_new']),
                       text=f"Latest: {int(last_row['SMA_new'])}; "
                       f"{pct_vs_peak}% of the peak average",
                       xref="x",
                       yref="y",
                       showarrow=True,
                       font=dict(
        family="Courier New, monospace",
        size=16,
        color="#ffffff"
    ),
        align="center",
        xanchor='right',
        arrowhead=1,
        arrowsize=1,
        arrowwidth=2,
        arrowcolor="#636363",
        ax=-25,
        ay=-20,
        bordercolor="#c7c7c7",
        borderwidth=2,
        borderpad=4,
        bgcolor="brown",
        standoff=2,
        opacity=0.8)
    fig.update_xaxes(rangeslider_visible=True)
    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False})

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df['Death'],
                             line=dict(color='royalblue'),
                             name='Death'))
    fig.add_trace(go.Scatter(x=df.index, y=df['SMA_death'],
                             line=dict(color='coral'),
                             name='Average'))
    fig.update_layout(
        title='COVID-19 Malaysia: Daily Death VS 7-day Moving Average')
    fig.update_layout(hovermode="x unified",
                      xaxis=dict(
                          # Format spike
                          spikethickness=2,
                          spikecolor="#000000",
                      ),
                      legend=dict(
                          yanchor="top",
                          y=1.0,
                          xanchor="left",
                          x=0.01
                      ))
    # fig.update_xaxes(rangeslider_visible=True)
    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False})


def style_df(df, axis=0):
    df_copy = df.copy()
    df_copy.index = df_copy.index.strftime('%b %Y')
    df_copy = df_copy.style.background_gradient(cmap='Purples', axis=axis)
    return df_copy


# MONTHLY DATA
if (display_one == "Show by Monthly Cases") or monthly_checkbox:
    st.markdown("""
    ---

    # Monthly Cases
    """)
    df_m_style = style_df(df_m)
    st.dataframe(df_m_style, height=1200)

    def plot_bar(y):
        fig = px.bar(df_m, x=df_m.index, y=y, text=y, color=y,
                     title=f'COVID-19 Malaysia: Monthly {y}',
                     color_continuous_scale='Purp')
        fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
        fig.update_traces(texttemplate='%{text:,}')
        fig.update_layout(xaxis_title=None, yaxis_title=None,
                          uniformtext_minsize=8, uniformtext_mode='hide',
                          coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True,
                        config={"displayModeBar": False})

    plot_bar('New Case')
    plot_bar('Recovered')
    plot_bar('Death')

    long_df_m = pd.melt(df_m[['New Case', 'Recovered', 'Death']],
                        var_name='Case', value_name='Number',
                        ignore_index=False).reset_index()
    long_df_m['log_number'] = np.log10(long_df_m['Number']).round(4)

    fig = px.bar(long_df_m, x='Date',
                 y='Number', color='Case',
                 text='Number',
                 # hover_name='Case',
                 # hover_data={'Number': True, 'Case': False,
                 #             'log_number': False, 'Date': False},
                 color_discrete_sequence=['rebeccapurple',
                                          'teal',
                                          'coral']
                 )
    fig.update_traces(texttemplate='%{text:,}', hovertemplate='<b>%{y:,}</b>')
    fig.update_layout(title_text='COVID-19 Malaysia: Monthly Cases',
                      xaxis_title=None, yaxis_title='Log Scale',
                      uniformtext_minsize=10, barmode='group',
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.01
                      )
                      # hovermode="x unified"
                      )
    fig.update_yaxes(type='log')
    fig.update_xaxes(dtick="M1", tickformat="%b\n%Y")
    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False})

# STATE DATA
if (display_one == "Show by State Cases") or state_checkbox:
    st.markdown("""
    ---

    # State Cases
    """)

    # st.dataframe(df_state_total)

    fig = go.Figure()
    for col in dfState.columns:
        fig.add_trace(go.Scatter(x=dfState.index,
                                 y=dfState[col],
                                 name=col,
                                 visible=True
                                 )
                      )
        fig.update_layout(
            title='COVID-19 Malaysia: Daily Cases by State', height=600)
        fig.add_annotation(xref='paper',
                           yref='paper',
                           x=1, y=1.09,
                           showarrow=False,
                           font=dict(
                               # family="Courier New, monospace",
                               size=12,
                               color="royalblue"
                           ),
                           text='Tip: Double click a legend to isolate only the state')
    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False})

    fig = px.bar(df_state_total.sort_values('Confirmed'), x='Confirmed',
                 y='State_spaced', text='Confirmed',
                 color='Confirmed',
                 color_continuous_scale='Purp',
                 hover_name='State',
                 hover_data={'State_spaced': False, 'Confirmed': False}
                 )
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',
                      title=f'COVID-19 Malaysia: Total Cases as of {last_date}',
                      width=700, height=800,
                      xaxis_title=None, yaxis_title=None,
                      showlegend=False, coloraxis_showscale=False)
    fig.update_traces(texttemplate='%{text:,}')
    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False})

    fig = px.pie(df_state_total, values='Confirmed',
                 names='State', height=600,
                 hover_name='State',
                 hover_data={'State': False}
                 )
    fig.update_traces(textposition='inside', textinfo='percent+label')

    fig.update_layout(
        title=f'COVID-19 Malaysia: Proportion of Confirmed Cases as of {last_date}',
        # title_x=0.1
    )

    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False})


def plot_choropleth(df):
    fig = px.choropleth(
        df,
        locations="id",
        geojson=msia_geojson,
        color="Confirmed",
        hover_name="State",
        hover_data={"id": False, "Confirmed": True},
        # title="Confirmed Cases as of April 15, 2021",
        color_continuous_scale="YlOrRd"
    )
    fig.update_layout(
        # title_x = 0.5,
        geo=dict(
            showframe=False,
            showcoastlines=False,
            # projection_type = 'equirectangular',
            fitbounds="locations",
            visible=False
        )
    )
    # fig.update_geos()
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=600)
    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False})


if (display_one == "Show Choropleth Map") or map_checkbox:

    st.markdown(f"""
    ---

    # Choropleth Map for COVID-19 Cases by {last_date}
    """)
    st.markdown("\n")
    with st.spinner("Loading map..."):
        # plot_choropleth(df_state_total)

        # st.markdown("## Mapbox version")
        fig = px.choropleth_mapbox(
            df_state_total,
            locations="id",
            geojson=msia_geojson,
            color="Confirmed",
            hover_name="State",
            hover_data={"id": False, "Confirmed": True},
            color_continuous_scale="YlOrRd",
            # range_color=(0, max_log),
            mapbox_style='open-street-map',
            zoom=5.5,
            center={'lat': 4.1, 'lon': 109.4},
            opacity=0.6
        )
        fig.update_layout(
            margin={'r': 0, 't': 0, 'l': 0, 'b': 0},
            # coloraxis_colorbar={
            #     'title': 'Confirmed',
            #     'tickvals': values,
            #     'ticktext': ticks
            # }
        )

        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=600)
        st.plotly_chart(fig, use_container_width=True,
                        config={"displayModeBar": False})

if (display_one == "Show Animated Map!") or animated_checkbox:
    st.markdown("""
    ---

    # Animated Map based on Monthly State Cases
    """)

    df_longState = dfState.resample('M').sum().astype(int)
    df_longStyle = df_longState.copy()
    df_longStyle.rename(columns={'WP KUALA LUMPUR': 'KL',
                                 'WP LABUAN': 'LABUAN',
                                 'WP PUTRAJAYA': 'PUTRAJAYA',
                                 'PULAU PINANG': 'PENANG'}, inplace=True)
    df_longStyle = style_df(df_longStyle, axis=1)
    st.dataframe(df_longStyle, height=1200)

    st.markdown("""
    The animated map is shown in another tab to display the entire map clearly.
    """)

    def preprocess_long(df):
        df = pd.melt(df, ignore_index=False,
                     var_name='State', value_name='Confirmed')
        df['id'] = df.State.map(correct_state_id)
        df.reset_index(inplace=True)
        # remove rows with zero cases (not helping)
        # df = df[df.Confirmed > 0]
        # Sort it based on dates to possibly speed up the plotting
        df = df.sort_values('Date', ignore_index=True)
        # replace every month with first day
        df.Date = df.Date.dt.strftime("%b, %y")
        # convert the date to string for the plotly function to work
        df.Date = df.Date.astype(str)
        # change the column name to Month
        df.rename(columns={'Date': 'Month'}, inplace=True)
        return df

    df_longState = preprocess_long(df_longState)

    def plot_animated_choropleth(df, renderer='browser'):
        fig = px.choropleth(
            df,
            locations="id",
            geojson=msia_geojson,
            color="Confirmed",
            hover_name="State",
            hover_data={"id": False, "Confirmed": True},
            # title="Confirmed Cases as of April 15, 2021",
            color_continuous_scale="YlOrRd",
            animation_frame="Month"
        )
        fig.update_layout(
            # title_x = 0.5,
            geo=dict(
                showframe=False,
                # showcoastlines = True,
                # projection_type = 'equirectangular',
                fitbounds="locations",
                visible=False
            ),
            # for log transformed values
            # coloraxis_colorbar={
            #     'title': 'Confirmed',
            #     'tickvals': values,
            #     'ticktext': ticks
            # }
        )
        # fig.update_geos()
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=600)

        if not renderer:
            fig.show()
        else:
            # show in new tab to reduce the notebook size
            fig.show(renderer=renderer)

    with st.spinner("Preparing animated map ...\nThis may take awhile ..."):
        plot_animated_choropleth(df_longState)
        st.success("Map displayed on another tab.")
