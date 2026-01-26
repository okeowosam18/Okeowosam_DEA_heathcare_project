"""
Healthcare Metrics Dashboard - Streamlit Application
=====================================================
A comprehensive dashboard for analyzing nursing home staffing data.

Author: Samuel
Version: 1.1 - Fixed Decimal type handling
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import redshift_connector
from decimal import Decimal

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Healthcare Staffing Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def to_float(value):
    """Convert Decimal or any numeric type to float safely."""
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)

def to_int(value):
    """Convert to int safely."""
    if value is None:
        return 0
    return int(value)

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

@st.cache_resource
def get_connection():
    """Create a connection to Redshift Serverless."""
    try:
        conn = redshift_connector.connect(
            host=st.secrets["redshift"]["host"],
            database=st.secrets["redshift"]["database"],
            user=st.secrets["redshift"]["user"],
            password=st.secrets["redshift"]["password"],
            port=int(st.secrets["redshift"]["port"])
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None


@st.cache_data(ttl=600)  # Cache for 10 minutes
def run_query(query):
    """Execute a query and return results as DataFrame."""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        # Convert Decimal columns to float
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    # Check if first non-null value is Decimal
                    first_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if isinstance(first_val, Decimal):
                        df[col] = df[col].apply(lambda x: float(x) if x is not None else None)
                except:
                    pass
        
        return df
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        return pd.DataFrame()


# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def load_kpi_summary():
    """Load overall KPI summary."""
    query = "SELECT * FROM healthcare.v_kpi_summary;"
    return run_query(query)


def load_state_monthly():
    """Load state monthly aggregates."""
    query = """
        SELECT * FROM healthcare.v_state_monthly 
        ORDER BY state, year_month;
    """
    return run_query(query)


def load_state_summary():
    """Load state-level summary (latest month or overall)."""
    query = """
        SELECT 
            state,
            SUM(provider_count) as provider_count,
            ROUND(AVG(avg_hppd), 2) as avg_hppd,
            ROUND(AVG(avg_rn_mix_pct), 2) as avg_rn_mix_pct,
            ROUND(AVG(pct_meeting_benchmark), 1) as pct_meeting_benchmark,
            ROUND(AVG(pct_critical), 1) as pct_critical,
            SUM(total_nursing_hours) as total_nursing_hours
        FROM healthcare.v_state_monthly
        GROUP BY state
        ORDER BY avg_hppd DESC;
    """
    return run_query(query)


def load_provider_data(state_filter=None, limit=100):
    """Load provider-level data with optional state filter."""
    where_clause = f"WHERE state = '{state_filter}'" if state_filter else ""
    query = f"""
        SELECT * FROM healthcare.v_provider_monthly
        {where_clause}
        ORDER BY avg_hppd DESC
        LIMIT {limit};
    """
    return run_query(query)


def load_staffing_alerts():
    """Load staffing alerts."""
    query = """
        SELECT * FROM healthcare.v_staffing_alerts
        ORDER BY 
            CASE alert_level 
                WHEN 'HIGH' THEN 1 
                WHEN 'MEDIUM' THEN 2 
                ELSE 3 
            END,
            avg_hppd ASC
        LIMIT 100;
    """
    return run_query(query)


def load_daily_trends():
    """Load daily staffing trends."""
    query = """
        SELECT 
            work_date,
            COUNT(DISTINCT provider_id) as providers_reporting,
            ROUND(AVG(nursing_hppd), 2) as avg_hppd,
            ROUND(AVG(rn_skill_mix_pct), 2) as avg_rn_mix_pct,
            SUM(patient_census) as total_census,
            SUM(total_nursing_hours) as total_hours
        FROM healthcare.v_daily_metrics
        WHERE patient_census > 0
        GROUP BY work_date
        ORDER BY work_date;
    """
    return run_query(query)


def load_hppd_distribution():
    """Load HPPD distribution data."""
    query = """
        SELECT 
            CASE 
                WHEN nursing_hppd < 2.0 THEN '< 2.0'
                WHEN nursing_hppd < 2.5 THEN '2.0 - 2.5'
                WHEN nursing_hppd < 3.0 THEN '2.5 - 3.0'
                WHEN nursing_hppd < 3.5 THEN '3.0 - 3.5'
                WHEN nursing_hppd < 4.0 THEN '3.5 - 4.0'
                WHEN nursing_hppd < 4.5 THEN '4.0 - 4.5'
                WHEN nursing_hppd < 5.0 THEN '4.5 - 5.0'
                ELSE '5.0+'
            END as hppd_range,
            COUNT(*) as record_count
        FROM healthcare.v_daily_metrics
        WHERE patient_census > 0 AND nursing_hppd IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
    """
    return run_query(query)


# =============================================================================
# VISUALIZATION FUNCTIONS
# =============================================================================

def create_kpi_cards(kpi_data):
    """Create KPI metric cards."""
    if kpi_data.empty:
        st.warning("No KPI data available")
        return
    
    row = kpi_data.iloc[0]
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            label="Total Providers",
            value=f"{to_int(row['total_providers']):,}"
        )
    
    with col2:
        st.metric(
            label="Total States",
            value=f"{to_int(row['total_states'])}"
        )
    
    with col3:
        st.metric(
            label="Total Records",
            value=f"{to_int(row['total_records']):,}"
        )
    
    with col4:
        avg_hppd = to_float(row['overall_avg_hppd'])
        st.metric(
            label="Avg HPPD",
            value=f"{avg_hppd:.2f}",
            delta=f"{avg_hppd - 4.1:.2f} vs benchmark"
        )
    
    with col5:
        st.metric(
            label="RN Mix %",
            value=f"{to_float(row['overall_rn_mix_pct']):.1f}%"
        )
    
    with col6:
        st.metric(
            label="% Meeting Benchmark",
            value=f"{to_float(row['pct_meeting_benchmark']):.1f}%"
        )


def create_state_map(state_data):
    """Create a choropleth map of states by HPPD."""
    if state_data.empty:
        return None
    
    fig = px.choropleth(
        state_data,
        locations='state',
        locationmode='USA-states',
        color='avg_hppd',
        color_continuous_scale='RdYlGn',
        range_color=[2.5, 5.0],
        scope='usa',
        labels={'avg_hppd': 'Avg HPPD'},
        title='Average Nursing HPPD by State'
    )
    
    fig.update_layout(
        geo=dict(bgcolor='rgba(0,0,0,0)'),
        margin=dict(l=0, r=0, t=40, b=0),
        height=400
    )
    
    return fig


def create_state_bar_chart(state_data, metric='avg_hppd', title='Average HPPD by State'):
    """Create a bar chart comparing states."""
    if state_data.empty:
        return None
    
    sorted_data = state_data.sort_values(metric, ascending=True)
    
    fig = px.bar(
        sorted_data,
        x=metric,
        y='state',
        orientation='h',
        color=metric,
        color_continuous_scale='RdYlGn',
        title=title
    )
    
    if metric == 'avg_hppd':
        fig.add_vline(x=4.1, line_dash="dash", line_color="red", 
                      annotation_text="CMS Benchmark (4.1)")
    
    fig.update_layout(
        height=800,
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'}
    )
    
    return fig


def create_trend_chart(trend_data):
    """Create a time series chart of staffing trends."""
    if trend_data.empty:
        return None
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Average HPPD Over Time', 'Daily Census Over Time'),
        vertical_spacing=0.12
    )
    
    fig.add_trace(
        go.Scatter(
            x=trend_data['work_date'],
            y=trend_data['avg_hppd'],
            mode='lines',
            name='Avg HPPD',
            line=dict(color='#2E86AB', width=2)
        ),
        row=1, col=1
    )
    
    fig.add_hline(y=4.1, line_dash="dash", line_color="red", row=1, col=1,
                  annotation_text="CMS Benchmark")
    
    fig.add_trace(
        go.Scatter(
            x=trend_data['work_date'],
            y=trend_data['total_census'],
            mode='lines',
            name='Total Census',
            line=dict(color='#A23B72', width=2),
            fill='tozeroy'
        ),
        row=2, col=1
    )
    
    fig.update_layout(height=500, showlegend=True)
    
    return fig


def create_hppd_histogram(dist_data):
    """Create HPPD distribution histogram."""
    if dist_data.empty:
        return None
    
    order = ['< 2.0', '2.0 - 2.5', '2.5 - 3.0', '3.0 - 3.5', '3.5 - 4.0', '4.0 - 4.5', '4.5 - 5.0', '5.0+']
    dist_data['hppd_range'] = pd.Categorical(dist_data['hppd_range'], categories=order, ordered=True)
    dist_data = dist_data.sort_values('hppd_range')
    
    colors = ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    
    fig = px.bar(
        dist_data,
        x='hppd_range',
        y='record_count',
        title='Distribution of Nursing HPPD',
        labels={'hppd_range': 'HPPD Range', 'record_count': 'Number of Records'},
        color='hppd_range',
        color_discrete_sequence=colors
    )
    
    fig.update_layout(showlegend=False, height=400)
    
    return fig


def create_staffing_mix_pie():
    """Create a pie chart of staffing mix."""
    query = """
        SELECT 
            SUM(hrs_rn) as rn_hours,
            SUM(hrs_lpn) as lpn_hours,
            SUM(hrs_cna) as cna_hours
        FROM healthcare.v_daily_metrics
        WHERE patient_census > 0;
    """
    data = run_query(query)
    
    if data.empty:
        return None
    
    row = data.iloc[0]
    
    fig = px.pie(
        values=[to_float(row['rn_hours']), to_float(row['lpn_hours']), to_float(row['cna_hours'])],
        names=['RN', 'LPN', 'CNA'],
        title='Staffing Mix by Role',
        color_discrete_sequence=['#2E86AB', '#A23B72', '#F18F01']
    )
    
    fig.update_layout(height=400)
    
    return fig


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    """Main application entry point."""
    
    # Header
    st.title("🏥 Healthcare Staffing Analytics Dashboard")
    st.markdown("*Analyzing nursing home staffing patterns from CMS PBJ data (Q2 2024)*")
    
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select Page",
        ["📊 Overview", "🗺️ State Analysis", "🏢 Provider Details", "⚠️ Staffing Alerts", "📈 Trends"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.markdown("""
    This dashboard analyzes nursing home staffing data from the CMS 
    Payroll-Based Journal (PBJ) system for Q2 2024.
    
    **Key Metrics:**
    - HPPD: Hours Per Patient Day
    - CMS Benchmark: 4.1 HPPD
    """)
    
    # ==========================================================================
    # PAGE: OVERVIEW
    # ==========================================================================
    if page == "📊 Overview":
        st.header("Executive Summary")
        
        kpi_data = load_kpi_summary()
        create_kpi_cards(kpi_data)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("HPPD Distribution")
            dist_data = load_hppd_distribution()
            fig = create_hppd_histogram(dist_data)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Staffing Mix")
            fig = create_staffing_mix_pie()
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.subheader("Key Insights")
        
        if not kpi_data.empty:
            row = kpi_data.iloc[0]
            avg_hppd = to_float(row['overall_avg_hppd'])
            pct_benchmark = to_float(row['pct_meeting_benchmark'])
            pct_critical = to_float(row['pct_critical_understaffing'])
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.info(f"""
                **Staffing Gap**  
                The average HPPD of {avg_hppd:.2f} is 
                {((4.1 - avg_hppd) / 4.1 * 100):.1f}% below 
                the CMS benchmark of 4.1 hours.
                """)
            
            with col2:
                st.warning(f"""
                **Benchmark Performance**  
                Only {pct_benchmark:.1f}% of facility-days 
                meet the CMS staffing benchmark, indicating widespread 
                staffing challenges.
                """)
            
            with col3:
                st.error(f"""
                **Critical Understaffing**  
                {pct_critical:.1f}% of facility-days 
                show critical understaffing (HPPD < 2.5), which may 
                impact patient care quality.
                """)
    
    # ==========================================================================
    # PAGE: STATE ANALYSIS
    # ==========================================================================
    elif page == "🗺️ State Analysis":
        st.header("State-Level Analysis")
        
        state_data = load_state_summary()
        
        if not state_data.empty:
            st.subheader("Geographic Distribution of Staffing Levels")
            fig = create_state_map(state_data)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("States by Average HPPD")
                fig = create_state_bar_chart(state_data, 'avg_hppd', 'Average HPPD by State')
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("States by % Meeting Benchmark")
                fig = create_state_bar_chart(state_data, 'pct_meeting_benchmark', 
                                            '% Meeting CMS Benchmark by State')
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            st.subheader("State Summary Table")
            st.dataframe(
                state_data.style.background_gradient(subset=['avg_hppd'], cmap='RdYlGn'),
                use_container_width=True
            )
    
    # ==========================================================================
    # PAGE: PROVIDER DETAILS
    # ==========================================================================
    elif page == "🏢 Provider Details":
        st.header("Provider-Level Analysis")
        
        state_data = load_state_summary()
        states = ['All States'] + sorted(state_data['state'].tolist()) if not state_data.empty else ['All States']
        
        selected_state = st.selectbox("Select State", states)
        
        state_filter = None if selected_state == 'All States' else selected_state
        provider_data = load_provider_data(state_filter=state_filter, limit=200)
        
        if not provider_data.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Providers", len(provider_data))
            with col2:
                st.metric("Avg HPPD", f"{provider_data['avg_hppd'].mean():.2f}")
            with col3:
                st.metric("Avg Census", f"{provider_data['avg_daily_census'].mean():.1f}")
            with col4:
                benchmark_pct = (provider_data['avg_hppd'] >= 4.1).mean() * 100
                st.metric("% ≥ Benchmark", f"{benchmark_pct:.1f}%")
            
            st.markdown("---")
            
            st.subheader("Provider Staffing Overview")
            
            fig = px.scatter(
                provider_data,
                x='avg_daily_census',
                y='avg_hppd',
                color='avg_rn_mix_pct',
                size='days_reported',
                hover_data=['provider_id', 'state'],
                color_continuous_scale='Viridis',
                labels={
                    'avg_daily_census': 'Average Daily Census',
                    'avg_hppd': 'Average HPPD',
                    'avg_rn_mix_pct': 'RN Mix %'
                },
                title='Provider Staffing: Census vs HPPD'
            )
            
            fig.add_hline(y=4.1, line_dash="dash", line_color="red",
                         annotation_text="CMS Benchmark (4.1)")
            
            st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Provider Details")
            st.dataframe(
                provider_data[['provider_id', 'state', 'avg_daily_census', 'avg_hppd', 
                              'avg_rn_mix_pct', 'critical_days', 'days_reported']]
                .style.background_gradient(subset=['avg_hppd'], cmap='RdYlGn'),
                use_container_width=True
            )
    
    # ==========================================================================
    # PAGE: STAFFING ALERTS
    # ==========================================================================
    elif page == "⚠️ Staffing Alerts":
        st.header("Staffing Alerts")
        st.markdown("*Facilities with concerning staffing levels*")
        
        alerts = load_staffing_alerts()
        
        if not alerts.empty:
            col1, col2, col3 = st.columns(3)
            
            high_alerts = len(alerts[alerts['alert_level'] == 'HIGH'])
            med_alerts = len(alerts[alerts['alert_level'] == 'MEDIUM'])
            low_alerts = len(alerts[alerts['alert_level'] == 'LOW'])
            
            with col1:
                st.error(f"🔴 HIGH Alerts: {high_alerts}")
            with col2:
                st.warning(f"🟡 MEDIUM Alerts: {med_alerts}")
            with col3:
                st.info(f"🟢 LOW Alerts: {low_alerts}")
            
            st.markdown("---")
            
            alert_filter = st.multiselect(
                "Filter by Alert Level",
                ['HIGH', 'MEDIUM', 'LOW'],
                default=['HIGH', 'MEDIUM']
            )
            
            filtered_alerts = alerts[alerts['alert_level'].isin(alert_filter)]
            
            st.dataframe(
                filtered_alerts.style.apply(
                    lambda x: ['background-color: #ffcccc' if v == 'HIGH' 
                              else 'background-color: #fff3cd' if v == 'MEDIUM'
                              else '' for v in x],
                    subset=['alert_level']
                ),
                use_container_width=True
            )
    
    # ==========================================================================
    # PAGE: TRENDS
    # ==========================================================================
    elif page == "📈 Trends":
        st.header("Staffing Trends Over Time")
        
        trend_data = load_daily_trends()
        
        if not trend_data.empty:
            fig = create_trend_chart(trend_data)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            
            st.subheader("Monthly State Trends")
            
            state_monthly = load_state_monthly()
            
            if not state_monthly.empty:
                states = state_monthly['state'].unique().tolist()
                selected_states = st.multiselect(
                    "Select States to Compare",
                    states,
                    default=states[:5] if len(states) >= 5 else states
                )
                
                if selected_states:
                    filtered = state_monthly[state_monthly['state'].isin(selected_states)]
                    
                    fig = px.line(
                        filtered,
                        x='year_month',
                        y='avg_hppd',
                        color='state',
                        title='Average HPPD by State Over Time',
                        labels={'year_month': 'Month', 'avg_hppd': 'Avg HPPD'}
                    )
                    
                    fig.add_hline(y=4.1, line_dash="dash", line_color="red",
                                 annotation_text="CMS Benchmark")
                    
                    st.plotly_chart(fig, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray;'>
        Healthcare Staffing Analytics Dashboard | Data Source: CMS PBJ Q2 2024 | 
        Built with Streamlit
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
