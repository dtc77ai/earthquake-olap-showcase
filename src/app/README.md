# 🎨 Streamlit Application Development Guide

A practical guide to developing multi-page Streamlit applications, based on the Earthquake OLAP Analytics project.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture Philosophy](#architecture-philosophy)
- [Project Structure](#project-structure)
- [Development Workflow](#development-workflow)
- [Best Practices](#best-practices)
- [Component Design](#component-design)
- [Understanding Streamlit's Reactive Model](#understanding-streamlits-reactive-model)
- [State Management](#state-management)
- [Performance Optimization](#performance-optimization)
- [Debugging Tips](#debugging-tips)
- [Common Patterns](#common-patterns)
- [Deployment Considerations](#deployment-considerations)

## 🎯 Overview

This guide explains how to architect, develop, and maintain a production-ready Streamlit application. While the example uses earthquake data, the patterns and practices apply to any Streamlit project.

### Why This Architecture?

- ✅ **Scalable** - Easy to add new pages and features
- ✅ **Maintainable** - Clear separation of concerns
- ✅ **Reusable** - Components can be shared across pages
- ✅ **Testable** - Business logic separated from UI
- ✅ **Professional** - Follows industry best practices

## 🏗️ Architecture Philosophy

### Separation of Concerns

```bash
streamlit-app/
├── main.py              # Entry point & landing page
├── pages/               # Individual page modules
│   ├── 1_overview.py
│   ├── 2_analysis.py
│   └── 3_maps.py
└── components/          # Reusable UI components
    ├── charts.py
    └── filters.py
```

**Key Principles:**

1. **Main App** - Landing page, navigation, global config
2. **Pages** - Self-contained feature modules
3. **Components** - Reusable UI elements
4. **Business Logic** - Kept separate in `src/` modules

### Why Multi-Page Apps?

Streamlit's multi-page architecture (introduced in v1.10.0) provides:

- **Automatic navigation** - Sidebar menu generated automatically
- **URL routing** - Each page has its own URL
- **Independent state** - Pages can maintain separate state
- **Lazy loading** - Pages load only when accessed

## 📁 Project Structure

### Recommended Layout

```bash
src/app/
│
├── main.py                    # Main entry point
│   ├── Page configuration
│   ├── Landing page content
│   ├── Sidebar (global)
│   └── Footer
│
├── pages/                     # Multi-page app pages
│   ├── __init__.py
│   ├── 1_overview.py          # Numbers prefix for ordering
│   ├── 2_analysis.py
│   ├── 3_maps.py
│   └── 4_moon_phase.py
│
└── components/                # Reusable components
    ├── __init__.py
    ├── charts.py              # Chart generation functions
    └── filters.py             # Filter UI components
```

### File Naming Convention

**Pages:**

- Prefix with numbers for ordering: `1_`, `2_`, `3_`
- Use descriptive names: `overview`, `analysis`, `maps`
- Streamlit converts to title case in sidebar: "1 Overview", "2 Analysis"

**Components:**

- Group by functionality: `charts.py`, `filters.py`, `tables.py`
- Use clear, descriptive names
- One component type per file

## 🚀 Development Workflow

### Step 1: Setup and Configuration

**Create the main app file:**

```python
# src/app/main.py
import streamlit as st

# Page configuration (MUST be first Streamlit command)
st.set_page_config(
    page_title="Your App Name",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS (optional)
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
    }
    </style>
""", unsafe_allow_html=True)
```

**Key Points:**

- `st.set_page_config()` MUST be the first Streamlit command
- Configure layout, icons, and initial state here
- Add custom CSS for branding

### Step 2: Create the Landing Page

```python
# src/app/main.py (continued)

st.markdown('<h1 class="main-header">🎯 Your App Title</h1>', unsafe_allow_html=True)

st.markdown("""
## Welcome to Your Application

### Features
- Feature 1
- Feature 2
- Feature 3

Use the sidebar to navigate between pages.
""")
```

### Step 3: Add Sidebar (Global Navigation)

```python
# src/app/main.py (continued)

with st.sidebar:
    st.image("path/to/logo.png", width=200)
    
    st.markdown("---")
    
    st.markdown("### Quick Stats")
    # Add global metrics or info
    
    st.markdown("---")
    
    st.markdown("### About")
    st.markdown("Version: 1.0.0")
```

### Step 4: Create Individual Pages

**Page structure:**

```python
# src/app/pages/1_overview.py
import streamlit as st

# Page-specific configuration
st.set_page_config(page_title="Overview", page_icon="📊", layout="wide")

# Page title
st.title("📊 Overview Dashboard")

# Page content
st.markdown("Your page content here")

# Sidebar filters (page-specific)
with st.sidebar:
    st.header("Filters")
    filter_value = st.slider("Select value", 0, 100, 50)
```

**Best Practices for Pages:**

- Each page is self-contained
- Set page-specific config
- Use clear section headers
- Add page-specific sidebar content

### Step 5: Create Reusable Components

**Chart components:**

```python
# src/app/components/charts.py
import plotly.graph_objects as go

def create_bar_chart(df, x_col, y_col, title):
    """Create a reusable bar chart.
    
    Args:
        df: DataFrame with data
        x_col: Column name for x-axis
        y_col: Column name for y-axis
        title: Chart title
        
    Returns:
        Plotly figure
    """
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df[x_col],
        y=df[y_col],
        name=title
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title=x_col,
        yaxis_title=y_col,
        height=400
    )
    
    return fig
```

**Filter components:**

```python
# src/app/components/filters.py
import streamlit as st

def create_date_filter(min_date, max_date):
    """Create a reusable date range filter.
    
    Args:
        min_date: Minimum selectable date
        max_date: Maximum selectable date
        
    Returns:
        Tuple of (start_date, end_date)
    """
    st.subheader("📅 Date Range")
    
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input("Start", value=min_date)
    
    with col2:
        end_date = st.date_input("End", value=max_date)
    
    return start_date, end_date
```

### Step 6: Use Components in Pages

```python
# src/app/pages/2_analysis.py
import streamlit as st
from src.app.components.charts import create_bar_chart
from src.app.components.filters import create_date_filter

st.title("📈 Analysis")

# Use filter component
start_date, end_date = create_date_filter(min_date, max_date)

# Get data based on filters
df = get_filtered_data(start_date, end_date)

# Use chart component
fig = create_bar_chart(df, "date", "value", "Daily Trends")
st.plotly_chart(fig, use_container_width=True)
```

## 💡 Best Practices

### 1. Page Organization

**DO:**

- ✅ One feature per page
- ✅ Clear, descriptive page names
- ✅ Logical navigation flow
- ✅ Consistent layout across pages

**DON'T:**

- ❌ Cram everything into one page
- ❌ Use generic names like "page1.py"
- ❌ Mix unrelated features
- ❌ Inconsistent styling

### 2. Component Design

**DO:**

- ✅ Make components pure functions
- ✅ Accept parameters for customization
- ✅ Return Plotly figures, not display them
- ✅ Add docstrings with examples

**DON'T:**

- ❌ Hard-code values in components
- ❌ Mix data fetching with visualization
- ❌ Use global state in components
- ❌ Create overly complex components

### 3. State Management

**Use Session State for:**

- Filter values that persist across pages
- User preferences
- Cached computations
- Multi-step workflows

**Example:**

```python
# Initialize session state
if "filter_value" not in st.session_state:
    st.session_state.filter_value = 50

# Use session state
value = st.slider(
    "Select value",
    0, 100,
    value=st.session_state.filter_value,
    key="value_slider"
)

# Update session state
st.session_state.filter_value = value
```

### 4. Performance Optimization

**Use Caching:**

```python
@st.cache_data
def load_data():
    """Load data with caching."""
    return expensive_data_load()

@st.cache_resource
def get_database_connection():
    """Cache database connection."""
    return create_connection()
```

**When to Use Each:**

- `@st.cache_data` - For data (DataFrames, lists, dicts)
- `@st.cache_resource` - For connections (DB, API clients)

### 5. Layout Patterns

**Columns:**

```python
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Metric 1", "100")

with col2:
    st.metric("Metric 2", "200")

with col3:
    st.metric("Metric 3", "300")
```

**Tabs:**

```python
tab1, tab2, tab3 = st.tabs(["Overview", "Details", "Settings"])

with tab1:
    st.write("Overview content")

with tab2:
    st.write("Details content")

with tab3:
    st.write("Settings content")
```

**Expanders:**

```python
with st.expander("See explanation"):
    st.write("Detailed explanation here")
```

### 6. Error Handling

```python
try:
    data = load_data()
    if data.empty:
        st.warning("No data available")
    else:
        display_chart(data)
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()  # Stop execution
```

## 🔄 Understanding Streamlit's Reactive Model

### How Streamlit Works

Streamlit has a unique execution model that's fundamentally different from traditional web frameworks:

**Key Concept:** Every interaction reruns the entire script from top to bottom.

```python
# This entire script runs on every interaction
import streamlit as st

st.title("Counter App")

# This runs every time!
print("Script is running...")

if "count" not in st.session_state:
    st.session_state.count = 0

if st.button("Increment"):
    st.session_state.count += 1

st.write(f"Count: {st.session_state.count}")
```

**What happens when you click the button:**

1. User clicks "Increment" button
2. **Entire script reruns from line 1**
3. Session state persists across reruns
4. Button click is detected
5. Counter increments
6. UI updates with new value

### The Rerun Cycle

```bash
User Action → Script Reruns → State Updates → UI Refreshes → Wait for Next Action
     ↑                                                                    ↓
     └────────────────────────────────────────────────────────────────────┘
```

### Why This Matters

**Implications:**

- ✅ **Simple mental model** - No callbacks, no event handlers
- ✅ **Declarative UI** - Describe what you want, not how to update it
- ⚠️ **Performance consideration** - Expensive operations run on every interaction
- ⚠️ **State management** - Must use `st.session_state` for persistence

### Practical Example: Form with Validation

```python
import streamlit as st

st.title("User Registration")

# Initialize session state
if "submitted" not in st.session_state:
    st.session_state.submitted = False
if "username" not in st.session_state:
    st.session_state.username = ""

# This runs on EVERY interaction
st.write("🔄 Script rerun count:", st.session_state.get("rerun_count", 0))
st.session_state.rerun_count = st.session_state.get("rerun_count", 0) + 1

# Input widgets
username = st.text_input(
    "Username",
    value=st.session_state.username,
    key="username_input"
)

email = st.text_input("Email")

# Validation (runs on every rerun)
is_valid = len(username) >= 3 and "@" in email

# Button state changes trigger rerun
if st.button("Submit", disabled=not is_valid):
    st.session_state.submitted = True
    st.session_state.username = username

# Display result (persists via session state)
if st.session_state.submitted:
    st.success(f"✅ Registered: {st.session_state.username}")
    
    # Reset button triggers another rerun
    if st.button("Register Another"):
        st.session_state.submitted = False
        st.session_state.username = ""
        st.rerun()  # Explicit rerun
```

**Execution flow:**

1. **First load** - Script runs, form displays, rerun_count = 1
2. **Type in username** - Script reruns, validation updates, rerun_count = 2
3. **Type in email** - Script reruns, button enables, rerun_count = 3
4. **Click Submit** - Script reruns, success message shows, rerun_count = 4
5. **Click Reset** - Script reruns, form clears, rerun_count = 5

### Controlling Reruns

**Prevent unnecessary reruns with caching:**

```python
@st.cache_data
def expensive_computation(param):
    """This only runs when param changes."""
    import time
    time.sleep(2)  # Simulate expensive operation
    return param * 2

# First call: runs and caches
result = expensive_computation(5)

# Subsequent calls with same param: uses cache
result = expensive_computation(5)  # Instant!

# Different param: runs again
result = expensive_computation(10)  # Takes 2 seconds
```

**Force a rerun programmatically:**

```python
if st.button("Refresh Data"):
    # Clear cache and rerun
    st.cache_data.clear()
    st.rerun()
```

**Stop execution early:**

```python
if not user_authenticated:
    st.error("Please log in")
    st.stop()  # Stops script execution here

# This code only runs if authenticated
st.write("Welcome to the dashboard!")
```

### Common Pitfalls

#### Pitfall 1: Expensive operations without caching

```python
# ❌ BAD - Runs on every interaction
def load_data():
    return pd.read_csv("huge_file.csv")  # Slow!

df = load_data()  # Loads on every button click!

# ✅ GOOD - Cached
@st.cache_data
def load_data():
    return pd.read_csv("huge_file.csv")

df = load_data()  # Loads once, cached thereafter
```

#### Pitfall 2: Not using session state for persistence

```python
# ❌ BAD - Resets on every rerun
counter = 0

if st.button("Increment"):
    counter += 1  # Always resets to 0!

st.write(counter)  # Always shows 0

# ✅ GOOD - Persists across reruns
if "counter" not in st.session_state:
    st.session_state.counter = 0

if st.button("Increment"):
    st.session_state.counter += 1

st.write(st.session_state.counter)  # Shows actual count
```

#### Pitfall 3: Side effects in the main script

```python
# ❌ BAD - Writes to file on every rerun
with open("log.txt", "a") as f:
    f.write("Script ran\n")  # File grows huge!

# ✅ GOOD - Only write when needed
if st.button("Save Log"):
    with open("log.txt", "a") as f:
        f.write(f"Saved at {datetime.now()}\n")
```

### Advanced: Controlling Rerun Behavior

**Conditional rendering:**

```python
# Only show expensive chart if checkbox is enabled
show_chart = st.checkbox("Show Detailed Chart")

if show_chart:
    # This expensive operation only runs when checkbox is True
    with st.spinner("Generating chart..."):
        fig = create_complex_chart(data)
        st.plotly_chart(fig)
```

**Debouncing user input:**

```python
import time

# Initialize
if "last_input_time" not in st.session_state:
    st.session_state.last_input_time = 0

search_term = st.text_input("Search")

# Only search if user stopped typing for 0.5 seconds
current_time = time.time()
if search_term and (current_time - st.session_state.last_input_time) > 0.5:
    results = search_database(search_term)
    st.write(results)

st.session_state.last_input_time = current_time
```

### Mental Model Summary

**Think of Streamlit as:**

- A **reactive spreadsheet** - Change input, everything recalculates
- A **pure function** - Same inputs → same outputs
- A **declarative UI** - Describe the UI, Streamlit handles updates

**Not as:**

- ❌ Event-driven framework (like JavaScript)
- ❌ Stateful application (like Flask with sessions)
- ❌ Component-based framework (like React)

**Key Mantra:** "The script is the app. Every interaction reruns the script."

## 🔄 State Management

### Session State Best Practices

**Initialize at the top:**

```python
# Initialize all session state at the beginning
if "initialized" not in st.session_state:
    st.session_state.initialized = True
    st.session_state.filter_min = 0
    st.session_state.filter_max = 100
    st.session_state.selected_page = "overview"
```

**Use unique keys:**

```python
# Good - unique keys prevent conflicts
st.slider("Min Value", key="filter_min_slider")
st.slider("Max Value", key="filter_max_slider")

# Bad - no keys can cause issues
st.slider("Min Value")
st.slider("Max Value")
```

**Persist filters across pages:**

```python
# Page 1
if "global_filter" not in st.session_state:
    st.session_state.global_filter = "default"

filter_value = st.selectbox(
    "Filter",
    options=["A", "B", "C"],
    index=["A", "B", "C"].index(st.session_state.global_filter)
)
st.session_state.global_filter = filter_value

# Page 2 - filter value is preserved!
st.write(f"Current filter: {st.session_state.global_filter}")
```

## ⚡ Performance Optimization

### 1. Efficient Data Loading

```python
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_large_dataset():
    """Load and cache large dataset."""
    df = pd.read_csv("large_file.csv")
    return df

# Load once, reuse everywhere
df = load_large_dataset()
```

### 2. Lazy Loading

```python
# Only load data when needed
if st.button("Load Data"):
    with st.spinner("Loading..."):
        data = load_expensive_data()
        st.success("Data loaded!")
        st.dataframe(data)
```

### 3. Pagination

```python
# For large datasets, use pagination
page_size = 100
page = st.number_input("Page", min_value=1, value=1)

start_idx = (page - 1) * page_size
end_idx = start_idx + page_size

st.dataframe(df.iloc[start_idx:end_idx])
```

### 4. Optimize Visualizations

```python
# Limit data points for charts
MAX_POINTS = 1000

if len(df) > MAX_POINTS:
    # Sample or aggregate
    df_chart = df.sample(n=MAX_POINTS)
    st.info(f"Showing {MAX_POINTS} sampled points")
else:
    df_chart = df

fig = create_chart(df_chart)
st.plotly_chart(fig)
```

## 🐛 Debugging Tips

### 1. Use st.write() for Debugging

```python
# Quick debug output
st.write("Debug:", variable_name)
st.write("Type:", type(variable_name))
st.write("Shape:", df.shape if hasattr(df, 'shape') else 'N/A')
```

### 2. Display Session State

```python
# See all session state
with st.expander("Debug: Session State"):
    st.write(st.session_state)
```

### 3. Catch and Display Errors

```python
try:
    result = risky_operation()
except Exception as e:
    st.error(f"Error: {e}")
    
    # Show full traceback in expander
    with st.expander("Full Error Details"):
        import traceback
        st.code(traceback.format_exc())
```

### 4. Use Logging

```python
import logging

logger = logging.getLogger(__name__)

# Log to file
logger.info("User accessed page")
logger.error(f"Error occurred: {error}")

# Display logs in app (development only)
if st.checkbox("Show Logs"):
    with open("app.log") as f:
        st.code(f.read())
```

### 5. Hot Reload

Streamlit automatically reloads when files change:

- Save your file
- Browser refreshes automatically
- State is preserved (mostly)

**Tip:** Use "Always rerun" in the menu for continuous development.

## 🎨 Common Patterns

### Pattern 1: Filter → Query → Display

```python
# 1. Filters in sidebar
with st.sidebar:
    min_value = st.slider("Min", 0, 100, 0)
    max_value = st.slider("Max", 0, 100, 100)

# 2. Query data based on filters
filtered_data = df[
    (df['value'] >= min_value) & 
    (df['value'] <= max_value)
]

# 3. Display results
st.metric("Filtered Records", len(filtered_data))
st.dataframe(filtered_data)
```

### Pattern 2: Tabs for Multiple Views

```python
tab1, tab2, tab3 = st.tabs(["Chart", "Table", "Raw Data"])

with tab1:
    st.plotly_chart(create_chart(df))

with tab2:
    st.dataframe(df.describe())

with tab3:
    st.json(df.to_dict())
```

### Pattern 3: Progressive Disclosure

```python
# Show summary first
st.metric("Total Records", len(df))

# Offer details on demand
if st.checkbox("Show Details"):
    st.dataframe(df)
    
    if st.checkbox("Show Statistics"):
        st.write(df.describe())
```

### Pattern 4: Multi-Step Workflow

```python
# Step 1
if "step" not in st.session_state:
    st.session_state.step = 1

if st.session_state.step == 1:
    st.header("Step 1: Select Data")
    if st.button("Next"):
        st.session_state.step = 2
        st.rerun()

elif st.session_state.step == 2:
    st.header("Step 2: Configure")
    if st.button("Next"):
        st.session_state.step = 3
        st.rerun()

elif st.session_state.step == 3:
    st.header("Step 3: Results")
    if st.button("Start Over"):
        st.session_state.step = 1
        st.rerun()
```

## 🚀 Deployment Considerations

### 1. Configuration

**Use environment variables:**

```python
import os

# Development vs Production
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

if DEBUG:
    st.sidebar.write("🔧 Debug Mode")
```

### 2. Secrets Management

**Use Streamlit secrets:**

```python
# .streamlit/secrets.toml (not committed to git)
# [database]
# host = "localhost"
# password = "secret"

# In your app
import streamlit as st

db_host = st.secrets["database"]["host"]
db_password = st.secrets["database"]["password"]
```

### 3. Resource Limits

**Set appropriate limits:**

```python
# Limit file upload size
uploaded_file = st.file_uploader(
    "Upload CSV",
    type=["csv"],
    help="Max 200MB"
)

if uploaded_file and uploaded_file.size > 200_000_000:
    st.error("File too large")
    st.stop()
```

### 4. Error Boundaries

**Graceful degradation:**

```python
try:
    # Try optimal approach
    result = fast_method()
except Exception:
    # Fall back to slower but reliable method
    st.warning("Using fallback method")
    result = slow_but_reliable_method()
```

## 📚 Additional Resources

### Official Documentation

- [Streamlit Docs](https://docs.streamlit.io/)
- [API Reference](https://docs.streamlit.io/library/api-reference)
- [Multi-page Apps](https://docs.streamlit.io/library/get-started/multipage-apps)

### Community

- [Streamlit Forum](https://discuss.streamlit.io/)
- [GitHub Issues](https://github.com/streamlit/streamlit/issues)
- [Gallery](https://streamlit.io/gallery)

### Best Practices

- [Performance Tips](https://docs.streamlit.io/library/advanced-features/caching)
- [Deployment Guide](https://docs.streamlit.io/streamlit-community-cloud/get-started)

## 🎯 Summary

**Key Takeaways:**

1. **Structure matters** - Organize pages and components logically
2. **Reuse components** - DRY principle applies to UI too
3. **Manage state carefully** - Use session state for persistence
4. **Optimize performance** - Cache data and resources
5. **Handle errors gracefully** - Always provide user feedback
6. **Keep it simple** - Streamlit's strength is simplicity

**Development Cycle:**

1. Start with `main.py` - landing page and config
2. Add pages one at a time - test as you go
3. Extract reusable components - refactor early
4. Add state management - when needed
5. Optimize performance - profile and improve
6. Deploy and iterate - gather feedback

---
