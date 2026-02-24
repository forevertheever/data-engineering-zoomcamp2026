import marimo

app = marimo.App()


@app.cell
def __():
    import duckdb
    import pandas as pd
    import plotly.express as px
    import marimo as mo
    return duckdb, pd, px, mo


@app.cell
def __(mo):
    mo.md("# Open Library Authors Visualization")


@app.cell
def __(duckdb, mo):
    # Connect to DuckDB database created by dlt pipeline
    con = duckdb.connect(
        "/workspaces/data-engineering-zoomcamp2026/dlt_workshop/open_library_pipeline.duckdb"
    )
    
    # Verify connection
    try:
        result = con.execute("SELECT COUNT(*) FROM open_library_pipeline_dataset.books").fetchone()
        mo.md(f"## ✓ Connected to DuckDB Pipeline\n\n**Books loaded:** {result[0]}")
    except Exception as e:
        mo.md(f"Error: {str(e)}")
    
    return con


@app.cell
def __(con, pd):
    # Fetch all authors from both books
    query_authors1 = """
    SELECT * FROM open_library_pipeline_dataset."books__isbn_0451524934__authors"
    """
    query_authors2 = """
    SELECT * FROM open_library_pipeline_dataset."books__isbn_0451524977__authors"
    """
    
    try:
        authors1 = con.execute(query_authors1).df()
        authors1['book_isbn'] = '0451524934'
        authors1['book_title'] = 'Nineteen Eighty-Four'
    except Exception as e:
        authors1 = None
        print(f"Error loading authors1: {e}")
    
    try:
        authors2 = con.execute(query_authors2).df()
        authors2['book_isbn'] = '0451524977'
        authors2['book_title'] = 'Benjamin Franklin'
    except Exception as e:
        authors2 = None
        print(f"Error loading authors2: {e}")
    
    # Combine authors
    if authors1 is not None and authors2 is not None:
        all_authors = pd.concat([authors1, authors2], ignore_index=True)
    elif authors1 is not None:
        all_authors = authors1
    elif authors2 is not None:
        all_authors = authors2
    else:
        all_authors = pd.DataFrame()
    
    print(f"Total author records: {len(all_authors)}")
    
    return all_authors, authors1, authors2


@app.cell
def __(all_authors, pd, mo):
    if len(all_authors) > 0:
        # Show the raw data info
        mo.md(f"## Authors Data\n\n**Total records:** {len(all_authors)}")
        
        # Get unique column names (excluding dlt metadata)
        cols = [c for c in all_authors.columns.tolist() if not c.startswith('_dlt')]
        mo.md(f"**Key columns:** {', '.join(cols[:5])}")
        
        # Group by author name and count occurrences
        author_counts = all_authors.groupby('name').size().reset_index(name='book_count')
        author_counts = author_counts.sort_values('book_count', ascending=False)
        
        # Get top 10 authors (or all if less than 10)
        top_10_authors = author_counts.head(10)
        
        mo.md(f"## Top Authors by Book Count\n\n**Found {len(author_counts)} unique authors**")
    else:
        top_10_authors = pd.DataFrame()
        mo.md("No author data available")
    
    return author_counts, top_10_authors


@app.cell
def __(top_10_authors):
    # Display the top 10 table
    top_10_authors


@app.cell
def __(top_10_authors, px, mo):
    if len(top_10_authors) > 0:
        # Create visualization
        fig = px.bar(
            top_10_authors,
            x='name',
            y='book_count',
            title='Top Authors by Book Count in Open Library Dataset',
            labels={'name': 'Author Name', 'book_count': 'Number of Books'},
            color='book_count',
            color_continuous_scale='viridis'
        )
        
        fig.update_layout(
            xaxis_tickangle=-45,
            height=500,
            showlegend=False
        )
        
        fig
    else:
        mo.md("No data to visualize")


@app.cell
def __(author_counts, mo):
    mo.md(f"""
    ## Summary Statistics
    
    - **Unique authors**: {len(author_counts) if 'author_counts' in dir() else 0}
    - **Source**: Open Library REST API via dlt pipeline
    - **Database**: DuckDB with direct data access
    - **Datasets**: 2 books (ISBN 0451524934 - Nineteen Eighty-Four, ISBN 0451524977 - Benjamin Franklin)
    - **Visualization**: Plotly interactive charts
    """)
