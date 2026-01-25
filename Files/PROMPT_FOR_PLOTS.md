# Ready-to-Use Prompt for Thesis Plots

When tests are complete and results are ready, paste this prompt to generate the plots:

---

Create a Python plotting script for my thesis benchmark results.

## Data File
- File: C:\Users\tboutas\MyProjects\etl-flink-project\Files\Tests_For_Performance.xlsx
- Format: Excel with 4 sheets, semicolon-separated values
- Structure: Metrics as rows, Tests as columns

## The 4 Sheets and Required Plots

### Sheet 1: "Scalability vs Parallelism"
- Plot: Line chart
- X-axis: Parallelism Level (4, 8, 11, 22, 33)
- Y-axis: Processing Time (seconds)
- Title: "Scalability: Processing Time vs Parallelism"

### Sheet 2: "Throughput vs Input Size"
- Plot: Line chart
- X-axis: Input Size (10K, 100K, 500K, 1M, 2M, 4M records)
- Y-axis: Throughput (records/second)
- Title: "Throughput vs Input Data Size"

### Sheet 3: "Active Configurations vs Time"
- Plot: Line chart or bar chart
- X-axis: Number of Active Configurations (1, 2, 4, 8)
- Y-axis: Processing Time (seconds)
- Title: "Impact of Active Configurations on Processing Time"

### Sheet 4: "Window Size vs Time"
- Plot: Line chart
- X-axis: Window Size (seconds)
- Y-axis: Processing Time (seconds)
- Title: "Window Size Impact on Processing Time"

## Requirements
- Single Python file: generate_plots.py
- Use: pandas, matplotlib, seaborn
- Academic/thesis quality:
  - Large fonts (14pt labels, 16pt titles)
  - Grid enabled
  - Markers on data points
  - Clean style (seaborn 'whitegrid')
- Save each plot as PNG (300 dpi) and PDF
- Output folder: ./output/

## Output Files
- fig1_scalability.png, fig1_scalability.pdf
- fig2_throughput.png, fig2_throughput.pdf
- fig3_configurations.png, fig3_configurations.pdf
- fig4_window_size.png, fig4_window_size.pdf

Run with: python generate_plots.py
