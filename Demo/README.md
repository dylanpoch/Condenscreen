# CondenScreen Demo Workflow

This folder contains a complete demonstration of the CondenScreen high-throughput screening analysis pipeline. Follow these steps to process demo data and reproduce the results shown in `DemoData.xlsx`.

## Workflow Overview

The CondenScreen pipeline consists of three main steps:

1. **Image Processing**: CellProfiler pipeline processes raw TIF images
2. **Data Integration**: Import processed data into R
3. **Statistical Analysis**: Generate Z-scores and individual compound scores

---

## Step 1: Image Processing with CellProfiler

### Download Raw Images

1. Download the raw TIF image files from the demo dataset
2. Extract and organize the images in a dedicated directory (e.g., `RawImages/`)

### Run CellProfiler Pipeline

1. Open **CellProfiler** on your system
2. Load the pipeline file: `CondenScreenDemo.cpproj`
3. Configure the input and output directories:
   - **Input Directory**: Drag and drop images into "Images" tab of CondenScreenDemo
   - **Output Directory**: Create and point to a new output folder (e.g., `ProcessedData/`)
4. Run the pipeline
5. CellProfiler will generate tabular files containing extracted features for each image

**Note**: Update all directory paths in the `.cpproj` file to match your system's file structure.

---

## Step 2: Well Selection and Data Configuration

### Configure Well Selection in the GUI

The `CondenScreenDemo.Rmd` file includes an interactive GUI for well plate configuration:

1. Open the RMarkdown file in RStudio
2. In the **CondenScreen Well Selection GUI**, configure the following:
   - **Plate Type**: Select **384-well plate**
   - **Positive Control Wells**: Select **B01-O01** and **B24-O24**
   - **Test Condition Wells**: Select **B02-O23**
   - **Unselected Rows**: Leave rows **A** and **P** unselected (edge wells)
   - **Negative Control**: Designated as "Negative Ctrl"

![CondenScreen Well Selection GUI](image.png)

### Save Well Selection File

After configuring wells in the GUI:
1. Set the **Save Directory** to point to the same location as `process_batchDemo.R`
2. Name the file: `well_selection.csv`
3. **Important**: Save `well_selection.csv` directly to the same directory as `process_batchDemo.R`

---

## Step 3: Import Data and Run Analysis

### Prepare Metadata

1. Review `DemoMetadata.xlsx` - this file contains plate metadata and experimental conditions
2. Import this file into your R environment as instructed in the analysis script

### Run the R Analysis Script

1. Open `process_batchDemo.R` in RStudio
2. **Update all file paths** to match your system:
   - Path to CellProfiler output CSV files
   - Path to `DemoMetadata.xlsx`
   - Path to `well_selection.csv` (saved from the GUI)
   - Output directory for results
3. Run the script line by line or source the entire file

### Output Files

The script will generate:
- **DemoData.xlsx**: Final results spreadsheet containing:
  - Z'-score (assay quality metric)
  - Compound Z-scores for each tested compound
  - Well-by-well measurements and classifications

---

## Expected Results

### Assay Quality Metrics

The Z'-score indicates assay quality (range: -∞ to 1, where >0.5 is acceptable). Note: this Demo will result in a lower Z'-Score and less significant compound scores due to limited image sampling.
