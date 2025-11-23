# CondensateML

This folder contains a Jupyter notebook implementing the core supervised machine-learning approach to distinguishing condensate phenotypes using microscopy images.
## Overview

### Microscope image preprocessing and filtering into single-cell frames
### ResNet18-based feature extraction (512D embeddings)
### UMAP projection + anchor-based similarity scoring

The main notebook(s) perform the following high-level steps:
1. Load microscopy images data.
2. Basic data cleaning and preprocessing (crop images into single-cell frames, resize, filter, and normalize).
3. Apply  metadata file to link individual cells with their genotype KO condition. Feature extraction or feature selection (e.g., extracting intensity/shape features, etc.).
4. Train machine-learning models (using ResNet18 with pretrained IMAGENET1K_V1).
5. Model evaluation using cross-validation and metrics (accuracy, loss, ROC AUC, confusion matrix.).
6. Principal component clustering and UMAP projection + anchor-based similarity scoring.
7. Visualization of results and saving trained model.

## Primary files
- `*.ipynb` — Notebook(s) implementing the analysis (open these with Jupyter).
- similarity_model_best.pt contains the trained model
- CondensateML_rankings.xlsx + esnet18_patch_metadata.xls contains result outputs 

## Key Python dependencies (minimal, annotate and pin versions locally)
- python 3.8+
- numpy
- pandas
- scikit-learn
- matplotlib
- seaborn
- scikit-image (if image processing is used)
- joblib (for model serialization)
- tqdm (optional, for progress bars)
- jupyter / jupyterlab

If a `requirementsPy.txt` exists at the repository root, install dependencies with:
```
conda [or pip] install <X_dependency>
```

## Expected runtime
- *Highly* recommended to have GPU access in order to complete image processing and model training.
- With modest GPU (> GeForce RTX 2070 or equivlent) a small test set of < 1k TIF images can be completed in under 1 hour. 
- Larger datasets or heavier feature extraction (especially image-based) can take longer; runtime will vary by CPU/GPU and dataset size. The repository was tested Windows 10 (Ryzen 7 2700X, 64 GB).

## License
The code in this folder follows the repository license: Apache License 2.0 (see top-level LICENSE).
