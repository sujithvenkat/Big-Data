# Data Engineering Take-Home Assignment

## Project Structure

data_engineering_takehome/Assessment
│
├── code/
│   ├── assessments.py
│   ├── test_assessments.py
├── jupyter/
│   └── assessment_notebook.ipynb
│
└── MyDocument.md
└── README.md


## How to Run

To execute the full solution and generate answers for all six questions:
spark-submit assessments.py "path_to_input nyc-jobs.csv" "output_path"

Example:

spark-submit assessments.py data/nyc-jobs.csv output/


## Notes

- `assessments.py` contains the full implementation and answers to all six questions and the output file got generted
- `test_assessments.py` contains few unit testcases tried.
- `assessment_notebook.ipynb`  contains exploratory data analysis (EDA) 
  and visualizations (bar charts) executed in a Docker-based Jupyter environment
- `mydocument.md` explains assumptions and design decisions.