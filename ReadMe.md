# Nginx Log Analysis

This repository contains a task to parse, clean, and visualize Nginx log files, as well as an ETL process to store parsed logs into a MySQL database.

## Project Context

This project was originally given to me as part of an interview task for a Data Engineer position at a tech company. I thought it was a valuable project to showcase my skills in data processing, cleaning, and visualization, so I decided to share it here.

## Project Requirements

Before running the project, ensure you have the following installed:

  1. **Python**: Version 3.8 or higher
  2. **MySQL Database**: Make sure you have access to a MySQL database where you can create tables for storing the parsed log data.

## Project Structure

```plaintext
nginx-log-analysis/
├── config/
│   └── config.ini
├── data/
│   └── Data engineering internship.pdf  # The problem explanation and the desired output by company
│   └── nginx_logs.txt  # Raw Nginx log file provided for the task
├── notebooks/
|   └── demo.ipynb  # Notebook file to show parsing process for better understanding
│   └── log_analysis.ipynb
├── reports/
│   └── task_summary.pdf  # Summary report of the task
├── scripts/
│   └── ETL.py
├── requirements.txt
├── README.md
└── .gitignore
```



## Setup Instructions

To run the script, follow these steps:

1. **Create a virtual environment:**

    ```sh
    python -m venv env
    ```

2. **Activate the virtual environment:**

   - On Windows:
     ```sh
     .\env\Scripts\activate
     ```
   - On macOS/Linux:
     ```sh
     source env/bin/activate
     ```

3. **Install the requirements:**

    ```sh
    pip install -r Requirements.txt
    ```

4. **Run the ETL script:**
  ```sh
    python scripts/etl.py
  ```

5. **Run the Jupyter notebook for analysis and visualization:**
  ```
    jupyter notebook notebooks/log_analysis.ipynb
  ```