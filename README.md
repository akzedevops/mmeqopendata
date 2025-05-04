# Earthquake Data Analysis

## Description

This Python project provides tools for collecting, analyzing, and visualizing earthquake data. It includes scripts for data export, performing various types of analysis on earthquake events, and visualizing the results.

## Installation

To set up the project and install the necessary dependencies, please follow these steps:

### 1. Install pip

`pip` is the package installer for Python. If you don't have it installed, you can typically install it using your operating system's package manager.

*   **On Debian/Ubuntu:**

    ```bash
    sudo apt update
    sudo apt install python3-pip
    ```

*   **On macOS (using Homebrew):**

    ```bash
    brew install python
    ```
    (Homebrew installs pip automatically with Python)

*   **On Windows:**

    Download the get-pip.py script from [https://pip.pypa.io/en/stable/installation/](https://pip.pypa.io/en/stable/installation/) and run it using `python get-pip.py`.

### 2. Create and Activate a Virtual Environment

It's highly recommended to use a virtual environment to manage project dependencies and avoid conflicts with system-wide Python packages.

*   **Create a virtual environment:**

    ```bash
    python3 -m venv .venv
    ```
    This will create a folder named `.venv` in your project directory.

*   **Activate the virtual environment:**

    *   **On Windows:**

        ```bash
        .venv\Scripts\activate
        ```

    *   **On macOS and Linux:**

        ```bash
        source .venv/bin/activate
        ```

    You should see `(.venv)` at the beginning of your terminal prompt, indicating that the virtual environment is active.

### 3. Install Project Dependencies

With the virtual environment activated, install the required libraries using `pip` and the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

This will install all the packages listed in `requirements.txt`.

## Usage

Before running any of the project scripts, make sure your virtual environment is activated (see Installation step 2).

### Data Export

The `dataexport.py` script is used to export earthquake data. 

To run the data export script, use the following command:

```bash
python dataexport.py
```

Follow any prompts or instructions provided by the script.

### Data Analysis

The `advanalysis.py` and `adv2analysis.py` scripts are used for earthquake data analysis.

To run `advanalysis.py`:

```bash
python advanalysis.py
```

To run `adv2analysis.py`:

```bash
python adv2analysis.py
```

Refer to the specific scripts for command-line arguments or configuration options they might support.

### Data Visualization

The `visualizer.py` script is used to visualize the earthquake data or analysis results.

To run the visualizer:

```bash
python visualizer.py
```

Consult the `visualizer.py` script for details on how to use it and what visualizations it generates.

## Contact

For any questions or issues, please contact me from aungkhantzawd@gmail.com