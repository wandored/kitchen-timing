# SFTP Data Retrieval Script

This script connects to an SFTP server, retrieves data from CSV files, and performs calculations on the data. It uses the `pysftp` library for SFTP communication and `pandas` for data manipulation.

## Prerequisites

Before running the script, ensure that you have the following:

- Python 3.x installed
- The `pysftp` and `pandas` libraries installed. You can install them using `pip`:
  ```
  pip install pysftp pandas
  ```
- Access to the SFTP server and the necessary credentials.

## Usage

1. Clone the repository or download the code files.

2. Open a terminal or command prompt and navigate to the directory containing the code files.

3. Modify the script to include the correct SFTP credentials and file paths. You can update the following section in the code:

   ```python
   with open("/etc/toast_config.json") as config_file:
       config = json.load(config_file)
   ```

4. Run the script by executing the following command:

   ```
   python kitchen_timing.py
   ```

   Make sure to replace `kitchen_timing.py` with the actual name of the Python script file.

   The script will connect to the SFTP server, retrieve data from CSV files, and perform calculations on the data.

## License

This project is licensed under the [MIT License](LICENSE).

Feel free to modify and adapt the code according to your needs.

```
