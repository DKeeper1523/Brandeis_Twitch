
# CS STREAM to Tabular data via CSV OCR capture

This is a data cleaning tools specialized to clean ocr data caputured in CS tournament. This cleaner was originally designed to clean data from 2023 Blast Paris Major. However, the general frame work can work with any other OCR CS stream capture.

## 📦 Installation Instructions
### 📁 Clone Repo
clone repo
```bash
git clone https://github.com/tianyaohu/Brandeis_Twitch_RA.git
```

### 🛠️ install dependencies
```
!pip install fuzzywuzzy python-Levenshtein
!pip install tqdm
```

### Run Main
*Example Command*
```bash
#First, change Working Directory
cd /content/drive/MyDrive/"Twitch Project"

!python Brandeis_Twitch_RA/main_asynch.py rawdata 'Sample Video Analysis'/basic_information.csv -l
```

<details>
<summary><b>Argument Details</b></summary>

- `<src>`: Specifies the directory containing raw video analysis data, structured with subdirectories for each stream to process. This is a required argument.
- `<path_csv_info>`: Indicates the path to a CSV file with information about the teams in the streams, essential for merging specific team data. This is a required argument.
- `-out` [optional]: Defines the directory where cleaned video data will be stored. Defaults to a directory named `clean_data` if omitted.
- `-l, --log` [optional]: Enables logging of the process, generating log files for debugging or verification. Useful for detailed process tracking.
- `--thresh` [optional]: Sets a threshold for the minimum number of rows an in-game round must have to be included in the final cleaned data. Default is 15, adjustable for data quality.

</details>

### Integration with Google Drive via Collab

Here is a collab template to run over a dataset stored in Google Drive. Make a copy and rock on!
https://colab.research.google.com/drive/1wN2sMh9uCyxhcwhYlWNCosvVY9xZc1GG?usp=sharing

# 🔴 Implementation Detail
## Main Asynch
The `main_asynch.py` script manages data cleaning by integrating video, audio, and text sources into a unified CSV dataset. It supports command-line arguments for customizing input/output paths, logging, and cleaning thresholds.



