from clean_video import *
from truncate_utils import *

#adding command line running capbility
import argparse
import os

# Create the parser and add arguments
parser = argparse.ArgumentParser(
    prog = 'CSGO_Video_analysis_cleaner',
    description= 'clean csgo twitch stream data '
)
#required arguments for directory for 
parser.add_argument(dest='src', type=str, help="Looks under the current working directory for the directory containing raw video analysis data (it should have folders for each stream)")
parser.add_argument(dest='path_csv_info', type=str, help="CSV containing basic infommation for team competing")
parser.add_argument('-out', type=str, help="directory for storing cleaned video data")
parser.add_argument('-l', '--log', action='store_true', help='Toggle logging')
parser.add_argument('--thresh', type=int, nargs='?', const=30, help='minimum number row for in_game round')
#Run with: python Brandeis_Twitch_RA/main.py rawdata Brandeis_Twitch_RA/basic_information.csv -out clean

# Parse and print the results
args = parser.parse_args()

#get dir_src
dir_src = args.src
#   - minimum # of row for ingame round in video
DEFAULT_MIN_ROW = 30
min_row = args.thresh if args.thresh is not None else DEFAULT_MIN_ROW #if thresh has specified value 
print("MIIINININININ", args.thresh is not None)

#check if current working directory has raw data folder
if not os.path.exists(dir_src):
    raise Exception("cwd: ",os.getcwd(),"\n Current working directory does not contain raw data folder\n Make sure that raw data folder is under the current working directory")
#get dir_out
dir_out = 'clean_data'if args.out is None else args.out
#create dir_out if it does not exist
if not os.path.exists(dir_out):
    print(2234234)
    os.makedirs(dir_out)
    print(dir_out, ' created under the current working directory.')

#Get df_info
path_info = args.path_csv_info
if not os.path.isfile(path_info):
    raise Exception("basic_information.csv does not exist! This file is required.")
else:
    #check if file is accessible
    if not os.access(path_info, os.R_OK):
        raise Exception("basic_information.csv cannot be read! Read permission is required! please make .csv readable.")
#init info dataframe
df_info = pd.read_csv(path_info)

if __name__ == "__main__":
    #csv names
    CSV_TEXT = "/audio_text_analysis.csv"
    CSV_VIDEO = "/video_analysis.csv"
    CSV_AUDIO = "/audio_analysis.csv"

    #brows immediate sub directories
    gen_sub = (os.walk(dir_src))
    gen_sub.__next__() #skiping root
    for x in gen_sub: 
        path2data = x[0]
        path_out = dir_out + path2data[path2data.rindex('/'):]

        #init logger
        if args.log:
            init_log(log_name = path_out + ".log")

        #reading
        df_text = pd.read_csv(path2data + CSV_TEXT)
        df_video = pd.read_csv(path2data + CSV_VIDEO)
        df_audio = pd.read_csv(path2data + CSV_AUDIO)

        #clean data
        df_video = cleanVideoDf(df_video, df_info, min_row) 
        # df_video.to_csv(path_out + ".csv", index = False)

        all = truncateAndCombineAll(df_text, df_audio, df_video)
        #write to csv
        all.to_csv(path_out + ".csv", index = True, index_label="Time_Stamp")

        print(path2data, "finished")
        break
