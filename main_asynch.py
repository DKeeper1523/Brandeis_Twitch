from clean_video import *
from truncate_utils import *

#adding command line running capbility
import argparse
import os
import time
from tqdm.auto import tqdm
from multiprocessing import Pool, current_process
from functools import partial

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

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
#Run with: python3 Brandeis_Twitch_RA/main_asynch.py rawdata Brandeis_Twitch_RA/basic_information.csv -l

# Parse and print the results
args = parser.parse_args()

#get dir_src
dir_src = args.src
#   - minimum # of row for ingame round in video
DEFAULT_MIN_ROW = 15
min_row = args.thresh if args.thresh is not None else DEFAULT_MIN_ROW #if thresh has specified value 

#check if current working directory has raw data folder
if not os.path.exists(dir_src):
    raise Exception("cwd: ",os.getcwd(),"\n Current working directory does not contain raw data folder\n Make sure that raw data folder is under the current working directory")

def createDirIfMissing(path_dir):
    #create dir_out if it does not exist
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)
        print(path_dir, ' created under the current working directory.')

# get all dir needed
dir_root = 'clean_data'if args.out is None else args.out
dir_members = dir_root + '/members'
path_csv_alldata = dir_root + '/all_data.csv'
#Create Dir needed for clean data
createDirIfMissing(dir_root)
createDirIfMissing(dir_members)

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

def cleanAndMerge(name_sub):
    #csv names
    CSV_TEXT = "/audio_text_analysis.csv"
    CSV_VIDEO = "/video_analysis.csv"
    CSV_AUDIO = "/audio_analysis.csv"
    CSV_VOCAL = "/audio_analysis_vocal.csv"

    path2data = dir_src + '/' + name_sub
    path_out = dir_members + '/' + name_sub

    #init logger
    if args.log:
        path_log = dir_members + '/log' 
        createDirIfMissing(path_log)
        init_log(log_name = path_out + name_sub + ".log")

    #reading
    df_text = pd.read_csv(path2data + CSV_TEXT)
    df_video = pd.read_csv(path2data + CSV_VIDEO, low_memory=False, encoding='latin1')
    df_audio = pd.read_csv(path2data + CSV_AUDIO)
    df_vocal = pd.read_csv(path2data + CSV_VOCAL)

    cur_process_id = current_process()._identity[0]-1
    #clean data
    df_video = cleanVideoDf(path2data+"_video processing", cur_process_id, df_video, df_info, min_row) 

    #unify index
    unify_index(df_video, [df_text, df_audio])

    #combine combined
    combined = truncateAndCombineAll(df_text, df_audio, df_video, df_vocal)

    #reset and insert index
    combined.reset_index(drop=True, inplace=True)
    # combined.insert(1, "Time_Stamp", combined.index)

    #write to csv
    combined.to_csv(path_out + ".csv", index = False)
    return combined

def main():
    #brows immediate sub directories
    ls_dir_sub = list(os.walk(dir_src))[0][1]

    #record starting time
    tic = time.time()
    #init pool of processes
    pool = Pool(processes= min([os.cpu_count(), len(ls_dir_sub)]))
    
    #link process with task along with progress bar
    pbar = tqdm(ls_dir_sub, desc='Main loop')

    res = list(pool.imap(cleanAndMerge, pbar))

    #start process
    pool.close()
    pool.join()

    #sorting all raw csv based on date and Stream Type
    sorted_result = sorted(res, key= lambda df: df.Date[0] + df.Stream[0])
    final = pd.concat(sorted_result, axis=0)

    #GameID: the id for BO game played by two team (including both bo1,bo3)
    cols = ['Stage', 'Team_0', 'Team_1']
    possible_id = ((final[cols].shift() != final[cols]).any(axis=1)).cumsum()
    final.loc[:, 'GameID'] = possible_id

    final.to_csv(path_csv_alldata, index=False)

    toc = time.time()
    print(f'Completed in {toc - tic} seconds')

if __name__ == "__main__":
    main()

    #brows immediate sub directories
    # gen_sub = list(os.walk(dir_members))[0][2] #unpack; only get sub files

    # ls_df = [pd.read_csv(dir_members+'/'+path) for path in gen_sub if '.csv' in path]

    # sorted_df = sorted(ls_df, key= lambda df: df.Date[0] + df.Stream[0])
    # final = pd.concat(sorted(ls_df, key= lambda df: df.Date[0]), axis=0)

    # cols = ['Stage','Map','Team_0', 'Team_1']
    # possible_id = ((final[cols].shift() != final[cols]).any(axis=1)).cumsum()
    # final.loc[:, 'GameID'] = possible_id

    # final.to_csv(path_csv_alldata, index=False)