import pandas as pd
import numpy as np
from utils import printFull
import logging

def unify_index(df_src, other_dfs):
    indicies_src = df_src.index
    for df in other_dfs:
        including_index = df.index[df.index.isin(indicies_src)]
        df.loc[:, :] = df.loc[including_index, :]
    #Unification is finish
    # print("Unification is finished")

def truncateAndCombineAll(df_text, df_audio, df_video, df_vocal, df_music):
    #audio truncating
    truncateAudio(df_audio)
    # truncateAudio(df_vocal)
    # truncateAudio(df_music)
    #video truncating
    truncateVideo(df_video)
    #get the minimal length
    min_len = min([len(x) for x in [df_video, df_audio, df_text]])

    #remove original indices
    #  - audio
    df_audio = df_audio.iloc[:, 1:]
    # df_vocal = df_vocal.iloc[:, 1:]
    # df_music = df_music.iloc[:, 1:]
    #  - video
    # df_video = df_video.iloc[:, 1:] #first column is Round_ID
    df_video.rename(columns={'Timestamp': 'Stream_Time_Past'}, inplace=True)
    #  - text
    df_text = df_text.iloc[:, 1:]

    #concatenate
    combined = pd.concat([df_video, df_audio, df_text], axis=1)
    #trucate to the shortest df
    combined = combined.iloc[:min_len, :]
    return combined

def truncateAudio(df_audio, num_split = 5):
    audio_step = len(df_audio)//num_split
    #stepping for inex
    FLAG_INDEX = [1,3] #deleting on 2nd and 4th place
    flag = 0
    #should be log message here
    for i in range(audio_step, len(df_audio), audio_step):
        if flag in FLAG_INDEX:
            logging.info("Audio dropping row#: "+str(i))
            df_audio.drop(i, axis = 0, inplace = True)
        flag += 1

def truncateVideo(df_video, step = 1000):
    df_video = df_video.drop(df_video.index[::1000], axis=0, inplace = True)
