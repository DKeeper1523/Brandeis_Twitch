import pandas as pd
import numpy as np
import logging

def truncateAndCombineAll(df_text, df_audio, df_video, file_name):
    truncateAudio(df_audio)
    truncateVideo(df_video)
    len_text = len(df_text)
    len_video = len(df_video)
    len_audio = len(df_audio)
    min_len = min([len_audio, len_text, len_video])
    #drop old indicies
    df_audio = df_audio.iloc[:min_len, 1:]
    df_video = df_video.iloc[:min_len, 1:]
    df_text = df_text.iloc[:min_len, 1:]
    # print("text:", len(df_text), "audio", len(df_audio), "video", len(df_video))
    #concatenate
    combined = pd.concat([df_video, df_audio, df_text])
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
            df_audio.drop(i, inplace=True)
        flag += 1

def truncateVideo(df_video, step = 1000):
    indicies2drop = range(step-1, len(df_video), step)
    df_video.drop(pd.Index(indicies2drop), axis=0, inplace=True)