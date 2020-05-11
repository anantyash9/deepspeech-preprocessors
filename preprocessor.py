import webvtt
from webvtt.errors import MalformedCaptionError
from pydub import AudioSegment
import pandas
import glob
import os
import random
from os import path
import threading
import time


def to_csv(file_list, save_location):
    valid = [" ", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
             "m", "n", "o", "p", 'q', "r", "s", "t", "u", 'v', 'w', 'x', 'y', 'z', "'"]
    bucket = None
    df = {'train': None, 'dev': None, 'test': None}
    counter = 1
    maxsize = 0
    for file in file_list:
        print(counter)
        counter += 1
        local_file = file
        trans_file = file.replace('.wav', '.txt')
        with open(trans_file, "r") as fin:
            transcript = ' '.join(fin.read().strip().lower().split(' ')[
                                  2:]).replace('.', '')
            transcript = ''.join(
                [character for character in transcript if (character in valid)])

        if (not (transcript.isspace() or transcript is None or transcript == '')) and os.path.getsize(local_file) > 150644:
            random_value = random.randint(0, 100)
            if random_value < 11:
                bucket = 'test'
            elif random_value < 30:
                bucket = 'dev'
            elif random_value < 100:
                bucket = 'train'
            if df[bucket] is None:
                df[bucket] = pandas.DataFrame(data=[(os.path.abspath(local_file), os.path.getsize(local_file), transcript)],
                                              columns=["wav_filename", "wav_filesize", "transcript"])
            else:
                new_row = pandas.DataFrame(data=[(os.path.abspath(local_file), os.path.getsize(local_file), transcript)],
                                           columns=["wav_filename", "wav_filesize", "transcript"])
                df[bucket] = pandas.concat(
                    [new_row, df[bucket]], ignore_index=True)
            if maxsize < os.path.getsize(local_file):
                maxsize = os.path.getsize(local_file)
    print(maxsize)
    df['train'].to_csv(save_location+"/train.csv", index=False)
    df['test'].to_csv(save_location+"/test.csv", index=False)
    df['dev'].to_csv(save_location+"/dev.csv", index=False)


def convert_to_spec(path):
    track = AudioSegment.from_file(path)
    track = track.set_channels(1)
    track = track.set_frame_rate(16000)
    track = track.set_sample_width(2)
    return track


def convert_intermedate_form(sub_path, files, id, save_location):
    segments = []
    text = []
    i = 0
    track = convert_to_spec(files)
    try:
        sub = webvtt.read(sub_path)
        sub = sub[10:-10]
        for caption in sub:
            clean_text = caption.text.replace(
                '\n', ' ').replace(',', ' ').replace('-', ' ')
            clean_text = ''.join([character for character in clean_text if (
                character.isalpha() or character == ' ')])
            start = (caption.start.split(":"))
            s = float(start[0])*3600+float(start[1])*60+float(start[2])
            end = (caption.end.split(":"))
            e = float(end[0])*3600+float(end[1])*60+float(end[2])
            temp = track[s*1000:e*1000]
            if len(clean_text.split(' ')) <= 3 or (e-s) < 3 or (e-s) > 20:
                continue
            if not path.exists(save_location+'/'+id+'/'):
                os.makedirs(save_location+'/'+id+'/')

            with open(save_location+'/'+id+'/'+str(i)+".txt", "w") as text_file:
                text_file.write(clean_text.replace('\n', ' '))

            temp.export(save_location + '/' +
                        id+'/'+str(i)+".wav", format="wav")

            i += 1
    except MalformedCaptionError as e:
        pass


def list_valid_files(directory):
    list_files = glob.glob(directory+'/*.m4a')+glob.glob(directory+'/*.webm')
    rm = []
    for files in list_files:
        key = files.split('.')[0]
        if not(path.exists(key+'.en.vtt')):
            rm.append(files)
    list_files = list(set(list_files)-set(rm))
    return list_files


def pre_process(list_files, save_location):
    t = time.time()
    j = 1
    for files in list_files:
        if threading.active_count() > 5:
            time.sleep(2)
        id = files.split('/')[-1].split('.')[0]
    #     convert_intermedate_form(files.split('.')[0]+'.en.vtt',track,id)
        threading.Thread(target=convert_intermedate_form, args=(
            files.split('.')[0]+'.en.vtt', files, id, save_location)).start()
    #     _thread.start_new_thread(convert_intermedate_form,(files.split('.')[0]+'.en.vtt',track,id,))
        print(j)
        print(time.time()-t)
        j += 1


list_files = list_valid_files('/media/infyblr/ssd2/DeepSpeech/data-youtube')
pre_process(list_files, 'pre-proccessed')
list_processed_files = glob.glob('pre-proccessed/*/*.wav')
to_csv(list_processed_files, 'csv')
