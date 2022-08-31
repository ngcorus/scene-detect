from scenedetect import detect, ContentDetector
from datetime import datetime
import pandas as pd

class SceneDetect:
    def detectscene(self, videoadd, id):
        print('Detecting scenes...')
        scene_list = detect(videoadd, ContentDetector())
        scenes = []
        frames = []
        for i, scene in enumerate(scene_list):
            scenes.append(scene[0].get_timecode())
            frames.append(scene[0].get_frames())

        # Convert list of items in a single string delimited by commas
        scenesAsString = ','.join(map(str,scenes))
        framesAsString = ','.join(map(str,frames))

        final_data = {'Id': id, 'Scenes':scenesAsString, 'Frames': framesAsString }
        return final_data

# Use for testing during development
# scendetecttest = SceneDetect()
# scendetecttest = scendetecttest.detectscene('../data/video1.mp4')