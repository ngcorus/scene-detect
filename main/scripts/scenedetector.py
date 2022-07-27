from scenedetect import detect, ContentDetector
from datetime import datetime
import pandas as pd

class SceneDetect:
    def detectscene(self, videoadd, id):
        print('Detecting scenes...')
        scene_list = detect(videoadd, ContentDetector())
        # chapter = [0,586,906,1299,1852,2226]
        scenes = []
        frames = []
        for i, scene in enumerate(scene_list):
            scenes.append(scene[0].get_timecode())
            frames.append(scene[0].get_frames())
            # print('Scene %2d: Start %s / Frame %d, End %s / Frame %d' % (
            #     i+1,
            #     scene[0].get_timecode(), scene[0].get_frames(),
            #     scene[1].get_timecode(), scene[1].get_frames(),)
            # )

        # Convert list of items in a single string delimited by commas
        scenesAsString = ','.join(map(str,scenes))
        framesAsString = ','.join(map(str,frames))

        final_data = {'Id': id, 'Scenes':scenesAsString, 'Frames': framesAsString }
        return final_data

        # Create a datframe with the video, scene and frame info
        # df = pd.DataFrame(columns=['Id','Scenes','Frames'])
        # df.at[1, 'Id'] = id
        # df.at[1, 'Scenes'] = scenesAsString
        # df.at[1, 'Frames'] = framesAsString

# For development
# scendetecttest = SceneDetect()
# scendetecttest = scendetecttest.detectscene('../data/video1.mp4')