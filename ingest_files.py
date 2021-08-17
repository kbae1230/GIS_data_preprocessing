from data_source import DataSource
import argparse
import json

class IngestWorker:
    def __init__(self):
        self.__ds=DataSource.get_postgis_idc()
        self.__cursor = self.__ds.cursor()
    
    def __del__(self):
        self.__ds.close()

    def has_scene(self, scene_id):
        sql = f'SELECT count(id) FROM tbl_sia_scenes WHERE scene_name = upper("{scene_id}")'
        self.__cursor.execute(sql)
        count = self.__cursor.fetchone()
        return count > 0

def main(scenes_filepath):
    scenes = json.load(scenes_filepath)
    worker = IngestWorker()

    for id, path in enumerate(scenes):
        if worker.has_scene(id):
            continue

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description = 'Scene ingest'
    )
    parser.add_argument(
        '--scene_filepath',
        default=None,
        metavar='PATH',
        help='scene id-path dictionary file location'
        type=str
    )
    args = parser.parse_args()
    main(**vars(args))