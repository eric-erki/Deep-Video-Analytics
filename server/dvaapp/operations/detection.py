import logging
from ..models import TrainedModel
try:
    from dvalib import detector
except ImportError:
    logging.warning("Could not import indexer / clustering assuming running in front-end mode")


class Detectors(object):
    _detectors = {}

    @classmethod
    def load_detector(cls,cd):
        cd.ensure()
        if cd.pk not in Detectors._detectors:
            if cd.detector_type == TrainedModel.TFD:
                Detectors._detectors[cd.pk] = detector.TFDetector(model_path=cd.get_model_path(),
                                                                  class_index_to_string=
                                                                  cd.arguments['class_index_to_string'])
            elif cd.detector_type == TrainedModel.YOLO:
                    # class_names = {k: v for k, v in json.loads(self.class_names)}
                    # args = {'root_dir': model_dir,
                    #         'detector_pk': self.pk,
                    #         'class_names':{i: k for k, i in class_names.items()}
                    #         }
                Detectors._detectors[cd.pk] = detector.YOLODetector(cd.get_yolo_args())
            elif cd.name == 'face':
                Detectors._detectors[cd.pk] = detector.FaceDetector()
            elif cd.name == 'textbox':
                Detectors._detectors[cd.pk] = detector.TextBoxDetector(model_path=cd.get_model_path())
            else:
                raise ValueError,"{}".format(cd.pk)