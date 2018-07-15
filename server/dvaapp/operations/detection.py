from ..models import TrainedModel
from dvalib import detector


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
                raise NotImplementedError("YOLO model has been removed")
            elif cd.name == 'face':
                Detectors._detectors[cd.pk] = detector.FaceDetector()
            elif cd.name == 'textbox':
                Detectors._detectors[cd.pk] = detector.TextBoxDetector(model_path=cd.get_model_path())
            else:
                raise ValueError,"{}".format(cd.pk)