from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import math
import sys
import os.path
from PIL import Image
import logging
import numpy as np
from .base_analyzer import BaseAnnotator
from collections import namedtuple
sys.path.append(os.path.join(os.path.dirname(__file__), "../../repos/"))  # remove once container is rebuild

if os.environ.get('PYTORCH_MODE',False):
    import crnn.utils as utils
    import crnn.dataset as dataset
    import torch
    from torch.autograd import Variable
    import crnn.models.crnn as crnn_model
    logging.info("In pytorch mode, not importing TF")
elif os.environ.get('CAFFE_MODE',False):
    pass
elif os.environ.get('MXNET_MODE',False):
    import mxnet as mx
    from PIL import Image
else:
    import tensorflow as tf
    from tensorflow.contrib.slim.python.slim.nets import inception
    from tensorflow.python.training import saver as tf_saver
    slim = tf.contrib.slim

Batch = namedtuple('Batch', ['data'])


def inception_preprocess(image, central_fraction=0.875):
    image = tf.cast(tf.image.decode_jpeg(image, channels=3), tf.float32)
    # image = tf.image.central_crop(image, central_fraction=central_fraction)
    image = tf.expand_dims(image, [0])
    # TODO try tf.image.resize_image_with_crop_or_pad and tf.image.extract_glimpse
    image = tf.image.resize_bilinear(image, [299, 299], align_corners=False)
    # Center the image about 128.0 (which is done during training) and normalize.
    image = tf.multiply(image, 1.0 / 127.5)
    return tf.subtract(image, 1.0)


class OpenImagesAnnotator(BaseAnnotator):

    def __init__(self,model_path,gpu_fraction=None):
        super(OpenImagesAnnotator, self).__init__()
        self.name = "inception"
        self.object_name = "tag"
        self.net = None
        self.tf = True
        self.session = None
        self.label_set = 'open_images_tags'
        self.graph_def = None
        self.input_image = None
        self.predictions = None
        self.num_classes = 6012
        self.top_n = 25
        self.network_path = model_path
        self.labelmap_path = model_path.replace('open_images.ckpt','open_images_labelmap.txt')
        self.dict_path = model_path.replace('open_images.ckpt','open_images_dict.csv')
        self.labelmap = [line.rstrip() for line in file(self.labelmap_path).readlines()]
        if gpu_fraction:
            self.gpu_fraction = gpu_fraction
        else:
            self.gpu_fraction = float(os.environ.get('GPU_MEMORY', 0.15))

    def load(self):
        if self.session is None:
            if len(self.labelmap) != self.num_classes:
                logging.error("{} lines while the number of classes is {}".format(len(self.labelmap), self.num_classes))
            self.label_dict = {}
            for line in tf.gfile.GFile(self.dict_path).readlines():
                words = [word.strip(' "\n') for word in line.split(',', 1)]
                self.label_dict[words[0]] = words[1]
            logging.warning("Loading the network {} , first apply / query will be slower".format(self.name))
            config = tf.ConfigProto()
            config.gpu_options.per_process_gpu_memory_fraction = self.gpu_fraction
            g = tf.Graph()
            with g.as_default():
                self.input_image = tf.placeholder(tf.string)
                processed_image = inception_preprocess(self.input_image)
                with slim.arg_scope(inception.inception_v3_arg_scope()):
                    logits, end_points = inception.inception_v3(processed_image, num_classes=self.num_classes, is_training=False)
                self.predictions = end_points['multi_predictions'] = tf.nn.sigmoid(logits, name='multi_predictions')
                saver = tf_saver.Saver()
                self.session = tf.InteractiveSession(config=config)
                saver.restore(self.session, self.network_path)

    def apply(self,image_path):
        if self.session is None:
            self.load()
        img_data = tf.gfile.FastGFile(image_path).read()
        predictions_eval = np.squeeze(self.session.run(self.predictions, {self.input_image: img_data}))
        results = {self.label_dict.get(self.labelmap[idx], 'unknown'):predictions_eval[idx]
                   for idx in predictions_eval.argsort()[-self.top_n:][::-1]}
        labels = [t for t,v in results.iteritems() if v > 0.1]
        text = " ".join(labels)
        metadata = {t:round(100.0*v,2) for t,v in results.iteritems() if v > 0.1}
        return self.object_name,text,metadata,labels


class CRNNAnnotator(BaseAnnotator):

    def __init__(self,model_path):
        super(CRNNAnnotator, self).__init__()
        self.session = None
        self.object_name = "text"
        self.model_path = model_path
        self.alphabet = '0123456789abcdefghijklmnopqrstuvwxyz'
        self.cuda = False

    def load(self):
        logging.info("Loding CRNN model first apply will be slow")
        if torch.cuda.is_available():
            self.session = crnn_model.CRNN(32, 1, 37, 256, 1).cuda()
            self.cuda = True
        else:
            self.session = crnn_model.CRNN(32, 1, 37, 256, 1)
        self.session.load_state_dict(torch.load(self.model_path))
        self.session.eval()
        self.converter = utils.strLabelConverter(self.alphabet)
        self.transformer = dataset.resizeNormalize((100, 32))

    def apply(self,image_path):
        if self.session is None:
            self.load()
        image = Image.open(image_path).convert('L')
        if self.cuda:
            image = self.transformer(image).cuda()
        else:
            image = self.transformer(image)
        image = image.view(1, *image.size())
        image = Variable(image)
        preds = self.session(image)
        _, preds = preds.max(2)
        preds = preds.transpose(1, 0).contiguous().view(-1)
        preds_size = Variable(torch.IntTensor([preds.size(0)]))
        sim_pred = self.converter.decode(preds.data, preds_size.data, raw=False)
        return self.object_name,sim_pred,{},None


class LocationNet(BaseAnnotator):

    def __init__(self,model_path,epoch):
        super(LocationNet, self).__init__()
        self.session = None
        self.object_name = "location"
        self.model_path = model_path
        self.cuda = False
        self.model_path = model_path
        self.prefix = "{}/RN101-5k500".format(model_path)
        self.epoch = epoch
        self.grids = []

    def load(self):
        sym, arg_params, aux_params = mx.model.load_checkpoint(self.prefix, self.epoch)
        self.session = mx.mod.Module(symbol=sym, context=mx.gpu())
        self.session.bind([('data', (1, 3, 224, 224))], for_training=False)
        self.session.set_params(arg_params, aux_params, allow_missing=True)
        self.mean_rgb = np.array([123.68, 116.779, 103.939]).reshape((3, 1, 1))
        with open('{}/grids.txt'.format(self.model_path), 'r') as f:
            for line in f:
                line = line.strip().split('\t')
                lat = float(line[1])
                lng = float(line[2])
                self.grids.append((lat, lng))

    def apply(self,image_path):
        if self.session is None:
            self.load()
        self.session.forward(Batch(self.preprocess(image_path)), is_train=False)
        prob = self.session.get_outputs()[0].asnumpy()[0]
        pred = np.argsort(prob)[::-1]
        results = {}
        for i in range(5):
            pred_loc = self.grids[int(pred[i])]
            results[i+1] = {'score':prob[pred[i]],
                            'lat':pred_loc[0],
                            'long': pred_loc[1],
                            'bin_index':int(pred[i])
                            }
        return self.object_name,"",results,None

    def preprocess(self,path):
        img = Image.load(path)
        short_side = min(img.shape[:2])
        yy = int((img.shape[0] - short_side) / 2)
        xx = int((img.shape[1] - short_side) / 2)
        crop_img = img[yy: yy + short_side, xx: xx + short_side]
        resized_img = img.resize(224, 224)
        sample = np.asarray(resized_img) * 256
        sample = np.swapaxes(sample, 0, 2)
        sample = np.swapaxes(sample, 1, 2)
        normed_img = sample - self.mean_rgb
        normed_img = normed_img.reshape((1, 3, 224, 224))
        return [mx.nd.array(normed_img)]