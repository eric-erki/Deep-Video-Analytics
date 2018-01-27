import face_embedding
import argparse
import cv2
import numpy as np

parser = argparse.ArgumentParser(description='face model test')
# general
parser.add_argument('--image-size', default='112,112', help='')
parser.add_argument('--model', default='../models/model-r34-amf/model,0', help='path to load model.')
parser.add_argument('--gpu', default=None, type=int, help='gpu id')
parser.add_argument('--det', default=2, type=int, help='mtcnn option, 2 means using R+O, else using O')
parser.add_argument('--flip', default=0, type=int, help='whether do lr flip aug')
parser.add_argument('--threshold', default=1.24, type=float, help='ver dist threshold')
args = parser.parse_args()

if __name__ == '__main__':
    model = face_embedding.FaceModel(args)
    img = cv2.imread('/Users/aub3/1.jpg')
    f1 = model.get_feature(img)
    img = cv2.imread('/Users/aub3/2.jpg')
    f2 = model.get_feature(img)
    img = cv2.imread('/Users/aub3/3.jpg')
    f3 = model.get_feature(img)
    dist1 = np.sum(np.square(f1-f2))
    dist2 = np.sum(np.square(f1-f3))
    print(dist1,dist2)