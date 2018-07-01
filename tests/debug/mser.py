import cv2
import numpy as np
import sys

if __name__ == '__main__':
    mser = cv2.MSER_create()
    img = cv2.imread(sys.argv[-1])
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    visualization = img.copy()
    regions, _ = mser.detectRegions(gray)
    hulls = [cv2.convexHull(p.reshape(-1, 1, 2)) for p in regions]
    cv2.polylines(visualization, hulls, 1, (0, 255, 0))
    mask = np.zeros((img.shape[0], img.shape[1], 1), dtype=np.uint8)
    for contour in hulls:
        cv2.drawContours(mask, [contour], -1, (255, 255, 255), -1)
    text_only = cv2.bitwise_and(img, img, mask=mask)
    cv2.imwrite('img2.jpg', visualization)
    cv2.imwrite("textonly.jpg", text_only)

