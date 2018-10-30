import cv2
from face import Detection
import getopt
import sys
import numpy as np


def runDetection(camera_or_video=0, face_min_size=32, min_confidence=.75):
    cap = cv2.VideoCapture(camera_or_video)
    det = Detection(face_min_size=face_min_size,face_crop_margin=10)

    while True:
        # Capture frame-by-frame
        ret, frame = cap.read()

        if not ret:
            break

        faces = det.find_faces(frame)

        # markup face with
        if len(faces) > 0:
            for f in faces:
                b = f.bounding_box
                if b[-1] > min_confidence:
                    c = tuple(np.random.choice(range(256), size=3))
                    cv2.rectangle(frame, (b[0], b[1]), (b[2], b[3]), (int(c[0]),int(c[1]),int(c[2])), 3)

        # Display the resulting frame
        cv2.imshow('image', frame)

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    # When everything done, release the capture
    cap.release()
    cv2.destroyAllWindows()


def help():
    print('enrollment.py -c <config file>')


def main(argv):
    """ main functions """

    confidence = 0.75
    camera_video = 0
    min_size = 20

    try:
        opts, args = getopt.getopt(argv, "hi:c:s:",
                                   ["help=", "camera_or_video=","confidence=","min_size:"])
    except getopt.GetoptError:
        help()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            help()
            sys.exit()
        elif opt in ("-c", "--confidence"):
            confidence = float(arg)
        elif opt in ("-i", "--camera_or_video"):
            camera_video = arg
        elif opt in ("-s", "--min_size"):
            min_size = int(arg)

    runDetection(camera_or_video=camera_video,face_min_size=min_size,min_confidence=confidence)


if __name__ == "__main__":
    main(sys.argv[1:])
