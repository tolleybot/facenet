import numpy as np
import cv2
from face import Detection

cap = cv2.VideoCapture(0)
det = Detection(face_min_size=64)

while(True):
    # Capture frame-by-frame
    ret, frame = cap.read()

    if not ret:
        break

    faces = det.find_faces(frame)

    # Display the resulting frame
    cv2.imshow('image', frame)

    for i,f in enumerate(faces):
        cv2.imshow(str(i),f.image)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

# When everything done, release the capture
cap.release()
cv2.destroyAllWindows()