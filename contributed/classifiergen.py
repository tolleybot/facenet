from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import numpy as np
import argparse
import facenet
import os
import sys
import math
import pickle
from sklearn.svm import SVC

""" Regenerates or creates the embedding classifier"""


class ClassifierGenerator:

    def __init__(self,
                 data_dir,
                 embedding_model,
                 image_size=160,
                 batch_size=90):

        self._data_dir = data_dir
        self._model = embedding_model
        self._image_size = image_size
        self._batch_size = batch_size

    def build_classifier(self, output_file='./output_classifer.pkl'):

        with tf.Graph().as_default():

            with tf.Session() as sess:

                np.random.seed(666)

                dataset = facenet.get_dataset(self._data_dir)

                # Check that there are at least one training image per class
                for cls in dataset:
                    assert (len(cls.image_paths) > 0, 'There must be at least one image for each class in the dataset')

                paths, labels = facenet.get_image_paths_and_labels(dataset)

                print('Number of classes: %d' % len(dataset))
                print('Number of images: %d' % len(paths))

                # Load the model
                print('Loading feature extraction model')
                facenet.load_model(self._model)

                # Get input and output tensors
                images_placeholder = tf.get_default_graph().get_tensor_by_name("input:0")
                embeddings = tf.get_default_graph().get_tensor_by_name("embeddings:0")
                phase_train_placeholder = tf.get_default_graph().get_tensor_by_name("phase_train:0")
                embedding_size = embeddings.get_shape()[1]

                # Run forward pass to calculate embeddings
                print('Calculating features for images')
                nrof_images = len(paths)
                nrof_batches_per_epoch = int(math.ceil(1.0 * nrof_images / self._batch_size))
                emb_array = np.zeros((nrof_images, embedding_size))

                for i in range(nrof_batches_per_epoch):
                    start_index = i * self._batch_size
                    end_index = min((i + 1) * self._batch_size, nrof_images)
                    paths_batch = paths[start_index:end_index]
                    images = facenet.load_data(paths_batch, False, False, self._image_size)
                    feed_dict = {images_placeholder: images, phase_train_placeholder: False}
                    emb_array[start_index:end_index, :] = sess.run(embeddings, feed_dict=feed_dict)

                classifier_filename_exp = os.path.expanduser(output_file)

                # Train classifier

                print('Training classifier')
                model = SVC(kernel='linear', probability=True)
                model.fit(emb_array, labels)

                # Create a list of class names
                class_names = [cls.name.replace('_', ' ') for cls in dataset]

                # Saving classifier model
                with open(classifier_filename_exp, 'wb') as outfile:
                    pickle.dump((model, class_names), outfile)
                print('Saved classifier model to file "%s"' % classifier_filename_exp)


def parse_arguments(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument('data_dir', type=str,
                        help='Path to the data directory containing aligned LFW face patches.')
    parser.add_argument('model', type=str,
                        help='Could be either a directory containing the meta_file and ckpt_file or a model protobuf (.pb) file')
    parser.add_argument('classifier_filename',
                        help='Classifier model file name as a pickle (.pkl) file. ' +
                             'For training this is the output and for classification this is an input.')
    parser.add_argument('--use_split_dataset',
                        help='Indicates that the dataset specified by data_dir should be split into a training and test set. ' +
                             'Otherwise a separate test set can be specified using the test_data_dir option.',
                        action='store_true')
    parser.add_argument('--test_data_dir', type=str,
                        help='Path to the test data directory containing aligned images used for testing.')
    parser.add_argument('--batch_size', type=int,
                        help='Number of images to process in a batch.', default=90)
    parser.add_argument('--image_size', type=int,
                        help='Image size (height, width) in pixels.', default=160)
    parser.add_argument('--seed', type=int,
                        help='Random seed.', default=666)
    parser.add_argument('--min_nrof_images_per_class', type=int,
                        help='Only include classes with at least this number of images in the dataset', default=20)
    parser.add_argument('--nrof_train_images_per_class', type=int,
                        help='Use this number of images from each class for training and the rest for testing',
                        default=10)

    return parser.parse_args(argv)


def main(args):
    gen = ClassifierGenerator(data_dir=args.data_dir, embedding_model=args.model, image_size=args.image_size,
                              batch_size=args.batch_size)
    gen.build_classifier(args.classifier_filename)


if __name__ == '__main__':
    main(parse_arguments(sys.argv[1:]))
