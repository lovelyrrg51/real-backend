"""
20180828 code fixed

$ pip freeze
numpy==1.15.1
Pillow==5.2.0
pkg-resources==0.0.0

"""

#!/usr/bin/env python
 
import os
import time

import numpy
from PIL import Image
 
def create_image(width = 1920, height = 1080, num_of_images = 1):
    width = int(width)
    height = int(height)
    num_of_images = int(num_of_images)
 
    current = time.strftime("%Y%m%d%H%M%S")
#    os.mkdir(current)
 
    for n in range(num_of_images):
        filename = '{0}_{1:03d}.jpg'.format(current, n)
        rgb_array = numpy.random.rand(height,width,3) * 255
        img_array = rgb_array.astype('uint8')
        image = Image.fromarray(img_array).convert('RGB')
        image.save(filename)
        print(filename)
#        print(img_array)
def main(args):
    return create_image(width = args[0], height = args[1], num_of_images = args[2])
 
if __name__ == '__main__':
    import sys 
    status = main(sys.argv[1:])
    sys.exit(status)
