#!/usr/bin/env python

import argparse
import logging
import sys

# relative imports don't work from scripts, so depending on 'text_image' to be globally unique
# https://stackoverflow.com/a/16985066
import text_image


def parse_args():
    parser = argparse.ArgumentParser(description='Generate image with text')
    parser.add_argument('-t', dest='text', required=True, help='text to render into an image')
    parser.add_argument(
        '-o',
        dest='output_file',
        metavar='outputfile',
        type=argparse.FileType('wb'),
        required=True,
        help='file to write output image to',
    )
    parser.add_argument('-d', dest='debug', action='store_true', help='turn extra logging on')
    args = parser.parse_args()
    return args.output_file, args.text, args.debug


def main():
    output_file, text, debug = parse_args()
    dimensions_480p = (854, 480)
    if debug:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    output_buf = text_image.generate_text_image(text, dimensions_480p)
    output_file.write(output_buf.read())


if __name__ == '__main__':
    main()
