#!/usr/bin/env python

import argparse

import PIL.Image

# relative imports don't work from scripts, so depending on 'art' to be globally unique
# https://stackoverflow.com/a/16985066
from art import generate_basic_grid, generate_zoomed_grid

algorithims = {
    'basic': generate_basic_grid,
    'zoomed': generate_zoomed_grid,
}


def parse_args():
    parser = argparse.ArgumentParser(description='Generate album art')
    parser.add_argument(
        '-a', dest='algorithim', choices=['basic', 'zoomed'], required=True, help='generation algorithim to use'
    )
    parser.add_argument(
        '-o',
        dest='output_file',
        metavar='outputfile',
        type=argparse.FileType('wb'),
        required=True,
        help='file to write output image to',
    )
    parser.add_argument(
        'input_files',
        metavar='inputfile',
        type=argparse.FileType('rb'),
        nargs='+',
        help='file to read input image from',
    )
    args = parser.parse_args()
    return args.output_file, args.input_files, args.algorithim


def main():
    output_file, input_files, algorithim_id = parse_args()
    algo = algorithims[algorithim_id]
    output_image = algo([PIL.Image.open(fh) for fh in input_files])
    output_image.save(output_file, format='JPEG', quality=100)


if __name__ == '__main__':
    main()
