import logging
import os.path

import PIL.Image
import PIL.ImageDraw
import PIL.ImageFont

font_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'fonts', 'OpenSans-Regular.ttf')
logger = logging.getLogger()


def generate_text_image(text, dimensions, font_size=None):
    "Generate an image with text nicely wrapped and centered"
    assert text, 'Must be called with some text to render'

    image_width, image_height = dimensions
    image_aspect_ratio = image_width / image_height
    img = PIL.Image.new('RGB', dimensions)

    font_size = font_size or image_height // 10

    with open(font_path, 'rb') as fh:
        font = PIL.ImageFont.truetype(fh, size=font_size)

    # we want our text to match, more or less, the aspect ratio of the overall image
    draw = PIL.ImageDraw.Draw(img)

    # determine how big horizontal and vertical spaces are
    size_1 = draw.textsize('Z Z', font=font)
    size_2 = draw.textsize('Z\nZ', font=font)
    token_spacing = size_1[0] - 2 * size_2[0]
    line_height = size_1[1]
    line_spacing = size_2[1] - 2 * size_1[1]

    # tokenize then wrap the text so it looks good
    raw_tokens = text.split()
    token_widths = [draw.textsize(raw_token, font=font)[0] for raw_token in raw_tokens]
    text, text_width, text_height = rectangle_wrap(
        raw_tokens, token_widths, token_spacing, line_spacing, line_height, image_aspect_ratio
    )

    logger.debug(f'Computed text size: ({text_width}, {text_height})')
    logger.debug(f'Actual text size:   {draw.textsize(text, font=font)}')

    # if it's too big to fit in the image, shrink the font size and re-run the algo
    max_text_width = image_width * 0.9
    if text_width > max_text_width:
        font_size = int(font_size * max_text_width / text_width)
        return generate_text_image(text, dimensions, font_size=font_size)

    # write out the text in center of the image
    xy = ((image_width - text_width) / 2, (image_height - text_height) / 2 - line_spacing / 2)
    draw.text(xy, text, align='center', fill=(255, 255, 255), font=font)
    return img


class Token:
    "A word in a variable-width font"

    def __init__(self, token, token_width):
        self._token = token
        self._token_width = token_width

    def __str__(self):
        return self._token

    def __len__(self):
        return self._token_width


class Line:
    "A line of text in a variable-width font"

    def __init__(self, tokens, token_spacing):
        self._tokens = tokens
        self._token_spacing = token_spacing

    @property
    def first_token(self):
        return self._tokens[0] if self._tokens else None

    def pop_first_token(self):
        return self._tokens.pop(0)

    def append_token(self, token):
        self._tokens.append(token)

    def __str__(self):
        return ' '.join(str(t) for t in self._tokens)

    def __len__(self):
        if not self._tokens:
            return 0
        return sum(len(t) for t in self._tokens) + (len(self._tokens) - 1) * self._token_spacing


def rectangle_wrap(raw_tokens, token_widths, token_spacing, line_spacing, line_height, desired_aspect_ratio):
    """
    Given a series of tokens, their widths, information about spacing and a desired aspect ratio,
    return a block of text that closely matches the desired aspect ratio.

    Note that python standard library textwrap module assumes a monospace font, where as this
    utility is designed to work with variable width font.
    """
    tokens = [Token(*args) for args in zip(raw_tokens, token_widths)]

    # start all tokens in separate lines, we will iteratively move tokens up lines
    lines = [Line([token], token_spacing) for token in tokens]
    text_width = max(len(line) for line in lines)
    text_height = len(lines) * line_height + (len(lines) - 1) * line_spacing

    # iteratively flow tokens up the paragraph until we have just passed our desired aspect ratio
    while (line_cnt := len(lines)) > 1 and text_width / text_height < desired_aspect_ratio:

        # operate until we have shrunk our line cnt
        while len(lines) == line_cnt:

            # determine what our next text_width needs to be to change the token distribution
            this_line, next_line = lines[0], lines[1]
            min_new_text_width = len(this_line) + token_spacing + len(next_line.first_token)
            line_num_to_grow = 0
            for i, this_line in enumerate(lines[1:-1]):
                next_line = lines[i + 2]
                this_new_text_width = len(this_line) + token_spacing + len(next_line.first_token)
                if this_new_text_width < min_new_text_width:
                    min_new_text_width = this_new_text_width
                    line_num_to_grow = i

            # flow tokens up for subsequent lines
            line_num = line_num_to_grow
            while line_num < len(lines) - 1:
                this_line, next_line = lines[line_num], lines[line_num + 1]
                if len(next_line) == 0:
                    lines.pop(line_num + 1)
                    break
                new_line_width = len(this_line) + token_spacing + len(next_line.first_token)
                if new_line_width <= min_new_text_width:
                    this_line.append_token(next_line.pop_first_token())
                    continue
                line_num += 1

        # re-compute our overall size
        text_width = max(len(line) for line in lines)
        text_height = len(lines) * line_height + (len(lines) - 1) * line_spacing

    # serialize to our rectangle of text
    return ('\n'.join(str(line) for line in lines), text_width, text_height)
