import logging
import random

from pyleus.storm import Spout

log = logging.getLogger('counter')

LINES = """
Lorem ipsum dolor sit amet, consectetur
adipiscing elit. Curabitur pharetra ante eget
nunc blandit vestibulum. Curabitur tempus mi
a risus lacinia egestas. Nulla faucibus
elit vitae dignissim euismod. Fusce ac
elementum leo, ut elementum dui. Ut
consequat est magna, eu posuere mi
pulvinar eget. Integer adipiscing, quam vitae
pretium facilisis, mi ligula viverra sapien,
nec elementum lacus metus ac mi.
Morbi sodales diam non velit accumsan
mollis. Donec eleifend quam in metus
faucibus auctor. Cras auctor sapien non
mauris vehicula, vel aliquam libero luctus.
Sed eu lobortis sapien. Maecenas eu
fringilla enim. Ut in velit nec
lectus tincidunt varius. Sed vel dictum
nunc. Morbi mollis nunc augue, eget
sagittis libero laoreet id. Suspendisse lobortis
nibh mauris, non bibendum magna iaculis
sed. Mauris interdum massa ut sagittis
vestibulum. In ipsum lacus, faucibus eu
hendrerit at, egestas non nisi. Duis
erat mauris, aliquam in hendrerit eget,
aliquam vel nibh. Proin molestie porta
imperdiet. Interdum et malesuada fames ac
ante ipsum primis in faucibus. Praesent
vitae cursus leo, a congue justo.
Ut interdum tellus non odio adipiscing
malesuada. Mauris in ante nec erat
lobortis eleifend. Morbi condimentum interdum elit,
quis iaculis ante pharetra id. In
""".strip().split('\n')


class LineSpout(Spout):

    OUTPUT_FIELDS = ["line"]

    def next_tuple(self):
        line = random.choice(LINES)
        log.debug(line)
        self.emit((line,), tup_id=random.randrange(999999999))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/word_count_lines.log',
        format="%(message)s",
        filemode='a',
    )

    LineSpout().run()
