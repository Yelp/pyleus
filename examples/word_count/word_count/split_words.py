from pyleus.storm import SimpleBolt


class SplitWordsBolt(SimpleBolt):

    OUTPUT_FIELDS = ["word"]

    def process_tuple(self, tup):
        line, = tup.values
        for word in line.split():
            self.emit((word,), anchors=[tup])


if __name__ == '__main__':
    SplitWordsBolt().run()
