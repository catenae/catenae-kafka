from catenae import Link, Electron


class Filter(Link):
    def setup(self):
        self.allowed_words = set(['CPU', 'proof-of-work', 'chain', 'nodes'])

    def transform(self, electron):
        line = electron.value
        words = line.split()
        if set(words).intersection(self.allowed_words):
            self.logger.log(line)


if __name__ == "__main__":
    Filter().start()